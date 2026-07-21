/**
 * Two migration/cleanup gaps the parity audit flagged:
 *  - `truncateAll` was SQLite-only (a hardcoded `sqlite_master` query) despite
 *    the rest of the package being multi-dialect; it now goes through the
 *    shared dialect-aware catalog helper.
 *  - `db:wipe` had no public equivalent — `#dropAllTables()` was private.
 *    `MigrationRunner.wipe()` exposes it.
 *
 * Also covers `rollback({ batch })`, which rolls back to a target batch rather
 * than only the latest.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasDialect } from "../../src/query/native.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";
import { truncateAll } from "../../src/testing/DatabaseCleanup.js";

function toAdapter(conn: AsyncDatabaseConnection): DatabaseAdapter {
	return {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: <T>(sql: string, params?: unknown[]) => conn.query<T>(sql, params),
		close: () => conn.close(),
		runInTransaction: (batch) => conn.runInTransaction(batch),
	};
}

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

let conn: AsyncDatabaseConnection;
let tmpDir: string;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-wipe-"));
});

afterEach(async () => {
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
});

/** Write a numbered migration file that creates `table(col)`. */
async function writeMigration(name: string, table: string): Promise<void> {
	await fsp.writeFile(
		path.join(tmpDir, `${name}.ts`),
		`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('${table}', (t) => { t.increments('id') }) }
  async down() { this.schema.dropTable('${table}') }
}`,
	);
}

describe("truncateAll", () => {
	it("empties user tables but leaves ream_* and data structure intact", async () => {
		await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, n TEXT)");
		await conn.execute("CREATE TABLE ream_migrations (name TEXT)");
		await conn.execute("INSERT INTO users (n) VALUES ('a'), ('b')");
		await conn.execute("INSERT INTO ream_migrations (name) VALUES ('001')");

		await truncateAll(conn);

		// User rows gone, table still there.
		const users = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM users",
		);
		expect(users[0]?.n).toBe(0);
		// Framework bookkeeping untouched.
		const tracked = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM ream_migrations",
		);
		expect(tracked[0]?.n).toBe(1);
	});

	it("truncates in spite of a foreign key between the tables", async () => {
		await conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
		await conn.execute(
			"CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
		);
		await conn.execute("PRAGMA foreign_keys = ON");
		await conn.execute("INSERT INTO parent (id) VALUES (1)");
		await conn.execute("INSERT INTO child (id, pid) VALUES (1, 1)");

		// A naive DELETE of parent-first would trip the FK; truncateAll suspends it.
		await expect(truncateAll(conn)).resolves.toBeUndefined();
		const parent = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM parent",
		);
		expect(parent[0]?.n).toBe(0);
	});
});

describe("MigrationRunner.wipe()", () => {
	it("drops every table, including the migrations table", async () => {
		await writeMigration("001_widgets", "widgets");
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});
		await runner.migrate();

		await runner.wipe();

		const tables = await conn.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
		);
		// wipe leaves a TRULY empty database — even the lock table (dropped after
		// the lock is released, so its release still worked).
		expect(tables).toEqual([]);
	});
});

describe("dryRun()", () => {
	it("does not create the tracking table (side-effect-free)", async () => {
		await writeMigration("001_widgets", "widgets");
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});

		const plan = await runner.dryRun();

		// It reported the pending migration...
		expect(plan.map((p) => p.name)).toEqual(["001_widgets"]);
		expect(plan[0]?.sql.length).toBeGreaterThan(0);
		// ...but wrote NOTHING: no tracking table on a fresh database.
		const tracking = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='ream_migrations'",
		);
		expect(tracking[0]?.n).toBe(0);
	});
});

describe("rollback({ batch })", () => {
	it("rolls back to a target batch, not just the latest", async () => {
		await writeMigration("001_a", "a");
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});
		await runner.migrate(); // batch 1: a

		await writeMigration("002_b", "b");
		await runner.migrate(); // batch 2: b
		await writeMigration("003_c", "c");
		await runner.migrate(); // batch 3: c

		// Default rollback removes only the latest batch (c).
		expect(await runner.rollback()).toEqual(["003_c"]);

		// Rolling back to batch 0 removes everything still applied (b, then a).
		const rolled = await runner.rollback({ batch: 0 });
		expect(rolled.sort()).toEqual(["001_a", "002_b"]);

		const remaining = await conn.query<{ name: string }>(
			"SELECT name FROM ream_migrations",
		);
		expect(remaining).toEqual([]);
	});

	it("is a no-op when the target is already the current batch", async () => {
		await writeMigration("001_a", "a");
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});
		await runner.migrate(); // batch 1

		expect(await runner.rollback({ batch: 1 })).toEqual([]);
	});
});
