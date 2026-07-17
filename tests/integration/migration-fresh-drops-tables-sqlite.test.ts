/**
 * #10: `MigrationRunner.fresh()` must be a REAL `migrate:fresh` — drop every
 * table directly (no `down()`) then re-migrate — not an alias of `refresh()`
 * (which rolls back via each migration's `down()`). The discriminating property:
 * fresh() succeeds even when a `down()` is broken; refresh() cannot.
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
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";

/** Wrap a napi connection as a MigrationRunner adapter (execute → void). */
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

// A migration whose down() is intentionally broken: refresh() (which calls
// down()) blows up, but fresh() drops the table directly and never runs it.
const BROKEN_DOWN = `
import { Migration } from '${MIGRATION_SRC}'
export default class CreateFrags extends Migration {
  async up() { this.schema.createTable('frags', (t) => { t.increments('id'); t.string('label', 50) }) }
  async down() { throw new Error('down() intentionally broken') }
}
`;

let conn: AsyncDatabaseConnection;
let tmpDir: string;

beforeEach(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-fresh-"));
	await fsp.writeFile(path.join(tmpDir, "001_frags.ts"), BROKEN_DOWN, "utf8");
});

afterEach(async () => {
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
});

describe("atlas > MigrationRunner.fresh() drops tables directly (#10)", () => {
	it("succeeds despite a broken down(), where refresh() fails", async () => {
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});

		const executed = await runner.migrate();
		expect(executed).toEqual(["001_frags"]);
		await conn.execute("INSERT INTO frags (label) VALUES ('x')");

		// refresh() calls down() → the broken down() aborts it.
		await expect(runner.refresh()).rejects.toThrow(
			/down\(\) intentionally broken/,
		);

		// fresh() drops the table outright (no down()) then re-migrates.
		const result = await runner.fresh();
		expect(result.rolled).toEqual([]);
		expect(result.executed).toEqual(["001_frags"]);

		// The table exists again and is empty — a genuine fresh rebuild.
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM frags",
		);
		expect(rows[0]?.n).toBe(0);

		// And the migration is tracked exactly once (batch 1 after re-init).
		const tracked = await conn.query<{ name: string }>(
			"SELECT name FROM ream_migrations",
		);
		expect(tracked.map((r) => r.name)).toEqual(["001_frags"]);
	});

	it("drops orphan tables no migration tracks", async () => {
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});
		await runner.migrate();
		// An untracked table left behind by something outside the migration set.
		await conn.execute("CREATE TABLE orphan_leftover (id INTEGER PRIMARY KEY)");

		await runner.fresh();

		const orphan = await conn.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='orphan_leftover'",
		);
		expect(orphan).toEqual([]);
	});
});
