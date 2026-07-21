/**
 * Migration lock (Adonis Lucid / Knex parity): a `<tableName>_lock` table with
 * an `is_locked` flag, NOT a Postgres advisory lock — so it works on every
 * dialect and is fully exercised here against real in-memory SQLite.
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

function toAdapter(conn: AsyncDatabaseConnection): DatabaseAdapter {
	return {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: (sql, params) => conn.query(sql, params),
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
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-lock-"));
	await fsp.writeFile(
		path.join(tmpDir, "001_t.ts"),
		`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('t', (x) => { x.increments('id') }) }
  async down() { this.schema.dropTable('t') }
}`,
	);
});

afterEach(async () => {
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
});

function runner(disableLocks = false): MigrationRunner {
	return new MigrationRunner(toAdapter(conn), {
		migrationsDir: tmpDir,
		disableLocks,
	});
}

describe("migration lock (lock table)", () => {
	it("migrates and leaves the lock released (is_locked back to 0)", async () => {
		expect(await runner().migrate()).toEqual(["001_t"]);

		const rows = await conn.query<{ is_locked: number }>(
			"SELECT is_locked FROM ream_migrations_lock",
		);
		expect(rows[0]?.is_locked).toBe(0);
	});

	it("refuses to migrate while the lock is held", async () => {
		// Simulate another process holding the lock.
		await runner().migrate(); // creates + seeds the lock table
		await conn.execute("UPDATE ream_migrations_lock SET is_locked = 1");

		await expect(runner().rollback()).rejects.toThrow(
			/another migration is already running/i,
		);
	});

	it("releases the lock even when a migration throws", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "002_boom.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.raw('THIS IS NOT SQL') }
  async down() {}
}`,
		);
		await expect(runner().migrate()).rejects.toThrow();

		// The finally-release ran: the lock is free, so a later run can proceed.
		const rows = await conn.query<{ is_locked: number }>(
			"SELECT is_locked FROM ream_migrations_lock",
		);
		expect(rows[0]?.is_locked).toBe(0);
	});

	it("does not create a lock table when disableLocks is set", async () => {
		await runner(true).migrate();
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='ream_migrations_lock'",
		);
		expect(rows[0]?.n).toBe(0);
	});
});
