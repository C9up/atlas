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

	it("refuses to acquire when another holder owns the lock, without clobbering it", async () => {
		// Simulate another process holding the lock under its own token.
		await runner().migrate(); // creates + seeds the lock table
		await conn.execute(
			"UPDATE ream_migrations_lock SET is_locked = 1, locked_by = 'other-process'",
		);

		await expect(runner().rollback()).rejects.toThrow(
			/another migration is already running/i,
		);

		// Atomicity: our conditional UPDATE (WHERE is_locked = 0) matched no row,
		// so the other holder's token is untouched — we never stole/cleared it.
		const rows = await conn.query<{ is_locked: number; locked_by: string }>(
			"SELECT is_locked, locked_by FROM ream_migrations_lock",
		);
		expect(rows[0]?.is_locked).toBe(1);
		expect(rows[0]?.locked_by).toBe("other-process");
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

	it("forceUnlock() clears a stuck lock so migrations can proceed (Lucid migration:unlock)", async () => {
		await runner().migrate(); // creates the lock table
		// Simulate a process killed mid-migrate: lock stuck at 1 with a stale token.
		await conn.execute(
			"UPDATE ream_migrations_lock SET is_locked = 1, locked_by = 'dead-process' WHERE id = 1",
		);
		// A normal run is now wedged.
		await expect(runner().rollback()).rejects.toThrow(/already running/i);

		// forceUnlock clears it and reports it was held.
		expect(await runner().forceUnlock()).toBe(true);
		// Now a run proceeds again.
		await expect(runner().migrate()).resolves.toEqual([]);
	});

	it("does not create a lock table when disableLocks is set", async () => {
		await runner(true).migrate();
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='ream_migrations_lock'",
		);
		expect(rows[0]?.n).toBe(0);
	});

	it("upgrades an OLD lock table (no locked_by column) instead of crashing", async () => {
		// Simulate a lock table created by an earlier atlas: id + is_locked only.
		await conn.execute(
			"CREATE TABLE ream_migrations_lock (id INTEGER PRIMARY KEY AUTOINCREMENT, is_locked INTEGER NOT NULL)",
		);
		await conn.execute(
			"INSERT INTO ream_migrations_lock (is_locked) VALUES (0)",
		);

		// migrate() must add the missing locked_by column, not crash on UPDATE.
		expect(await runner().migrate()).toEqual(["001_t"]);
		const cols = await conn.query<{ name: string }>(
			"SELECT name FROM pragma_table_info('ream_migrations_lock')",
		);
		expect(cols.map((c) => c.name)).toContain("locked_by");
	});

	it("operates on the fixed id=1 row, so a stray row can't break the lock (never DELETEs)", async () => {
		await runner().migrate(); // creates the lock table (row id=1)
		// A stray row (some other id) must NOT affect the lock, and must NOT be
		// deleted — the redesign never DELETEs (which could wipe a held lock).
		await conn.execute(
			"INSERT INTO ream_migrations_lock (id, is_locked, locked_by) VALUES (99, 1, 'other')",
		);

		// The lock (id=1) is free, so the next run proceeds normally...
		await expect(runner().migrate()).resolves.toEqual([]);
		// ...and the stray row is untouched (no destructive recovery).
		const stray = await conn.query<{ locked_by: string }>(
			"SELECT locked_by FROM ream_migrations_lock WHERE id = 99",
		);
		expect(stray[0]?.locked_by).toBe("other");
	});
});
