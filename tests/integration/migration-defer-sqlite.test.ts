/**
 * `this.defer()` (Adonis Lucid parity): a migration seeds data into a table it
 * just created; the deferred callback runs after the schema statements, and the
 * migration is only recorded once the deferred work succeeds.
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
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-defer-"));
});

afterEach(async () => {
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
});

describe("migration this.defer()", () => {
	it("runs the deferred callback after the table is created, seeding it", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "001_seed.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() {
    this.schema.createTable('roles', (t) => { t.increments('id'); t.string('name') })
    this.defer(async (db) => {
      await db.execute("INSERT INTO roles (name) VALUES ('admin'), ('member')")
    })
  }
  async down() { this.schema.dropTable('roles') }
}`,
		);
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});
		await runner.migrate();

		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM roles",
		);
		expect(rows[0]?.n).toBe(2);
		// The migration is recorded as applied.
		const tracked = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM ream_migrations WHERE name = '001_seed'",
		);
		expect(tracked[0]?.n).toBe(1);
	});

	it("does not record the migration when a deferred callback throws", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "001_boom.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() {
    this.schema.createTable('widgets', (t) => { t.increments('id') })
    this.defer(async () => { throw new Error('seed failed') })
  }
  async down() { this.schema.dropTable('widgets') }
}`,
		);
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});
		await expect(runner.migrate()).rejects.toThrow(/seed failed/);

		// The schema DID run (table exists) but the migration is NOT recorded,
		// so it can be re-run once the seed is fixed.
		const table = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='widgets'",
		);
		expect(table[0]?.n).toBe(1);
		const tracked = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM ream_migrations WHERE name = '001_boom'",
		);
		expect(tracked[0]?.n).toBe(0);
	});
});
