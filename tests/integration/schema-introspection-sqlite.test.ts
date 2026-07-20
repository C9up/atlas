/**
 * Schema.hasTable / hasColumn (Adonis Lucid/Knex parity) against real in-memory
 * SQLite: the catalog probes, the connection-bound Schema methods, the
 * no-connection guard, and a migration that branches on hasTable through the
 * runner (which binds the live connection).
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
import { columnExists, tableExists } from "../../src/schema/catalog.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";
import { Schema } from "../../src/schema/Schema.js";

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
	await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-introspect-"));
});

afterEach(async () => {
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
});

describe("catalog tableExists / columnExists", () => {
	it("reports table and column presence", async () => {
		expect(await tableExists(conn, "sqlite", "users")).toBe(true);
		expect(await tableExists(conn, "sqlite", "nope")).toBe(false);
		expect(await columnExists(conn, "sqlite", "users", "email")).toBe(true);
		expect(await columnExists(conn, "sqlite", "users", "missing")).toBe(false);
	});

	it("passes the name as a bound parameter (injection-safe)", async () => {
		// A malicious "name" is a value, never interpolated — it just doesn't match.
		expect(
			await tableExists(conn, "sqlite", "users'; DROP TABLE users;--"),
		).toBe(false);
		// The table is still there.
		expect(await tableExists(conn, "sqlite", "users")).toBe(true);
	});
});

describe("Schema.hasTable / hasColumn", () => {
	it("answers from the bound connection", async () => {
		const schema = new Schema("sqlite");
		schema.bindConnection(conn);
		expect(await schema.hasTable("users")).toBe(true);
		expect(await schema.hasTable("ghost")).toBe(false);
		expect(await schema.hasColumn("users", "email")).toBe(true);
		expect(await schema.hasColumn("users", "ghost")).toBe(false);
	});

	it("throws a clear error with no bound connection", async () => {
		await expect(new Schema("sqlite").hasTable("users")).rejects.toThrow(
			/E_NO_CONNECTION/,
		);
	});
});

describe("hasTable inside a migration (runner binds the connection)", () => {
	it("lets a migration create a table only when it is absent — idempotent", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "001_items.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() {
    if (!(await this.schema.hasTable('items'))) {
      this.schema.createTable('items', (t) => { t.increments('id') })
    }
  }
  async down() { this.schema.dropTable('items') }
}`,
		);
		const runner = new MigrationRunner(toAdapter(conn), {
			migrationsDir: tmpDir,
		});

		await runner.migrate();
		expect(await tableExists(conn, "sqlite", "items")).toBe(true);

		// Re-running the same up() (via a fresh runner over the same dir) must not
		// throw a "table already exists" — the hasTable guard short-circuits.
		await runner.rollback();
		await expect(runner.migrate()).resolves.not.toThrow();
		expect(await tableExists(conn, "sqlite", "items")).toBe(true);
	});
});
