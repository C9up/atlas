/**
 * A migration dry run never writes.
 *
 * `dryRun()` ran each migration's `up()` with the live connection in reach:
 * an `up()` that wrote through the application's database — a query builder
 * insert, a raw statement, `execute`, a transaction — wrote for real while
 * the caller believed it was only previewing SQL. Every write is now refused
 * for the duration of a dry run; catalog reads (`schema.hasTable`) still work.
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
import { isReadOnlyStatement } from "../../src/schema/dryRunScope.js";
import { MigrationRunner } from "../../src/schema/MigrationRunner.js";
import { clearDb, setDb } from "../../src/services/db.js";

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;
const DB_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/services/db.ts"),
).href;

let conn: AsyncDatabaseConnection;
let dir: string;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE audit (msg TEXT)");
	await conn.execute("INSERT INTO audit VALUES ('kept')");
	setDb(conn);
	dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-dry-run-"));
});

afterEach(async () => {
	clearDb(conn);
	await conn.close();
	await fsp.rm(dir, { recursive: true, force: true });
});

function runner(): MigrationRunner {
	return new MigrationRunner(conn, { migrationsDir: dir, dialect: "sqlite" });
}

async function migration(write: string): Promise<void> {
	await fsp.writeFile(
		path.join(dir, "001_widgets.ts"),
		`import { Migration } from '${MIGRATION_SRC}'
import db, { getDb } from '${DB_SRC}'
export default class extends Migration {
  async up() {
    const exists = await this.schema.hasTable('audit')
    this.schema.createTable('widgets', (t) => { t.increments('id') })
    if (exists) { ${write} }
  }
  async down() { this.schema.dropTable('widgets') }
}`,
	);
}

async function audit(): Promise<unknown[]> {
	return conn.query("SELECT msg FROM audit");
}

describe("atlas > a migration dry run is read-only", () => {
	it.each([
		{
			label: "a query builder insert",
			write: "await db.table('audit').insert({ msg: 'x' })",
		},
		{
			label: "a raw statement",
			write: 'await db.rawQuery("DELETE FROM audit")',
		},
		{ label: "execute", write: 'await getDb()?.execute("DELETE FROM audit")' },
		{
			label: "a transaction",
			write:
				'await db.transaction(async (trx) => { await trx.query("DELETE FROM audit") })',
		},
		{
			label: "a write hidden in a WITH",
			write:
				'await db.rawQuery("WITH gone AS (DELETE FROM audit RETURNING msg) SELECT * FROM gone")',
		},
	])("refuses $label and leaves the database untouched", async ({ write }) => {
		await migration(write);
		await expect(runner().dryRun()).rejects.toMatchObject({
			code: "E_ATLAS_DRY_RUN_WRITE",
		});
		expect(await audit()).toEqual([{ msg: "kept" }]);
		const tables = await conn.query(
			"SELECT name FROM sqlite_master WHERE name IN ('widgets', 'ream_migrations')",
		);
		expect(tables).toEqual([]);
	});

	it("reads the catalog and returns the SQL", async () => {
		await migration("");
		const [result] = await runner().dryRun();
		expect(result?.name).toBe("001_widgets");
		expect(result?.sql.join("\n")).toContain('CREATE TABLE "widgets"');
		// Outside the dry run the connection writes again.
		await conn.execute("INSERT INTO audit VALUES ('after')");
		expect(await audit()).toHaveLength(2);
	});

	it("classifies read-only statements", () => {
		for (const sql of [
			"SELECT 1",
			"  /* c */ select name from sqlite_master",
			"PRAGMA table_info('t')",
			"SHOW TABLES",
			"WITH x AS (SELECT 1) SELECT * FROM x",
			"EXPLAIN SELECT 1",
			"(SELECT 1)",
		]) {
			expect(isReadOnlyStatement(sql), sql).toBe(true);
		}
		for (const sql of [
			"INSERT INTO t VALUES (1)",
			"WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
			"SELECT * INTO copy FROM t",
			"PRAGMA foreign_keys = OFF",
			"EXPLAIN ANALYZE DELETE FROM t",
			"SELECT 1; DELETE FROM t",
		]) {
			expect(isReadOnlyStatement(sql), sql).toBe(false);
		}
	});
});
