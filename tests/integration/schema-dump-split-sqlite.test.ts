/**
 * Splitting a schema dump into statements.
 *
 * `loadDump` split the file on every `;`, including those inside string
 * literals, quoted identifiers, comments, Postgres dollar-quoted bodies and
 * SQLite trigger bodies (`BEGIN …; END`). A dump with a CHECK on `'a;b'` or a
 * trigger could not be loaded, and a `;` inside a quoted name ended the
 * statement early — the rest of the name ran as SQL.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";
import { MigrationRunner } from "../../src/schema/MigrationRunner.js";
import { SchemaDumper } from "../../src/schema/SchemaDumper.js";
import { splitSqlStatements } from "../../src/schema/sqlSplit.js";

describe("atlas > splitSqlStatements", () => {
	it("splits only on a statement-ending ';'", () => {
		const sql = [
			"-- header; with a semicolon",
			"CREATE TABLE \"a;b\" (x text CHECK (x <> 'p;q'), y text DEFAULT 'it''s;'); /* c; */",
			"CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; $body$ LANGUAGE sql;",
			"CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO log VALUES (CASE WHEN 1 THEN 'x;' END); UPDATE a SET x = 1; END;",
			"INSERT INTO m (name) VALUES ('z')",
		].join("\n");
		expect(splitSqlStatements(sql, "postgres")).toEqual([
			"-- header; with a semicolon\nCREATE TABLE \"a;b\" (x text CHECK (x <> 'p;q'), y text DEFAULT 'it''s;')",
			"/* c; */\nCREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; $body$ LANGUAGE sql",
			"CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO log VALUES (CASE WHEN 1 THEN 'x;' END); UPDATE a SET x = 1; END",
			"INSERT INTO m (name) VALUES ('z')",
		]);
	});

	it("reads MySQL backslash escapes and backtick identifiers", () => {
		const sql =
			"CREATE TABLE `t;x` (c varchar(9) DEFAULT 'a\\';b' COMMENT 'q;');\nSELECT 1;";
		expect(splitSqlStatements(sql, "mysql")).toEqual([
			"CREATE TABLE `t;x` (c varchar(9) DEFAULT 'a\\';b' COMMENT 'q;')",
			"SELECT 1",
		]);
	});

	it("drops chunks that are only comments", () => {
		expect(
			splitSqlStatements("-- a\n;\n/* b */;\nSELECT 1;\n-- end\n", "sqlite"),
		).toEqual(["SELECT 1"]);
	});
});

describe("atlas > SQLite dump with a trigger and a ';' literal", () => {
	it("loads back into a fresh database", async () => {
		const src = await createNapiConnection("sqlite::memory:", 1, 1);
		const dst = await createNapiConnection("sqlite::memory:", 1, 1);
		const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-split-"));
		try {
			await src.execute(
				"CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT CHECK (note <> 'a;b'))",
			);
			await src.execute("CREATE TABLE audit (msg TEXT)");
			await src.execute(
				"CREATE TRIGGER items_log AFTER INSERT ON items BEGIN INSERT INTO audit VALUES ('added;' || NEW.id); END",
			);
			const dumpPath = path.join(dir, "schema.sql");
			const dumper = new SchemaDumper(src, { dumpPath });
			await dumper.run();
			expect(dumper.error).toBeUndefined();

			await new MigrationRunner(
				{
					execute: async (sql, params) => {
						await dst.execute(sql, params);
					},
					query: (sql, params) => dst.query(sql, params),
					close: async () => {},
				},
				{ dialect: "sqlite" },
			).loadDump(dumpPath);

			await dst.execute("INSERT INTO items (id, note) VALUES (1, 'ok')");
			expect(await dst.query("SELECT msg FROM audit")).toEqual([
				{ msg: "added;1" },
			]);
			await expect(
				dst.execute("INSERT INTO items (id, note) VALUES (2, 'a;b')"),
			).rejects.toThrow();
		} finally {
			await src.close();
			await dst.close();
			await fsp.rm(dir, { recursive: true, force: true });
		}
	});
});
