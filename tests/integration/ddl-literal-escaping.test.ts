/**
 * String literals in DDL — defaultTo, enum values, CHECK values, column and
 * table comments — on MySQL, where a backslash is an escape character inside
 * a string literal (default sql_mode, no NO_BACKSLASH_ESCAPES).
 *
 * Only `'` was doubled, so `defaultTo('C:\\temp\\new')` stored a TAB and a LF,
 * and a value ending in `\\'` closed the literal early: the rest of the value
 * became DDL. Backslashes are now doubled on MySQL; Postgres and SQLite keep
 * standard literals, where a backslash is an ordinary character.
 *
 * The statement shapes run everywhere; the round trip runs with
 * ATLAS_TEST_MYSQL_URL.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import type { AtlasDialect } from "../../src/query/native.js";
import { TableBuilder } from "../../src/schema/TableBuilder.js";

const WINDOWS_PATH = "C:\\temp\\new";
const BREAKOUT = "x\\', `injected` INT DEFAULT 42 -- ";
const TRAILING = "ends with backslash\\";

function probeTable(dialect: AtlasDialect): string[] {
	const b = new TableBuilder("probe_ddl");
	b.integer("id").primary();
	b.string("path", 100).defaultTo(WINDOWS_PATH);
	b.string("label", 100).defaultTo(BREAKOUT);
	b.string("tail", 100).defaultTo(TRAILING).comment(BREAKOUT);
	b.enum("kind", ["a\\b", TRAILING]).nullable();
	b.string("code", 50).nullable().checkIn(["q\\'", "plain"]);
	b.comment(TRAILING);
	return b.toStatements(dialect);
}

describe("atlas > DDL string literals", () => {
	it("doubles backslashes on MySQL", () => {
		const sql = probeTable("mysql").join("\n");
		expect(sql).toContain("DEFAULT 'C:\\\\temp\\\\new'");
		expect(sql).toContain("DEFAULT 'x\\\\'', `injected` INT DEFAULT 42 -- '");
		expect(sql).toContain("'ends with backslash\\\\'");
	});

	it("keeps standard literals on Postgres and SQLite", () => {
		for (const dialect of ["postgres", "sqlite"] as const) {
			const sql = probeTable(dialect).join("\n");
			expect(sql).toContain("'C:\\temp\\new'");
			expect(sql).toContain("'x\\'', `injected` INT DEFAULT 42 -- '");
		}
	});

	it("refuses a NUL byte in a default", () => {
		const b = new TableBuilder("probe_nul");
		b.string("v", 10).defaultTo("a\0b");
		expect(() => b.toStatements("mysql")).toThrow(/NUL/);
	});
});

const MYSQL_URL = process.env.ATLAS_TEST_MYSQL_URL ?? "";
const describeMysql = MYSQL_URL ? describe : describe.skip;

describeMysql("atlas > DDL string literals round-trip on MySQL", () => {
	let conn: AsyncDatabaseConnection;

	beforeAll(async () => {
		conn = await createNapiConnection(MYSQL_URL, 1, 1);
		await conn.execute("DROP TABLE IF EXISTS probe_ddl");
		for (const stmt of probeTable("mysql")) await conn.execute(stmt);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS probe_ddl");
		await conn.close();
	});

	it("stores each default exactly and adds no column", async () => {
		await conn.execute("INSERT INTO probe_ddl (id) VALUES (1)");
		const rows = await conn.query<Record<string, unknown>>(
			"SELECT * FROM probe_ddl",
		);
		expect(rows).toEqual([
			{
				id: 1,
				path: WINDOWS_PATH,
				label: BREAKOUT,
				tail: TRAILING,
				kind: null,
				code: null,
			},
		]);
	});

	it("keeps enum, check and comment values intact", async () => {
		await conn.execute(
			"INSERT INTO probe_ddl (id, kind, code) VALUES (2, ?, ?)",
			["a\\b", "q\\'"],
		);
		const comments = await conn.query<{ c: string }>(
			"SELECT CAST(COLUMN_COMMENT AS CHAR) AS c FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'probe_ddl' AND column_name = 'tail'",
		);
		expect(comments).toEqual([{ c: BREAKOUT }]);
		const table = await conn.query<{ c: string }>(
			"SELECT CAST(TABLE_COMMENT AS CHAR) AS c FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'probe_ddl'",
		);
		expect(table).toEqual([{ c: TRAILING }]);
		await expect(
			conn.execute("INSERT INTO probe_ddl (id, code) VALUES (3, 'q')"),
		).rejects.toThrow();
	});
});
