/**
 * A quoted or schema-qualified function name is never an allowed call.
 *
 * The select()/having() expression guard checked its function allow-list only
 * for a BARE name followed by `(`. Postgres and SQLite also call a
 * double-quoted name and MySQL a backticked one, so `"pg_read_file"('/etc/passwd')`
 * and `"pg_sleep"(30)` reached the SQL verbatim, strict mode on or not.
 */
import { describe, expect, it } from "vitest";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../../src/query/DatabaseQueryBuilder.js";
import type { AtlasDialect } from "../../src/query/native.js";

const exec: QueryExecutor = {
	query: async () => [],
	execute: async () => ({ rowsAffected: 0 }),
};
const dialects: AtlasDialect[] = ["sqlite", "postgres", "mysql"];
const calls = [
	`"pg_read_file"('/etc/passwd')`,
	`"pg_sleep"(30)`,
	`"set_config"('role', 'postgres', false)`,
	"`sleep`(30)",
	`"pg_catalog"."pg_sleep"(1)`,
	"pg_catalog.pg_sleep(1)",
	"public.count(id)",
];

describe.each(dialects)("atlas > quoted function calls (%s)", (dialect) => {
	const users = () => new DatabaseQueryBuilder(exec, dialect, "users");

	it.each(calls)("select() refuses %s", (call) => {
		expect(() => users().select(`${call} as leak`).toSQL()).toThrow(
			expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
		);
		expect(() =>
			users()
				.select([`${call} as leak`])
				.toSQL(),
		).toThrow(expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }));
		expect(() => users().select({ leak: call }).toSQL()).toThrow(
			expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
		);
	});

	it.each(calls)("having() and orHaving() refuse %s", (call) => {
		const grouped = () => users().select("id").groupBy("id");
		expect(() => grouped().having(call, ">", 0).toSQL()).toThrow(
			expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
		);
		expect(() =>
			grouped().having("id", ">", 0).orHaving(call, ">", 0).toSQL(),
		).toThrow(expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }));
	});

	it.each(calls)("an aggregate over %s is refused", (call) => {
		expect(() => users().count(call).toSQL()).toThrow(
			expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
		);
		expect(() => users().sum(call).toSQL()).toThrow(
			expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
		);
	});

	it("still accepts an allowed call over a quoted column", () => {
		const q = dialect === "mysql" ? "`" : '"';
		const column = `${q}amount${q}`;
		expect(users().select(`sum(${column}) as total`).toSQL().sql).toContain(
			`sum(${column}) as total`,
		);
		expect(
			users()
				.select("id")
				.groupBy("id")
				.having(`count (${column})`, ">", 1)
				.toSQL().sql,
		).toContain(`HAVING count (${column}) >`);
	});
});
