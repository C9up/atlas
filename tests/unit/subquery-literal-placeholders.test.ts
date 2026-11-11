/**
 * Embedding a sub-query renumbers its placeholders in SQL code only.
 *
 * The renumbering was a find/replace over the compiled sub-SQL, so a `$1`
 * the caller wrote inside a string literal was rewritten too:
 * `whereRaw("note = 'costs $1'")` in a sub-query after an outer binding
 * compared the note with 'costs $2'.
 */
import { describe, expect, it } from "vitest";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../../src/query/DatabaseQueryBuilder.js";

const exec: QueryExecutor = {
	query: async () => [],
	execute: async () => ({ rowsAffected: 0 }),
};

describe("atlas > a sub-query's literals survive placeholder renumbering", () => {
	it("keeps a literal dollar placeholder on postgres", () => {
		const sub = new DatabaseQueryBuilder(exec, "postgres", "u")
			.whereRaw("note = 'costs $1'")
			.where("a", 1)
			.select("id");
		const compiled = new DatabaseQueryBuilder(exec, "postgres", "t")
			.where("x", 5)
			.whereIn("id", sub)
			.toSQL()
			.toNative();
		expect(compiled.sql).toContain("note = 'costs $1'");
		expect(compiled.sql).toContain('"a" = $2');
		expect(compiled.bindings).toEqual([5, 1]);
	});

	it.each(["sqlite", "mysql"] as const)(
		"keeps a literal dollar placeholder on %s",
		(dialect) => {
			const sub = new DatabaseQueryBuilder(exec, dialect, "u")
				.whereRaw("note = 'costs $1'")
				.where("a", 1)
				.select("id");
			const compiled = new DatabaseQueryBuilder(exec, dialect, "t")
				.where("x", 5)
				.whereIn("id", sub)
				.toSQL()
				.toNative();
			expect(compiled.sql).toContain("note = 'costs $1'");
			expect(compiled.bindings).toEqual([5, 1]);
		},
	);
});
