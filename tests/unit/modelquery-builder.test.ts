import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("widgets_b")
class Widget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@Column() declare status: string;
	@Column() declare age: number;
}

function db() {
	return wrapPrepareMock({
		prepare() {
			return { run: () => ({ changes: 0 }), all: () => [] };
		},
	});
}

function q(dialect: "sqlite" | "postgres" = "sqlite") {
	return new BaseRepository(Widget, db(), { dialect }).query();
}

/** Compile a builder chain to SQL without a database round-trip. */
function sql(
	build: (query: ReturnType<typeof q>) => unknown,
	dialect?: "sqlite" | "postgres",
) {
	const query = q(dialect);
	build(query);
	return query.toSQL().sql;
}

describe("atlas > ModelQuery builder → SQL", () => {
	it("whereNull / whereNotNull emit IS NULL / IS NOT NULL", () => {
		expect(sql((b) => b.whereNull("name"))).toMatch(/IS NULL/i);
		expect(sql((b) => b.whereNotNull("name"))).toMatch(/IS NOT NULL/i);
	});

	it("whereNot emits !=", () => {
		expect(sql((b) => b.whereNot("status", "x"))).toMatch(/!=|<>/);
	});

	it("whereColumn compares two columns (no value binding)", () => {
		const out = sql((b) => b.whereColumn("age", ">", "id"));
		expect(out).toMatch(/"age"\s*>\s*"id"/);
	});

	it("whereColumn rejects an injection-shaped identifier", () => {
		// The malicious identifier is rejected before any quoting — by the column
		// resolver (unknown column) and, as defense-in-depth, the strict-charset guard.
		expect(() =>
			sql((b) => b.whereColumn('age" OR "1"="1', "=", "id")),
		).toThrow(/does not exist|valid column identifier/);
	});

	it("whereColumn rejects a non-allowlisted operator", () => {
		expect(() => sql((b) => b.whereColumn("age", "EVIL", "id"))).toThrow(
			/not allowed/,
		);
	});

	it("whereIn / whereNotIn with an array", () => {
		expect(sql((b) => b.whereIn("status", ["a", "b"]))).toMatch(/IN\s*\(/i);
		expect(sql((b) => b.whereNotIn("status", ["a", "b"]))).toMatch(/NOT IN/i);
	});

	it("orWhere* family combines predicates with OR", () => {
		expect(sql((b) => b.where("status", "a").orWhereNull("name"))).toMatch(
			/OR.*IS NULL/i,
		);
		expect(sql((b) => b.where("status", "a").orWhereIn("age", [1, 2]))).toMatch(
			/OR.*IN\s*\(/i,
		);
		expect(sql((b) => b.where("status", "a").orWhereNot("age", 1))).toMatch(
			/OR.*(!=|<>)/i,
		);
		expect(
			sql((b) => b.where("status", "a").orWhereLike("name", "x%")),
		).toMatch(/OR.*LIKE/i);
	});

	it("whereIn with a subquery emits a nested SELECT", () => {
		const out = sql((b) =>
			b.whereIn("id", q().select("id").where("status", "active")),
		);
		expect(out).toMatch(/IN\s*\(\s*SELECT/i);
	});

	it("whereBetween / whereNotBetween", () => {
		expect(sql((b) => b.whereBetween("age", [1, 5]))).toMatch(/BETWEEN/i);
		expect(sql((b) => b.whereNotBetween("age", [1, 5]))).toMatch(
			/NOT BETWEEN/i,
		);
	});

	it("whereLike emits LIKE", () => {
		expect(sql((b) => b.whereLike("name", "a%"))).toMatch(/LIKE/i);
	});

	it("whereILike compiles (rewritten to LOWER(...) LIKE on sqlite)", () => {
		expect(sql((b) => b.whereILike("name", "a%"))).toMatch(/LIKE/i);
	});

	it("orWhere produces an OR branch", () => {
		const out = sql((b) => b.where("status", "a").orWhere("status", "b"));
		expect(out).toMatch(/OR/i);
	});

	it("a where callback produces a parenthesised group", () => {
		const out = sql((b) =>
			b.where((g) => g.where("status", "a").orWhere("status", "b")),
		);
		expect(out).toMatch(/\(/);
	});

	it("select narrows the projection", () => {
		const out = sql((b) => b.select(["id", "name"]));
		expect(out).toMatch(/"id"/);
		expect(out).toMatch(/"name"/);
		expect(out).not.toMatch(/SELECT \*/i);
	});

	it("distinct emits DISTINCT", () => {
		expect(sql((b) => b.distinct().select("status"))).toMatch(/DISTINCT/i);
	});

	it("orderBy / limit / offset", () => {
		const out = sql((b) => b.orderBy("age", "desc").limit(10).offset(5));
		expect(out).toMatch(/ORDER BY/i);
		expect(out).toMatch(/LIMIT/i);
		expect(out).toMatch(/OFFSET/i);
	});

	it("forUpdate emits FOR UPDATE on postgres", () => {
		expect(sql((b) => b.forUpdate(), "postgres")).toMatch(/FOR UPDATE/i);
	});

	it("forNoKeyUpdate / forKeyShare emit their Postgres lock clauses", () => {
		expect(sql((b) => b.forNoKeyUpdate(), "postgres")).toMatch(
			/FOR NO KEY UPDATE$/i,
		);
		expect(sql((b) => b.forKeyShare(), "postgres")).toMatch(/FOR KEY SHARE$/i);
	});

	it("skipLocked / noWait compose onto the base lock clause", () => {
		expect(sql((b) => b.forUpdate().skipLocked(), "postgres")).toMatch(
			/FOR UPDATE SKIP LOCKED$/i,
		);
		expect(sql((b) => b.forShare().noWait(), "postgres")).toMatch(
			/FOR SHARE NOWAIT$/i,
		);
	});

	it("whereRaw appends a raw predicate", () => {
		expect(sql((b) => b.whereRaw("age > ?", [18]))).toMatch(/age > /i);
	});

	it("pivot filters record constraints with the right operators (Lucid names)", () => {
		const query = q();
		query
			.wherePivot("active", true)
			.whereInPivot("role_id", [1, 2])
			.whereNotPivot("banned", true)
			.whereNotInPivot("tier", ["free"]);
		expect([...query.pivotConstraints]).toEqual([
			{ column: "active", operator: "=", value: true },
			{ column: "role_id", operator: "IN", value: [1, 2] },
			{ column: "banned", operator: "!=", value: true },
			{ column: "tier", operator: "NOT IN", value: ["free"] },
		]);
	});

	it("wherePivotIn is an alias of whereInPivot", () => {
		const a = q();
		a.wherePivotIn("x", [1]);
		expect([...a.pivotConstraints]).toEqual([
			{ column: "x", operator: "IN", value: [1] },
		]);
	});

	it("clone() yields an independent query", () => {
		const base = q().where("status", "a");
		const cloned = base.clone().where("name", "x");
		// The clone has the extra predicate; the original does not.
		expect(cloned.toSQL().sql).toMatch(/"name"/);
		expect(base.toSQL().sql).not.toMatch(/"name"/);
	});
});
