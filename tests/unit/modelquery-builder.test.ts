import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
	setAtlasStrictMode,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("widgets_b")
class Widget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@Column() declare status: string;
	@Column() declare age: number;
}

@Entity("public.things")
class Thing extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare label: string;
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

	it("orderByRaw keeps its place among the plain orderBy terms", () => {
		const out = sql((b) =>
			b.orderBy("name").orderByRaw("age DESC NULLS LAST").orderBy("id", "desc"),
		);
		expect(out).toContain(
			`ORDER BY "name" ASC, age DESC NULLS LAST, "id" DESC`,
		);
	});

	it("groupByRaw keeps its place among the plain groupBy terms", () => {
		const out = sql((b) =>
			b.groupBy("status").groupByRaw("DATE_TRUNC('day', created_at)"),
		);
		expect(out).toContain(`GROUP BY "status", DATE_TRUNC('day', created_at)`);
	});

	it("orderByRaw/groupByRaw are disabled in strict mode, like whereRaw", () => {
		setAtlasStrictMode(true);
		try {
			expect(() => sql((b) => b.orderByRaw("RANDOM()"))).toThrow(
				/strict mode/i,
			);
			expect(() => sql((b) => b.groupByRaw("x"))).toThrow(/strict mode/i);
			// The typed forms stay available — that's the point of the gate.
			expect(() => sql((b) => b.orderBy("name"))).not.toThrow();
		} finally {
			setAtlasStrictMode(false);
		}
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

	it("skipLocked / noWait without a base lock throw (no silent no-op)", () => {
		expect(() => sql((b) => b.skipLocked(), "postgres")).toThrow(
			/requires a base row lock/,
		);
		expect(() => sql((b) => b.noWait(), "postgres")).toThrow(
			/requires a base row lock/,
		);
		// Order-independent: modifier chained before the base lock is still valid.
		expect(sql((b) => b.skipLocked().forUpdate(), "postgres")).toMatch(
			/FOR UPDATE SKIP LOCKED$/i,
		);
	});

	it("whereExpr rejects SQL keywords in the extra expression (safe arithmetic only)", () => {
		// Arithmetic extra is fine.
		expect(sql((b) => b.whereExpr("age", "+ 1", ">", 5))).toMatch(/"age" \+ 1/);
		// A bare SQL keyword that would alter predicate logic is rejected.
		expect(() => sql((b) => b.whereExpr("age", "OR status", ">", 0))).toThrow(
			/SQL keyword|arithmetic/i,
		);
		expect(() => sql((b) => b.whereExpr("age", "AND 1", "=", 1))).toThrow(
			/SQL keyword|arithmetic/i,
		);
	});

	it("select() validates a bare identifier like where/orderBy (typo raises)", () => {
		expect(() => sql((b) => b.select("nope_not_a_col"))).toThrow(
			/does not exist|valid column/i,
		);
		// Expressions / * are left untouched (not validated).
		expect(sql((b) => b.select("COUNT(*) AS n"))).toMatch(/COUNT\(\*\)/i);
	});

	it("join onVal binds the value as a parameter (AdonisJS/Knex parity)", () => {
		const query = q();
		query.innerJoin("users", (j) =>
			j.on("users.id", "orders.user_id").andOnVal("orders.status", "paid"),
		);
		const { sql, params } = query.toSQL();
		expect(sql).toMatch(/JOIN .*ON .*=.*AND .*= \?/i);
		expect(params).toContain("paid");
	});

	it("join identifiers are validated before quoting (injection rejected)", () => {
		expect(() =>
			sql((b) =>
				b.innerJoin("users", (j) => j.on('users"."id', "orders.user_id")),
			),
		).toThrow(/valid|identifier|expected \[table/i);
		// A legitimate qualified identifier is fine.
		expect(
			sql((b) => b.innerJoin("users", (j) => j.on("users.id", "orders.uid"))),
		).toMatch(/JOIN/i);
	});

	it("the join TABLE name is validated too (not just the columns)", () => {
		expect(() => sql((b) => b.crossJoin('evil"table'))).toThrow(
			/valid|identifier|expected \[table/i,
		);
		expect(() =>
			sql((b) => b.innerJoin('t"x', (j) => j.on("a.b", "c.d"))),
		).toThrow(/valid|identifier/i);
	});

	it("a schema-qualified base table + join projects schema.table.column", () => {
		const query = new BaseRepository(Thing, db(), {
			dialect: "postgres",
		}).query();
		query.innerJoin("owners", (j) =>
			j.on("owners.id", "public.things.owner_id"),
		);
		const { sql } = query.toSQL();
		// Anti-clobber select must quote all three segments (was rejected before).
		expect(sql).toMatch(/"public"\."things"\."id"/);
		expect(sql).toMatch(/"public"\."things"\."label"/);
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
			{ column: "active", operator: "=", value: true, type: "and" },
			{ column: "role_id", operator: "IN", value: [1, 2], type: "and" },
			{ column: "banned", operator: "!=", value: true, type: "and" },
			{ column: "tier", operator: "NOT IN", value: ["free"], type: "and" },
		]);
	});

	it("wherePivotIn is an alias of whereInPivot", () => {
		const a = q();
		a.wherePivotIn("x", [1]);
		expect([...a.pivotConstraints]).toEqual([
			{ column: "x", operator: "IN", value: [1], type: "and" },
		]);
	});

	it("records the or/and form of each pivot filter", () => {
		const query = q();
		query
			.orWherePivot("active", true)
			.orWhereInPivot("role_id", [1])
			.orWhereNotPivot("banned", true)
			.orWhereNotInPivot("tier", ["free"])
			.whereNullPivot("revoked_at")
			.orWhereNotNullPivot("granted_at");
		expect([...query.pivotConstraints]).toEqual([
			{ column: "active", operator: "=", value: true, type: "or" },
			{ column: "role_id", operator: "IN", value: [1], type: "or" },
			{ column: "banned", operator: "!=", value: true, type: "or" },
			{ column: "tier", operator: "NOT IN", value: ["free"], type: "or" },
			{ column: "revoked_at", operator: "IS NULL", value: null, type: "and" },
			{
				column: "granted_at",
				operator: "IS NOT NULL",
				value: null,
				type: "or",
			},
		]);
	});

	it("treats andWherePivot as wherePivot", () => {
		const a = q().andWherePivot("active", true);
		expect([...a.pivotConstraints]).toEqual([
			{ column: "active", operator: "=", value: true, type: "and" },
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
