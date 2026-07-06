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

@Entity("widgets_gb")
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

function sql(
	build: (query: ReturnType<typeof q>) => unknown,
	dialect?: "sqlite" | "postgres",
) {
	const query = q(dialect);
	build(query);
	return query.toSQL();
}

describe("atlas > ModelQuery groupBy / having / union / with", () => {
	it("groupBy emits GROUP BY with resolved columns", () => {
		expect(sql((b) => b.select("status").groupBy("status")).sql).toMatch(
			/GROUP BY "status"/i,
		);
	});

	it("having emits HAVING with a bound param", () => {
		const { sql: out, params } = sql((b) =>
			b.select("status").groupBy("status").having("COUNT(*)", ">", 2),
		);
		expect(out).toMatch(/HAVING COUNT\(\*\) > /i);
		expect(params).toContain(2);
	});

	it("orHaving combines with OR", () => {
		const { sql: out } = sql((b) =>
			b
				.select("status")
				.groupBy("status")
				.having("COUNT(*)", ">", 2)
				.orHaving("COUNT(*)", "<", 100),
		);
		expect(out).toMatch(/HAVING COUNT\(\*\) > .* OR COUNT\(\*\) < /i);
	});

	it("havingRaw appends a raw HAVING fragment with re-indexed bindings", () => {
		const { sql: out, params } = sql((b) =>
			b.select("status").groupBy("status").havingRaw("COUNT(*) > ?", [5]),
		);
		expect(out).toMatch(/HAVING.*COUNT\(\*\) > /i);
		expect(params).toContain(5);
	});

	it("union appends a UNION branch and merges params", () => {
		const { sql: out, params } = sql((b) =>
			b.where("status", "a").union(q().where("status", "b")),
		);
		expect(out).toMatch(/UNION \(/i);
		expect(out).not.toMatch(/UNION ALL/i);
		expect(params).toEqual(["a", "b"]);
	});

	it("unionAll appends a UNION ALL branch", () => {
		const { sql: out } = sql((b) =>
			b.where("status", "a").unionAll(q().where("status", "b")),
		);
		expect(out).toMatch(/UNION ALL \(/i);
	});

	it("with registers a CTE and re-indexes its params", () => {
		const { sql: out, params } = sql(
			(b) =>
				b.with("recent", q().where("status", "active")).where("age", ">", 1),
			"postgres",
		);
		expect(out).toMatch(/WITH "recent" AS \(/i);
		// CTE params come before the outer WHERE params.
		expect(params).toEqual(["active", 1]);
	});

	it("with rejects an invalid CTE name", () => {
		expect(() => q().with("bad name", q())).toThrow(/valid identifier/);
	});

	it("clone() carries groupBy / having / union independently", () => {
		const base = q().select("status").groupBy("status");
		const cloned = base.clone().having("COUNT(*)", ">", 1);
		expect(cloned.toSQL().sql).toMatch(/HAVING/i);
		expect(base.toSQL().sql).not.toMatch(/HAVING/i);
	});
});
