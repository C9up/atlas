/**
 * The HAVING family of the upstream chainable contract, on both builders:
 * the and and or spelling of every having method, the callback (grouped)
 * and raw forms of having()/orHaving(), and havingExists on the model
 * builder. Each method was missing — a TypeError at runtime for code ported
 * from the upstream ORM, while the WHERE side had every variant.
 */
import "reflect-metadata";
import { afterEach, describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
	RawSql,
	setAtlasStrictMode,
} from "../../src/index.js";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../../src/query/DatabaseQueryBuilder.js";
import type { AtlasDialect } from "../../src/query/native.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("having_orders")
class Order extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare status: string;
	@Column() declare total: number;
}

@Entity("having_posts")
class Post extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare state: string;
}

const exec: QueryExecutor = {
	query: async () => [],
	execute: async () => ({ rowsAffected: 0 }),
};

function mockDb() {
	return wrapPrepareMock({
		prepare() {
			return { run: () => ({ changes: 0 }), all: () => [] };
		},
	});
}

const dialects: AtlasDialect[] = ["sqlite", "postgres", "mysql"];

afterEach(() => setAtlasStrictMode(false));

/**
 * The calls shared by both builders, each with the HAVING it must compile
 * to after `having("status", "=", "a")`. `q` is the dialect's identifier
 * quote; `sql` is the Knex-normalized `?` form.
 */
interface Builder {
	having(column: string, operator: string, value: unknown): unknown;
	toSQL(): { sql: string; bindings: unknown[] };
}

describe.each(dialects)("atlas > HAVING family (%s)", (dialect) => {
	const q = dialect === "mysql" ? "`" : '"';
	const id = (name: string) => `${q}${name}${q}`;
	const S = id("status");

	const cases: Array<
		[
			string,
			(b: DatabaseQueryBuilder) => unknown,
			(m: ReturnType<BaseRepository<Order>["query"]>) => unknown,
			string,
			unknown[],
		]
	> = [
		[
			"andHaving",
			(b) => b.andHaving("total", ">", 1),
			(m) => m.andHaving("total", ">", 1),
			`AND ${id("total")} > ?`,
			[1],
		],
		[
			"orHavingIn",
			(b) => b.orHavingIn("status", ["b", "c"]),
			(m) => m.orHavingIn("status", ["b", "c"]),
			`OR ${S} IN (?, ?)`,
			["b", "c"],
		],
		[
			"andHavingIn",
			(b) => b.andHavingIn("status", ["b"]),
			(m) => m.andHavingIn("status", ["b"]),
			`AND ${S} IN (?)`,
			["b"],
		],
		[
			"orHavingNotIn",
			(b) => b.orHavingNotIn("status", ["b"]),
			(m) => m.orHavingNotIn("status", ["b"]),
			`OR ${S} NOT IN (?)`,
			["b"],
		],
		[
			"andHavingNotIn",
			(b) => b.andHavingNotIn("status", ["b"]),
			(m) => m.andHavingNotIn("status", ["b"]),
			`AND ${S} NOT IN (?)`,
			["b"],
		],
		[
			"orHavingNull",
			(b) => b.orHavingNull("total"),
			(m) => m.orHavingNull("total"),
			`OR ${id("total")} IS NULL`,
			[],
		],
		[
			"andHavingNull",
			(b) => b.andHavingNull("total"),
			(m) => m.andHavingNull("total"),
			`AND ${id("total")} IS NULL`,
			[],
		],
		[
			"orHavingNotNull",
			(b) => b.orHavingNotNull("total"),
			(m) => m.orHavingNotNull("total"),
			`OR ${id("total")} IS NOT NULL`,
			[],
		],
		[
			"andHavingNotNull",
			(b) => b.andHavingNotNull("total"),
			(m) => m.andHavingNotNull("total"),
			`AND ${id("total")} IS NOT NULL`,
			[],
		],
		[
			"orHavingBetween",
			(b) => b.orHavingBetween("total", [1, 5]),
			(m) => m.orHavingBetween("total", [1, 5]),
			`OR ${id("total")} BETWEEN ? AND ?`,
			[1, 5],
		],
		[
			"andHavingBetween",
			(b) => b.andHavingBetween("total", [1, 5]),
			(m) => m.andHavingBetween("total", [1, 5]),
			`AND ${id("total")} BETWEEN ? AND ?`,
			[1, 5],
		],
		[
			"orHavingNotBetween",
			(b) => b.orHavingNotBetween("total", [1, 5]),
			(m) => m.orHavingNotBetween("total", [1, 5]),
			`OR ${id("total")} NOT BETWEEN ? AND ?`,
			[1, 5],
		],
		[
			"andHavingNotBetween",
			(b) => b.andHavingNotBetween("total", [1, 5]),
			(m) => m.andHavingNotBetween("total", [1, 5]),
			`AND ${id("total")} NOT BETWEEN ? AND ?`,
			[1, 5],
		],
		[
			"orHavingRaw",
			(b) => b.orHavingRaw("count(*) > ?", [2]),
			(m) => m.orHavingRaw("count(*) > ?", [2]),
			"OR (count(*) > ?)",
			[2],
		],
		[
			"andHavingRaw",
			(b) => b.andHavingRaw("count(*) > ?", [2]),
			(m) => m.andHavingRaw("count(*) > ?", [2]),
			"AND (count(*) > ?)",
			[2],
		],
		[
			"having(callback)",
			(b) =>
				b.having((g) => {
					g.having("total", ">", 1).orHaving("total", "<", 0);
				}),
			(m) =>
				m.having((g) => {
					g.having("total", ">", 1).orHaving("total", "<", 0);
				}),
			`AND (${id("total")} > ? OR ${id("total")} < ?)`,
			[1, 0],
		],
		[
			"orHaving(callback)",
			(b) =>
				b.orHaving((g) => {
					g.havingNull("total").havingIn("status", ["x"]);
				}),
			(m) =>
				m.orHaving((g) => {
					g.havingNull("total").havingIn("status", ["x"]);
				}),
			`OR (${id("total")} IS NULL AND ${S} IN (?))`,
			["x"],
		],
		[
			"andHaving(callback)",
			(b) => b.andHaving((g) => g.orHavingRaw("sum(total) > ?", [9])),
			(m) => m.andHaving((g) => g.orHavingRaw("sum(total) > ?", [9])),
			"AND ((sum(total) > ?))",
			[9],
		],
		[
			"having(db.raw)",
			(b) => b.having(new RawSql("count(*) > ?", [3])),
			(m) => m.having(new RawSql("count(*) > ?", [3])),
			"AND (count(*) > ?)",
			[3],
		],
		[
			"orHaving(db.raw)",
			(b) => b.orHaving(new RawSql("count(*) > ?", [3])),
			(m) => m.orHaving(new RawSql("count(*) > ?", [3])),
			"OR (count(*) > ?)",
			[3],
		],
	];

	const expectHaving = (
		builder: Builder,
		tail: string,
		bindings: unknown[],
	) => {
		const { sql, bindings: all } = builder.toSQL();
		expect(sql.endsWith(`HAVING ${S} = ? ${tail}`)).toBe(true);
		expect(all).toEqual(["a", ...bindings]);
	};

	it.each(cases)(
		"%s on the database builder",
		(_name, onDb, _onModel, tail, bindings) => {
			const b = new DatabaseQueryBuilder(exec, dialect, "having_orders")
				.groupBy("status")
				.having("status", "=", "a");
			onDb(b);
			expectHaving(b, tail, bindings);
		},
	);

	it.each(cases)(
		"%s on the model builder",
		(_name, _onDb, onModel, tail, bindings) => {
			const m = new BaseRepository(Order, mockDb(), { dialect })
				.query()
				.groupBy("status")
				.having("status", "=", "a");
			onModel(m);
			expectHaving(m, tail, bindings);
		},
	);

	it("a grouped having opens the clause when it comes first", () => {
		const b = new DatabaseQueryBuilder(exec, dialect, "having_orders")
			.groupBy("status")
			.having((g) => g.having("total", ">", 1).orHaving("total", "<", 0));
		expect(b.toSQL().sql).toMatch(
			new RegExp(
				`GROUP BY ${S} HAVING \\(${id("total")} > \\? OR ${id("total")} < \\?\\)$`,
			),
		);
	});

	it("the raw forms stay behind strict mode", () => {
		setAtlasStrictMode(true);
		const b = () =>
			new DatabaseQueryBuilder(exec, dialect, "having_orders").groupBy(
				"status",
			);
		const m = () =>
			new BaseRepository(Order, mockDb(), { dialect })
				.query()
				.groupBy("status");
		for (const call of [
			() => b().orHavingRaw("count(*) > 1"),
			() => b().andHavingRaw("count(*) > 1"),
			() => b().having(new RawSql("count(*) > 1")),
			() => b().orHaving(new RawSql("count(*) > 1")),
			() => m().orHavingRaw("count(*) > 1"),
			() => m().andHavingRaw("count(*) > 1"),
			() => m().having(new RawSql("count(*) > 1")),
			() => m().orHaving(new RawSql("count(*) > 1")),
		]) {
			expect(call).toThrow(/strict mode/i);
		}
	});

	describe("havingExists on the model builder", () => {
		const orders = () =>
			new BaseRepository(Order, mockDb(), { dialect })
				.query()
				.groupBy("status")
				.having("status", "=", "a");
		const posts = () =>
			new BaseRepository(Post, mockDb(), { dialect })
				.query()
				.where("state", "open");
		const sub = `(SELECT * FROM ${id("having_posts")} WHERE ${id("state")} = ?)`;

		const existsMethods: Array<
			[
				(
					| "havingExists"
					| "andHavingExists"
					| "orHavingExists"
					| "havingNotExists"
					| "andHavingNotExists"
					| "orHavingNotExists"
				),
				string,
			]
		> = [
			["havingExists", "AND EXISTS"],
			["andHavingExists", "AND EXISTS"],
			["orHavingExists", "OR EXISTS"],
			["havingNotExists", "AND NOT EXISTS"],
			["andHavingNotExists", "AND NOT EXISTS"],
			["orHavingNotExists", "OR NOT EXISTS"],
		];

		it.each(existsMethods)("%s", (method, keyword) => {
			const m = orders();
			m[method](posts());
			expectHaving(m, `${keyword} ${sub}`, ["open"]);
		});

		it("takes a callback building the subquery on the same model", () => {
			const m = orders().havingExists((s) => s.where("total", ">", 5));
			expectHaving(
				m,
				`AND EXISTS (SELECT * FROM ${id("having_orders")} WHERE ${id("total")} > ?)`,
				[5],
			);
		});
	});
});
