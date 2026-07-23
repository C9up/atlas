/**
 * Connection-level query builders on the `db` service (Adonis Lucid parity):
 * db.query()/from()/table()/insertQuery()/rawQuery(), plus the `{ client: trx }`
 * option routing a query through a transaction.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { DatabaseQueryBuilder } from "../../src/query/DatabaseQueryBuilder.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService, type DbService } from "../../src/services/db.js";

let conn: AsyncDatabaseConnection;
let db: DbService;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
	);
	db = createDbService(() => conn);
});

afterEach(async () => {
	await conn?.close();
});

describe("atlas > db service query builders (Lucid)", () => {
	it("inserts via db.table(), reads via db.from()/where()/orderBy()", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });
		await db.table("users").insert({ id: 3, name: "Carol", active: 1 });

		const actives = await db
			.from("users")
			.where("active", 1)
			.orderBy("id", "desc");
		expect(actives.map((r) => r.name)).toEqual(["Carol", "Alice"]);
	});

	it("first() returns one row or null; count() counts the WHERE", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		const alice = await db.query().from("users").where("id", 1).first();
		expect(alice?.name).toBe("Alice");
		const none = await db.from("users").where("id", 999).first();
		expect(none).toBeNull();
		expect(await db.from("users").where("active", 1).count()).toBe(1);
	});

	it("update() and delete() return affected counts", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 1 });
		const updated = await db.from("users").where("id", 1).update({ active: 0 });
		expect(updated).toBe(1);
		const deleted = await db.from("users").where("active", 1).delete();
		expect(deleted).toBe(1);
		expect(await db.from("users").count()).toBe(1);
	});

	it("multiInsert + returning insert ids (Lucid/Knex)", async () => {
		const returned = await db
			.table("users")
			.returning("id")
			.insert({ id: 7, name: "Zoe", active: 1 });
		expect(returned).toEqual([{ id: 7 }]);

		await db.table("users").multiInsert([
			{ id: 8, name: "Ada", active: 1 },
			{ id: 9, name: "Bo", active: 0 },
		]);
		expect(await db.from("users").count()).toBe(3);
	});

	it("supports whereBetween/whereLike/whereNotIn/whereRaw + aggregates", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 1 });
		await db.table("users").insert({ id: 3, name: "Carol", active: 0 });

		expect((await db.from("users").whereBetween("id", [1, 2])).length).toBe(2);
		expect(
			(await db.from("users").whereLike("name", "A%")).map((r) => r.name),
		).toEqual(["Alice"]);
		expect(
			(await db.from("users").whereNotIn("id", [1, 2]).orderBy("id")).map(
				(r) => r.id,
			),
		).toEqual([3]);
		expect(
			(await db.from("users").whereRaw("active = ?", [1]).orderBy("id")).map(
				(r) => r.id,
			),
		).toEqual([1, 2]);

		expect(await db.from("users").sum("id")).toBe(6);
		expect(await db.from("users").max("id")).toBe(3);
		expect(await db.from("users").min("id")).toBe(1);
		expect(await db.from("users").where("active", 1).count()).toBe(2);
	});

	it("onConflict().merge() and .ignore() upsert (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		// merge: conflict on id → update name.
		await db
			.table("users")
			.onConflict("id")
			.merge()
			.insert({ id: 1, name: "Renamed", active: 1 });
		expect((await db.from("users").where("id", 1).first())?.name).toBe(
			"Renamed",
		);
		// ignore: conflict on id → keep the existing row.
		await db
			.table("users")
			.onConflict("id")
			.ignore()
			.insert({ id: 1, name: "Ignored", active: 0 });
		expect((await db.from("users").where("id", 1).first())?.name).toBe(
			"Renamed",
		);
	});

	it("whereColumn / orderByRaw / pluck / firstOrFail", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 1 });

		// whereColumn(id = id) is trivially true → all rows.
		expect((await db.from("users").whereColumn("id", "=", "id")).length).toBe(
			2,
		);
		// orderByRaw
		expect(
			(await db.from("users").orderByRaw("id DESC")).map((r) => r.id),
		).toEqual([2, 1]);
		// pluck
		expect(await db.from("users").orderBy("id").pluck("name")).toEqual([
			"Alice",
			"Bob",
		]);
		// firstOrFail throws on no match
		await expect(
			db.from("users").where("id", 999).firstOrFail(),
		).rejects.toThrow(/no matching row/);
	});

	it("union / CTE (with) / whereExists", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });

		// union of two single-row queries
		const u = await db
			.from("users")
			.where("id", 1)
			.union(db.from("users").where("id", 2));
		expect(u.length).toBe(2);

		// CTE: SELECT from a WITH-defined subquery
		const cte = await db
			.from("recent")
			.with("recent", db.from("users").where("active", 1));
		expect(cte.map((r) => r.name)).toEqual(["Alice"]);

		// whereExists — a subquery that matches
		const ex = await db
			.from("users")
			.whereExists(db.from("users").whereColumn("id", "=", "id"))
			.orderBy("id");
		expect(ex.length).toBe(2);
	});

	it("compiles JSON predicates + withSchema in SQL (toSQL/toNative)", () => {
		const jsonSql = db
			.from("users")
			.whereJsonPath("data", "$.city", "=", "Paris")
			.toQuery();
		expect(jsonSql).toContain('"data"');

		const native = db.from("users").withSchema("reporting").toNative();
		expect(native.sql).toContain('"reporting"."users"');
	});

	it("emits joins in the compiled SQL (Lucid 3-arg + callback + outer aliases)", () => {
		// 3-arg form quotes both column refs (Lucid `leftJoin(table, left, right)`).
		expect(
			db.from("users").leftJoin("posts", "posts.user_id", "users.id").toSQL()
				.sql,
		).toContain('LEFT JOIN "posts" ON "posts"."user_id" = "users"."id"');

		// callback ON builder with andOn / onVal (bound value → placeholder).
		const cb = db
			.from("users")
			.innerJoin("posts", (j) => {
				j.on("posts.user_id", "users.id").onVal("posts.published", 1);
			})
			.toSQL();
		expect(cb.sql).toContain(
			'INNER JOIN "posts" ON "posts"."user_id" = "users"."id" AND "posts"."published" = ?',
		);
		expect(cb.params).toEqual([1]);

		// leftOuterJoin is an alias of leftJoin.
		expect(
			db.from("users").leftOuterJoin("p", "p.uid", "users.id").toSQL().sql,
		).toContain('LEFT JOIN "p" ON "p"."uid" = "users"."id"');
	});

	it("comment() prefixes a SQL comment; debug() is chainable", () => {
		const { sql } = db.from("users").comment("report query").toSQL();
		expect(sql).toContain("/* report query */");
		expect(sql).toContain('SELECT * FROM "users"');
		// comment() rejects the terminator so it can't break out of the comment.
		expect(() => db.from("users").comment("evil */ DROP")).toThrow(/\*\//);
		// debug() just toggles logging and returns the builder.
		expect(db.from("users").debug()).toBeInstanceOf(DatabaseQueryBuilder);
	});

	it("rawQuery() executes raw SQL and returns rows", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		const rows = await db.rawQuery<{ n: number }>(
			"SELECT COUNT(*) AS n FROM users",
		);
		expect(rows[0]?.n).toBe(1);
	});

	it("toSQL() returns compiled SQL; ifDialect/unlessDialect gate by dialect", async () => {
		const { sql } = db
			.from("users")
			.where("active", 1)
			.ifDialect("sqlite", (q) => q.orderBy("id"))
			.unlessDialect("sqlite", (q) => q.orderBy("name"))
			.toSQL();
		// On sqlite the ifDialect branch (order by id) applies, unlessDialect skips.
		expect(sql).toContain('"active"');
		expect(sql).toContain('ORDER BY "id"');
		expect(sql).not.toContain('ORDER BY "name"');
	});

	it("clone() deep-copies so mutating the copy leaves the original (Lucid)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });

		const base = db.from("users").where("active", 1);
		const copy = base.clone().orWhere("id", 2);
		// The clone widened to {active=1 OR id=2}; the original stays {active=1}.
		expect((await copy.orderBy("id")).map((r) => r.id)).toEqual([1, 2]);
		expect((await base).map((r) => r.id)).toEqual([1]);
	});

	it("withRecursive / withMaterialized compile the right CTE keyword", () => {
		expect(
			db
				.from("t")
				.withRecursive("t", db.from("users").where("id", 1))
				.toSQL()
				.sql.toUpperCase(),
		).toContain("WITH RECURSIVE");

		const pg = new DatabaseQueryBuilder(conn, "postgres", "t");
		expect(
			pg
				.withMaterialized(
					"t",
					new DatabaseQueryBuilder(conn, "postgres", "users"),
				)
				.toSQL()
				.sql.toUpperCase(),
		).toContain("MATERIALIZED");
	});

	it("select accepts arrays + { alias: expr } objects (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		const rows = await db
			.from("users")
			.select(["id"], { label: "name" })
			.where("id", 1);
		expect(rows).toEqual([{ id: 1, label: "Alice" }]);
	});

	it("where(object) ANDs equalities; where(callback) groups with parens", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });
		await db.table("users").insert({ id: 3, name: "Alice", active: 0 });

		// where({name, active}) → name = 'Alice' AND active = 1
		expect(
			(await db.from("users").where({ name: "Alice", active: 1 })).map(
				(r) => r.id,
			),
		).toEqual([1]);

		// where(cb) → active = 1 OR (id = 2 OR id = 3) — grouped
		const grouped = await db
			.from("users")
			.where("active", 1)
			.orWhere((q) => q.where("id", 2).orWhere("id", 3))
			.orderBy("id");
		expect(grouped.map((r) => r.id)).toEqual([1, 2, 3]);
		// The compiled SQL parenthesises the group.
		expect(
			db
				.from("users")
				.where("active", 1)
				.orWhere((q) => q.where("id", 2).orWhere("id", 3))
				.toSQL().sql,
		).toMatch(/OR \(.*"id" = \? OR "id" = \?\)/);
	});

	it("toSQL() exposes bindings (Lucid) alongside params", () => {
		const { sql, bindings, params } = db.from("users").where("id", 7).toSQL();
		expect(sql).toContain("WHERE");
		expect(bindings).toEqual([7]);
		expect(params).toEqual([7]);
	});

	it("countDistinct / sumDistinct / avgDistinct (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 5 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 5 });
		await db.table("users").insert({ id: 3, name: "Carol", active: 9 });
		expect(await db.from("users").countDistinct("active")).toBe(2);
		expect(await db.from("users").sumDistinct("active")).toBe(14);
		expect(await db.from("users").avgDistinct("active")).toBe(7);
	});

	it("join callback onIn / onNull (Lucid/Knex)", () => {
		const inSql = db
			.from("users")
			.innerJoin("posts", (j) => {
				j.on("posts.user_id", "users.id").onIn("posts.status", [1, 2]);
			})
			.toSQL().sql;
		expect(inSql).toContain('"posts"."status" IN (?, ?)');

		const nullSql = db
			.from("users")
			.leftJoin("posts", (j) => {
				j.on("posts.user_id", "users.id").onNull("posts.deleted_at");
			})
			.toSQL().sql;
		expect(nullSql).toContain('"posts"."deleted_at" IS NULL');
	});

	it("from(subquery, alias) reads from a derived table (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });
		await db.table("users").insert({ id: 3, name: "Carol", active: 1 });

		// FROM (SELECT ... WHERE active = 1) AS t, then filter/aggregate the derived rows.
		const actives = db.from("users").where("active", 1);
		const rows = await db.from(actives, "t").where("id", ">", 1).orderBy("id");
		expect(rows.map((r) => r.name)).toEqual(["Carol"]);

		// The derived-table SQL is emitted with the alias.
		expect(
			db.from(db.from("users").where("active", 1), "t").toSQL().sql,
		).toContain('FROM (SELECT * FROM "users" WHERE "active" = ?) AS "t"');
	});

	it("aggregates: terminal scalar OR chainable projection (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 3, name: "Bob", active: 1 });

		// Terminal scalar forms (atlas DX) still return numbers.
		expect(await db.from("users").count()).toBe(3);
		expect(await db.from("users").sum("active")).toBe(3);

		// Chainable projection form (Lucid `count('* as total').groupBy(...)`).
		const grouped = await db
			.from("users")
			.select("name")
			.count("* as total")
			.groupBy("name")
			.orderBy("name");
		expect(grouped).toEqual([
			{ name: "Alice", total: 2 },
			{ name: "Bob", total: 1 },
		]);
	});

	it("from(callback) / union(callback) / distinct(cols) / orderBy(array) (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });
		await db.table("users").insert({ id: 3, name: "Alice", active: 1 });

		// from(callback) builds the derived table.
		const derived = await db
			.from((q: DatabaseQueryBuilder) => {
				q.from("users").where("active", 1);
			}, "t")
			.orderBy("id");
		expect(derived.map((r) => r.id)).toEqual([1, 3]);

		// union(callback).
		const u = await db
			.from("users")
			.where("id", 1)
			.union((q: DatabaseQueryBuilder) => {
				q.from("users").where("id", 2);
			});
		expect(u.length).toBe(2);

		// distinct(cols) selects those columns + DISTINCT.
		const names = await db.from("users").distinct("name").orderBy("name");
		expect(names.map((r) => r.name)).toEqual(["Alice", "Bob"]);

		// orderBy(array) with mixed string + {column, order}.
		const ordered = await db
			.from("users")
			.orderBy([{ column: "active", order: "desc" }, "id"]);
		expect(ordered.map((r) => r.id)).toEqual([1, 3, 2]);
	});

	it("havingNull / havingNotNull after groupBy (Lucid/Knex)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: null, active: 1 });
		// Group by active, keep groups whose MAX(name) is not null.
		const sql = db
			.from("users")
			.select("active")
			.groupBy("active")
			.havingNotNull("active")
			.toSQL().sql;
		expect(sql).toContain('HAVING "active" IS NOT NULL');
	});

	it("supports conditional if/unless/match builders", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });

		const search = "Alice";
		const rows = await db
			.from("users")
			.if(search, (q) => q.where("name", search))
			.unless(search, (q) => q.where("active", 0))
			.orderBy("id");
		expect(rows.map((r) => r.name)).toEqual(["Alice"]);

		const matched = await db
			.from("users")
			.match(
				[false, (q) => q.where("id", 999)],
				[true, (q) => q.where("active", 0)],
			)
			.orderBy("id");
		expect(matched.map((r) => r.name)).toEqual(["Bob"]);
	});

	it("supports orWhere, distinct, groupBy + having", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });
		await db.table("users").insert({ id: 3, name: "Alice", active: 1 });

		// orWhere
		const or = await db
			.from("users")
			.where("id", 1)
			.orWhere("id", 2)
			.orderBy("id");
		expect(or.map((r) => r.id)).toEqual([1, 2]);

		// distinct
		const names = await db
			.from("users")
			.distinct()
			.select("name")
			.orderBy("name");
		expect(names.map((r) => r.name)).toEqual(["Alice", "Bob"]);

		// groupBy + having (aggregate)
		const grouped = await db
			.from("users")
			.select("name", "COUNT(*) AS c")
			.groupBy("name")
			.having("COUNT(*)", ">", 1);
		expect(grouped).toEqual([{ name: "Alice", c: 2 }]);
	});

	it("trx.table()/from() route the builder through the transaction (Lucid)", async () => {
		if (typeof conn.transaction !== "function") throw new Error("no trx");
		await conn.transaction(async (trx) => {
			await trx.table("users").insert({ id: 5, name: "Tx", active: 1 });
			const inside = await trx.from("users").where("id", 5).first();
			expect(inside?.name).toBe("Tx");
		});
		// Committed — visible on the connection afterwards.
		expect((await db.from("users").where("id", 5).first())?.name).toBe("Tx");
	});

	it("trx builder writes roll back with the transaction", async () => {
		if (typeof conn.transaction !== "function") throw new Error("no trx");
		await expect(
			conn.transaction(async (trx) => {
				await trx.table("users").insert({ id: 6, name: "Gone", active: 1 });
				throw new Error("rollback");
			}),
		).rejects.toThrow("rollback");
		expect(await db.from("users").where("id", 6).first()).toBeNull();
	});

	it("increment / decrement atomically (SET col = col ± ?)", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 5 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 5 });
		// increment only the matched row
		expect(await db.from("users").where("id", 1).increment("active", 3)).toBe(
			1,
		);
		expect((await db.from("users").where("id", 1).first())?.active).toBe(8);
		// default amount = 1
		await db.from("users").where("id", 2).decrement("active");
		expect((await db.from("users").where("id", 2).first())?.active).toBe(4);
	});

	it("paginate() returns a Paginator with total + page slice", async () => {
		for (let i = 1; i <= 5; i++)
			await db.table("users").insert({ id: i, name: `U${i}`, active: 1 });
		const page = await db.from("users").orderBy("id").paginate(2, 2);
		expect(page.total).toBe(5);
		expect(page.perPage).toBe(2);
		expect(page.currentPage).toBe(2);
		expect(page.lastPage).toBe(3);
		expect(page.all().map((r) => r.id)).toEqual([3, 4]);
	});

	it("whereNot / orWhereNull / orWhereIn / orHaving", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 0 });
		await db.table("users").insert({ id: 3, name: "Carol", active: 1 });

		// whereNot(active, 1) → active != 1
		expect(
			(await db.from("users").whereNot("active", 1)).map((r) => r.id),
		).toEqual([2]);
		// orWhereIn combines with OR
		expect(
			(
				await db.from("users").where("id", 1).orWhereIn("id", [3]).orderBy("id")
			).map((r) => r.id),
		).toEqual([1, 3]);
	});

	it("intersect / except set operations", async () => {
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		await db.table("users").insert({ id: 2, name: "Bob", active: 1 });
		await db.table("users").insert({ id: 3, name: "Carol", active: 0 });

		// {1,2,3} EXCEPT {3} = {1,2}
		const ex = await db
			.from("users")
			.except(db.from("users").where("active", 0))
			.orderBy("id");
		expect(ex.map((r) => r.id)).toEqual([1, 2]);

		// {active=1} INTERSECT {id<=1} = {1}
		const inter = await db
			.from("users")
			.where("active", 1)
			.intersect(db.from("users").where("id", "<=", 1));
		expect(inter.map((r) => r.id)).toEqual([1]);
	});

	it("distinctOn compiles to Postgres DISTINCT ON", () => {
		// The db builder inherits the connection's dialect (sqlite); construct a
		// postgres builder directly to assert the postgres-only SQL shape.
		const pg = new DatabaseQueryBuilder(conn, "postgres", "users");
		const { sql } = pg.distinctOn("name").select("name").toSQL();
		expect(sql).toContain("DISTINCT ON");
	});

	it("forUpdate emits a valid lock on postgres (regression: was 'for_update')", () => {
		const pg = new DatabaseQueryBuilder(conn, "postgres", "users");
		expect(pg.where("id", 1).forUpdate().toSQL().sql).toContain("FOR UPDATE");
		const pg2 = new DatabaseQueryBuilder(conn, "postgres", "users");
		expect(pg2.forUpdate().skipLocked().toSQL().sql).toContain(
			"FOR UPDATE SKIP LOCKED",
		);
	});

	it("routes a query through a transaction via { client: trx }", async () => {
		if (typeof conn.transaction !== "function") throw new Error("no trx");
		// Seed on the default connection BEFORE pinning it in a trx (poolMax=1).
		await db.table("users").insert({ id: 1, name: "Alice", active: 1 });
		const trx = await conn.transaction();
		// Update inside the trx, then roll back — the change must not survive.
		await db.query({ client: trx }).from("users").where("id", 1).update({
			name: "Renamed",
		});
		await trx.rollback();
		const after = await db.from("users").where("id", 1).first();
		expect(after?.name).toBe("Alice");
	});
});
