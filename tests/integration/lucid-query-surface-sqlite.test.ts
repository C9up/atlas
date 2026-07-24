/**
 * Lucid parity surface added to the connection-level query builder:
 * `select(db.raw())` / `select(subquery.as())`, `from((s) => s.as())`, the
 * 4-argument `join(table, left, operator, right)`, the full JoinClause `on*`
 * family (onIn/onNotIn/onNull/onNotNull/onBetween/onNotBetween/onExists/
 * onNotExists), CTE callbacks and `withRecursive(name, cb, columns)`.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	DatabaseQueryBuilder,
	makeTransactionQueryBuilders,
} from "../../src/query/DatabaseQueryBuilder.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { RawSql } from "../../src/query/QueryBuilder.js";
import { RawQueryBuilder } from "../../src/query/RawQueryBuilder.js";
import { createDbService, type DbService } from "../../src/services/db.js";

let conn: AsyncDatabaseConnection;
let db: DbService;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, team_id INTEGER)",
	);
	await conn.execute(
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)",
	);
	await conn.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)");
	db = createDbService(() => conn);
});

afterEach(async () => {
	await conn?.close();
});

describe("atlas > DB builder — Lucid select parity", () => {
	it("select(db.raw()) inlines a raw fragment carrying its own bindings", async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		const q = db
			.from("users")
			.select("name")
			.select(db.raw("? AS marker", ["x"]));
		expect(q.toSQL().sql).toContain("? AS marker");
		const rows = await q;
		expect(rows[0]).toMatchObject({ name: "Ada", marker: "x" });
	});

	it("select(subquery.as('alias')) projects a named subquery column", async () => {
		await db.table("teams").insert({ id: 1, name: "Blue" });
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("users").insert({ id: 2, name: "Bo", team_id: 1 });
		const q = db
			.from("teams")
			.select("name")
			.select(db.from("users").select("COUNT(*)").as("user_count"));
		expect(q.toSQL().sql).toContain(') AS "user_count"');
		const rows = await q;
		expect(rows[0]).toMatchObject({ name: "Blue", user_count: 2 });
	});

	it("select({ alias: 'column' }) object map + multiple string args", () => {
		const sql = db
			.from("users")
			.select("id", "name", { label: "name" })
			.toSQL().sql;
		expect(sql).toContain('"name" AS "label"');
	});

	it("db.raw(...).wrap(before, after) wraps the fragment (Lucid)", () => {
		const wrapped = db.raw("select 1", []).wrap("(", ") as one");
		expect(wrapped.sql).toBe("(select 1) as one");
		const sql = db.from("users").select(wrapped).toSQL().sql;
		expect(sql).toContain("(select 1) as one");
	});
});

describe("atlas > DB builder — Lucid from/derived-table parity", () => {
	it("from((s) => s.as('totals')) names the derived table via .as()", async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("users").insert({ id: 2, name: "Bo", team_id: 2 });
		const q = db
			.from((s) =>
				s
					.from("users")
					.select("team_id")
					.select("COUNT(*) AS c")
					.groupBy("team_id")
					.as("totals"),
			)
			.select("*");
		expect(q.toSQL().sql).toContain('AS "totals"');
		const rows = await q;
		expect(rows).toHaveLength(2);
	});
});

describe("atlas > DB builder — Lucid join parity", () => {
	beforeEach(async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("posts").insert({ id: 1, user_id: 1, title: "Hi" });
	});

	it("join(table, left, operator, right) — the 4-argument form", async () => {
		const q = db
			.from("posts")
			.select("posts.title")
			.join("users", "posts.user_id", "=", "users.id");
		expect(q.toSQL().sql).toContain(
			'INNER JOIN "users" ON "posts"."user_id" = "users"."id"',
		);
		const rows = await q;
		expect(rows[0]).toMatchObject({ title: "Hi" });
	});

	it("callback ON family: onIn/onNotIn/onNull/onNotNull/onBetween/onNotBetween", () => {
		const sql = db
			.from("posts")
			.leftJoin("users", (j) =>
				j
					.on("posts.user_id", "users.id")
					.onIn("users.id", [1, 2])
					.onNotIn("users.team_id", [9])
					.onNull("users.name")
					.onNotNull("posts.title")
					.onBetween("users.id", [1, 10])
					.onNotBetween("users.team_id", [100, 200]),
			)
			.toSQL();
		expect(sql.sql).toContain('ON "posts"."user_id" = "users"."id"');
		expect(sql.sql).toContain('AND "users"."id" IN (?, ?)');
		expect(sql.sql).toContain('AND "users"."team_id" NOT IN (?)');
		expect(sql.sql).toContain('AND "users"."name" IS NULL');
		expect(sql.sql).toContain('AND "posts"."title" IS NOT NULL');
		expect(sql.sql).toContain('AND "users"."id" BETWEEN ? AND ?');
		expect(sql.sql).toContain('AND "users"."team_id" NOT BETWEEN ? AND ?');
		// Bound values flow in placeholder order: IN(1,2), NOT IN(9), BETWEEN(1,10), NOT BETWEEN(100,200).
		expect(sql.params).toEqual([1, 2, 9, 1, 10, 100, 200]);
	});

	it("callback ON with operator + onExists/onNotExists", () => {
		const sql = db
			.from("posts")
			.innerJoin("users", (j) =>
				j
					.on("posts.user_id", ">=", "users.id")
					.onExists((s) => s.from("teams").where("id", 1)),
			)
			.toSQL();
		expect(sql.sql).toContain('ON "posts"."user_id" >= "users"."id"');
		expect(sql.sql).toContain("AND EXISTS (");
	});

	it("rejects an unknown join operator", () => {
		expect(() =>
			db.from("posts").join("users", "posts.user_id", "; DROP", "users.id"),
		).toThrow(/join operator/);
	});
});

describe("atlas > DB builder — Lucid CTE parity", () => {
	it("with(name, callback) accepts a callback body", async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("users").insert({ id: 2, name: "Bo", team_id: 2 });
		const q = db
			.query()
			.with("active", (c) => c.from("users").where("team_id", 1))
			.from("active")
			.select("name");
		expect(q.toSQL().sql).toContain('WITH "active" AS (');
		const rows = await q;
		expect(rows.map((r) => r.name)).toEqual(["Ada"]);
	});

	it("withRecursive(name, callback, columns) emits the output-column list", () => {
		const sql = db
			.query()
			.withRecursive("nums", (c) => c.from("users").select("id"), ["n"])
			.from("nums")
			.toSQL().sql;
		expect(sql).toContain('WITH RECURSIVE "nums"("n") AS (');
	});
});

describe("atlas > DB builder — Lucid where variants", () => {
	beforeEach(async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("users").insert({ id: 2, name: "Bo", team_id: 2 });
		await db.table("posts").insert({ id: 1, user_id: 1, title: "Hi" });
	});

	it("whereExists(callback) — correlated subquery", async () => {
		const rows = await db
			.from("users")
			.whereExists((q) =>
				q.from("posts").whereColumn("posts.user_id", "=", "users.id"),
			)
			.orderBy("id");
		expect(rows.map((r) => r.name)).toEqual(["Ada"]);
	});

	it("whereIn(column, subquery) + whereNotIn(column, subquery)", async () => {
		const withPosts = await db
			.from("users")
			.whereIn("id", db.from("posts").select("user_id"))
			.orderBy("id");
		expect(withPosts.map((r) => r.name)).toEqual(["Ada"]);
		const without = await db
			.from("users")
			.whereNotIn("id", db.from("posts").select("user_id"))
			.orderBy("id");
		expect(without.map((r) => r.name)).toEqual(["Bo"]);
	});

	it("whereNotColumn / orWhereColumn / whereNotRaw compile as expected", () => {
		expect(
			db.from("users").whereNotColumn("id", "=", "team_id").toSQL().sql,
		).toContain('NOT ("id" = "team_id")');
		expect(
			db
				.from("users")
				.where("id", 1)
				.orWhereColumn("id", ">", "team_id")
				.toSQL().sql,
		).toContain('OR ("id" > "team_id")');
		expect(db.from("users").whereNotRaw("id = ?", [1]).toSQL().sql).toContain(
			"NOT (id = ?)",
		);
	});

	it("returning(['id','name']) returns the inserted columns (sqlite 3.35+)", async () => {
		const rows = await db
			.table("teams")
			.returning(["id", "name"])
			.insert({ id: 7, name: "Red" });
		expect(rows[0]).toMatchObject({ id: 7, name: "Red" });
	});

	it("onConflict(array).merge() upserts", async () => {
		await db.table("teams").insert({ id: 1, name: "Blue" });
		await db
			.table("teams")
			.onConflict(["id"])
			.merge(["name"])
			.insert({ id: 1, name: "Navy" });
		const [t] = await db.from("teams").where("id", 1);
		expect(t.name).toBe("Navy");
	});
});

describe("atlas > db.rawQuery — Lucid raw query builder", () => {
	beforeEach(async () => {
		await db.table("users").insert({ id: 5, name: "Ada", team_id: 1 });
	});

	it("positional bindings: toSQL/toQuery + awaitable execution", async () => {
		const q = db.rawQuery("select name from users where id = ?", [5]);
		expect(q.toSQL()).toEqual({
			sql: "select name from users where id = ?",
			bindings: [5],
		});
		expect(q.toQuery()).toBe("select name from users where id = 5");
		const rows = await q;
		expect(rows[0]).toMatchObject({ name: "Ada" });
	});

	it("named bindings (:name) resolve to positional + values", async () => {
		const named = db.rawQuery("select name from users where id = :id", {
			id: 5,
		});
		expect(named.toSQL().sql).toBe("select name from users where id = ?");
		expect(named.toSQL().bindings).toEqual([5]);
		expect(await named).toHaveLength(1);
	});

	it("identifier bindings (?? / :name:) inline a quoted identifier", () => {
		expect(
			db.rawQuery("select ?? from users", ["users.name"]).toSQL().sql,
		).toBe('select "users"."name" from users');
		expect(
			db.rawQuery("select :col: from users", { col: "users.id" }).toSQL().sql,
		).toBe('select "users"."id" from users');
	});

	it("debug()/timeout()/reporterData() chain and return the builder", () => {
		const q = db
			.rawQuery("select 1")
			.debug()
			.timeout(1000)
			.reporterData({ tag: "x" });
		expect(q).toBeInstanceOf(RawQueryBuilder);
	});
});

describe("atlas > trx client — Lucid query surface", () => {
	it("exposes query() builder, rawQuery(), raw(), and low-level query(sql)", async () => {
		const seen: Array<[string, unknown]> = [];
		const exec = {
			query: async (sql: string, p?: unknown[]) => {
				seen.push([sql, p]);
				return [];
			},
			execute: async () => ({ rowsAffected: 0 }),
		};
		const trx = makeTransactionQueryBuilders(exec, "sqlite");
		expect(trx.query()).toBeInstanceOf(DatabaseQueryBuilder);
		expect(trx.rawQuery("select 1")).toBeInstanceOf(RawQueryBuilder);
		expect(trx.raw("now()")).toBeInstanceOf(RawSql);
		// The low-level query(sql, params) executor still works (no recursion).
		await trx.query("select ?", [1]);
		expect(seen).toEqual([["select ?", [1]]]);
	});
});

describe("atlas > DB builder — Lucid JSON where variants", () => {
	const fakeExec = {
		query: async () => [],
		execute: async () => ({}),
	};

	it("superset/subset + and/or/not variants compile (postgres)", () => {
		const sql = new DatabaseQueryBuilder(fakeExec, "postgres", "users")
			.whereJsonSuperset("data", { a: 1 })
			.orWhereNotJsonSubset("data", { b: 2 })
			.toSQL().sql;
		expect(sql).toContain('"data"::jsonb @>');
		expect(sql).toContain("OR NOT (");
		expect(sql).toContain('"data"::jsonb <@');
	});
});

describe("atlas > DB builder — Lucid whereIn tuple / insert-id / merge object", () => {
	it("whereIn(['a','b'], [[..],[..]]) — multi-column tuple IN", async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("users").insert({ id: 2, name: "Bo", team_id: 2 });
		const compiled = db
			.from("users")
			.whereIn(
				["id", "team_id"],
				[
					[1, 1],
					[2, 9],
				],
			)
			.toSQL();
		expect(compiled.sql).toContain('("id", "team_id") IN ((?, ?), (?, ?))');
		expect(compiled.params).toEqual([1, 1, 2, 9]);
		const rows = await db
			.from("users")
			.whereIn(
				["id", "team_id"],
				[
					[1, 1],
					[2, 9],
				],
			)
			.orderBy("id");
		// Only (1,1) matches — Bo is team 2, not 9.
		expect(rows.map((r) => r.name)).toEqual(["Ada"]);
	});

	it("insert without returning yields [insertId] on sqlite (Lucid)", async () => {
		const rows = await db.table("users").insert({ name: "Zoe", team_id: 1 });
		expect(rows).toHaveLength(1);
		expect(typeof rows[0]).toBe("number");
		const [found] = await db.from("users").where("id", rows[0]);
		expect(found.name).toBe("Zoe");
	});

	it("merge({ col: value }) sets custom update values on conflict", async () => {
		await db.table("teams").insert({ id: 1, name: "Blue" });
		await db
			.table("teams")
			.onConflict(["id"])
			.merge({ name: "Navy" })
			.insert({ id: 1, name: "ignored" });
		const [t] = await db.from("teams").where("id", 1);
		expect(t.name).toBe("Navy");
	});
});

describe("atlas > CTE on DML — Lucid with().insert()/update()", () => {
	it("carries a CTE onto INSERT with correct param ordering", async () => {
		// The CTE param (99) must be bound BEFORE the insert params (5, 'Red').
		await db
			.query()
			.with("recent", (q) => q.from("teams").where("id", 99))
			.table("teams")
			.insert({ id: 5, name: "Red" });
		const [t] = await db.from("teams").where("id", 5);
		expect(t.name).toBe("Red");
	});

	it("carries a CTE onto UPDATE with correct param ordering", async () => {
		await db.table("teams").insert({ id: 1, name: "Blue" });
		await db
			.query()
			.with("recent", (q) => q.from("teams").where("id", 42))
			.from("teams")
			.where("id", 1)
			.update({ name: "Navy" });
		const [t] = await db.from("teams").where("id", 1);
		expect(t.name).toBe("Navy");
	});
});

describe("atlas > raw bindings + merge raw — audit fixes", () => {
	it("named bindings leave Postgres `::casts` intact (:payload::jsonb)", () => {
		const q = db.rawQuery("select :payload::jsonb as data", {
			payload: '{"a":1}',
		});
		// `:payload` binds, `::jsonb` is preserved (not read as an identifier).
		expect(q.toSQL()).toEqual({
			sql: "select ?::jsonb as data",
			bindings: ['{"a":1}'],
		});
	});

	it("merge({ col: db.raw('expr + ?', [n]) }) threads the raw bindings", async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db
			.table("users")
			.onConflict(["id"])
			.merge({ team_id: db.raw("team_id + ?", [5]) })
			.insert({ id: 1, name: "ignored", team_id: 99 });
		const [u] = await db.from("users").where("id", 1);
		// Existing team_id (1) + 5 — the raw binding was applied.
		expect(u.team_id).toBe(6);
	});
});

describe("atlas > DB builder — Lucid timeout(ms)", () => {
	it("rejects when the query outlasts the deadline", async () => {
		const slowExec = {
			query: () =>
				new Promise<never[]>((resolve) => setTimeout(() => resolve([]), 100)),
			execute: async () => ({}),
		};
		const q = new DatabaseQueryBuilder(slowExec, "sqlite", "users").timeout(20);
		await expect(q.exec()).rejects.toThrow(/timed out/);
	});

	it("no timeout set → resolves normally", async () => {
		const fastExec = {
			query: async () => [],
			execute: async () => ({}),
		};
		const rows = await new DatabaseQueryBuilder(
			fastExec,
			"sqlite",
			"users",
		).exec();
		expect(rows).toEqual([]);
	});
});
