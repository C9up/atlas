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
import {
	createDbService,
	type DbService,
	registerConnection,
	unregisterConnection,
} from "../../src/services/db.js";

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

describe("atlas > DML surface — Lucid update/del/whereNot/multiInsert", () => {
	beforeEach(async () => {
		await db.table("teams").insert({ id: 1, name: "Blue" });
	});

	it("update('column', value) — 2-argument form", async () => {
		await db.from("teams").where("id", 1).update("name", "Navy");
		const [t] = await db.from("teams").where("id", 1);
		expect(t.name).toBe("Navy");
	});

	it("update({ col: db.raw('expr + ?', [n]) }) — raw SET value", async () => {
		await db.table("teams").insert({ id: 2, name: "Red" });
		await db
			.from("teams")
			.where("id", 2)
			.update({ id: db.raw("id + ?", [10]) });
		const rows = await db.from("teams").where("name", "Red");
		expect(rows[0].id).toBe(12);
	});

	it("del() is an alias of delete()", async () => {
		const n = await db.from("teams").where("id", 1).del();
		expect(n).toBe(1);
		expect(await db.from("teams").count()).toBe(0);
	});

	it("multiInsert fills missing keys with NULL (Lucid)", async () => {
		await db.table("teams").multiInsert([{ id: 5, name: "A" }, { id: 6 }]);
		const [six] = await db.from("teams").where("id", 6);
		expect(six.name).toBeNull();
	});

	it("whereNot object + callback forms", async () => {
		await db.table("teams").insert({ id: 2, name: "Red" });
		// object → NOT equality per key.
		const notBlue = await db
			.from("teams")
			.whereNot({ name: "Blue" })
			.orderBy("id");
		expect(notBlue.map((r) => r.name)).toEqual(["Red"]);
		// callback → NOT (group).
		const sql = db
			.from("teams")
			.whereNot((q) => q.where("id", 1).orWhere("id", 2))
			.toSQL().sql;
		expect(sql).toContain("NOT (");
	});

	it("wrapExisting() groups prior WHERE clauses (Lucid)", () => {
		const sql = db
			.from("teams")
			.where("id", 1)
			.orWhere("id", 2)
			.wrapExisting()
			.where("name", "Blue")
			.toSQL().sql;
		expect(sql).toContain('("id" = ? OR "id" = ?) AND "name" = ?');
	});

	it("withSchema() qualifies the table (SELECT emits it; DML uses the same helper)", async () => {
		// `#qualifiedTable()` — now used by DML too — emits the "schema"."table" form.
		const sql = db.from("teams").withSchema("main").where("id", 1).toSQL().sql;
		expect(sql).toContain('"main"."teams"');
		// A DML through the same helper executes against the qualified table
		// (sqlite's `main` schema is the default DB, so `main.teams` resolves).
		await db
			.from("teams")
			.withSchema("main")
			.where("id", 1)
			.update("name", "Q");
		const [t] = await db.from("teams").where("id", 1);
		expect(t.name).toBe("Q");
	});
});

describe("atlas > lazy DML — Lucid chain order + inspection", () => {
	beforeEach(async () => {
		await db.table("teams").insert({ id: 1, name: "Blue" });
	});

	it("insert(data).onConflict(...).merge() — the Lucid order", async () => {
		await db
			.table("teams")
			.insert({ id: 1, name: "Navy" })
			.onConflict(["id"])
			.merge(["name"]);
		const [t] = await db.from("teams").where("id", 1);
		expect(t.name).toBe("Navy");
	});

	it("insert(data).toSQL() inspects WITHOUT executing", async () => {
		const before = await db.from("teams").count();
		const q = db.table("teams").insert({ id: 9, name: "X" });
		expect(q.toSQL().sql).toContain('INSERT INTO "teams"');
		// Not run yet.
		expect(await db.from("teams").count()).toBe(before);
		await q; // now it executes
		expect(await db.from("teams").count()).toBe(before + 1);
	});

	it("update(data).returning(...) — returning AFTER update (Lucid order)", async () => {
		const rows = await db
			.from("teams")
			.where("id", 1)
			.update({ name: "Cyan" })
			.returning(["id", "name"]);
		expect(rows).toEqual([{ id: 1, name: "Cyan" }]);
	});

	it("update(data).toSQL() + delete().timeout() chain lazily", async () => {
		expect(
			db.from("teams").where("id", 1).update({ name: "Q" }).toSQL().sql,
		).toContain('UPDATE "teams" SET');
		const n = await db.from("teams").where("id", 1).delete().timeout(5000);
		expect(n).toBe(1);
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

describe("atlas > DB builder — Lucid inspection parity (toQuery / toSQL().toNative / DML debug)", () => {
	it("toQuery() interpolates bindings as literals (Lucid semantics)", async () => {
		const q = db.from("users").where("id", 5).where("name", "Ada");
		const sql = q.toQuery();
		expect(sql).toContain("= 5");
		expect(sql).toContain("'Ada'");
		expect(sql).not.toContain("?");
	});

	it("insert(...).toSQL().toNative() yields the native { sql, bindings }", () => {
		const compiled = db.table("users").insert({ id: 1, name: "Ada" }).toSQL();
		const native = compiled.toNative();
		expect(native.sql).toBe(compiled.sql);
		expect(native.bindings).toEqual(compiled.bindings);
		expect(native.sql).toMatch(/insert into/i);
	});

	it("delete(...).toQuery() interpolates its WHERE bindings", () => {
		const sql = db.from("users").where("id", 7).delete().toQuery();
		expect(sql).toContain("= 7");
		expect(sql).not.toContain("?");
	});

	it("insert(...).debug().reporterData(...) are chainable and awaitable", async () => {
		const rows = await db
			.table("users")
			.insert({ id: 2, name: "Bo" })
			.debug(false)
			.reporterData({ source: "test" });
		expect(Array.isArray(rows)).toBe(true);
		const found = await db.from("users").where("id", 2).first();
		expect(found).toMatchObject({ name: "Bo" });
	});

	it("delete().timeout(ms, { cancel: true }) is accepted (Lucid signature)", async () => {
		await db.table("users").insert({ id: 3, name: "Cy" });
		const n = await db
			.from("users")
			.where("id", 3)
			.delete()
			.timeout(1000, { cancel: true });
		expect(n).toBe(1);
	});
});

describe("atlas > db.connection(name, { mode }) — Lucid read/write guard", () => {
	beforeEach(() => {
		registerConnection("ro-test", conn);
	});
	afterEach(() => {
		unregisterConnection("ro-test", conn);
	});

	it("mode: 'read' allows reads but rejects writes on the scoped builders", async () => {
		await db.table("users").insert({ id: 1, name: "Ada" });
		const ro = db.connection("ro-test", { mode: "read" });

		const rows = await ro.from("users").where("id", 1);
		expect(rows[0]).toMatchObject({ name: "Ada" });

		expect(() => ro.from("users").where("id", 1).update({ name: "X" })).toThrow(
			/read/i,
		);
		expect(() => ro.table("users").insert({ id: 9, name: "Z" })).toThrow(
			/read/i,
		);
	});

	it("mode: 'write' (and the default) leave writes unrestricted", async () => {
		const rw = db.connection("ro-test", { mode: "write" });
		const rows = await rw.table("users").insert({ id: 2, name: "Bo" });
		expect(Array.isArray(rows)).toBe(true);
		expect(() =>
			db.connection("ro-test").table("users").insert({ id: 3 }),
		).not.toThrow();
	});
});

describe("atlas > ifDialect/unlessDialect accept Lucid dialect names", () => {
	it("ifDialect('sqlite3') matches atlas 'sqlite' (Lucid names its sqlite client sqlite3)", () => {
		let ran = false;
		db.from("users").ifDialect("sqlite3", () => {
			ran = true;
		});
		expect(ran).toBe(true);
	});

	it("ifDialect(['better-sqlite3','pg']) matches on the sqlite alias", () => {
		let ran = false;
		db.from("users").ifDialect(["better-sqlite3", "pg"], () => {
			ran = true;
		});
		expect(ran).toBe(true);
	});

	it("unlessDialect('sqlite3') does NOT run on sqlite", () => {
		let ran = false;
		db.from("users").unlessDialect("sqlite3", () => {
			ran = true;
		});
		expect(ran).toBe(false);
	});

	it("ifDialect('mysql2'/'postgres') does not run on sqlite", () => {
		let ran = false;
		db.from("users").ifDialect(["mysql2", "postgres"], () => {
			ran = true;
		});
		expect(ran).toBe(false);
	});
});

describe("atlas > db.query({ mode: 'read' }) write-guard", () => {
	it("rejects writes on a per-call read-mode builder", async () => {
		await db.table("users").insert({ id: 1, name: "Ada" });
		const rows = await db.query({ mode: "read" }).from("users").where("id", 1);
		expect(rows[0]).toMatchObject({ name: "Ada" });
		expect(() =>
			db.query({ mode: "read" }).from("users").update({ name: "X" }),
		).toThrow(/read/i);
	});
});

describe("atlas > DB builder — andWhere*/orWhereNot aliases (Lucid parity)", () => {
	beforeEach(async () => {
		await db.table("users").insert({ id: 1, name: "Ada", team_id: 1 });
		await db.table("users").insert({ id: 2, name: "Bo", team_id: 2 });
		await db.table("users").insert({ id: 3, name: "Cy", team_id: 2 });
	});

	it("andWhereNot / orWhereNot", async () => {
		const a = await db
			.from("users")
			.where("team_id", 2)
			.andWhereNot("name", "Bo");
		expect(a.map((r) => r.name)).toEqual(["Cy"]);
		const o = await db
			.from("users")
			.where("id", 1)
			.orWhereNot("team_id", 2)
			.orderBy("id");
		expect(o.map((r) => r.name)).toEqual(["Ada"]);
	});

	it("andWhereIn (values + tuple) / andWhereNotIn / andWhereNull / andWhereBetween", async () => {
		const vals = await db.from("users").andWhereIn("id", [1, 3]).orderBy("id");
		expect(vals.map((r) => r.name)).toEqual(["Ada", "Cy"]);

		const tuple = await db
			.from("users")
			.andWhereIn(["id", "name"], [[1, "Ada"]])
			.orderBy("id");
		expect(tuple.map((r) => r.name)).toEqual(["Ada"]);

		const notIn = await db
			.from("users")
			.andWhereNotIn("id", [2, 3])
			.orderBy("id");
		expect(notIn.map((r) => r.name)).toEqual(["Ada"]);

		const between = await db
			.from("users")
			.andWhereBetween("id", [2, 3])
			.orderBy("id");
		expect(between.map((r) => r.name)).toEqual(["Bo", "Cy"]);
	});
});

describe("atlas > db.connection() without a name → default service (Lucid)", () => {
	it("returns a working default-connection service", async () => {
		await db.table("users").insert({ id: 1, name: "Ada" });
		const rows = await db.connection().from("users").where("id", 1);
		expect(rows[0]).toMatchObject({ name: "Ada" });
	});
});
