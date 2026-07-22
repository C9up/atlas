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
