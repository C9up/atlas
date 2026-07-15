import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Order extends BaseModel {
	static override table = "orders";
	@PrimaryKey() declare id: string;
	@Column() declare userId: string;
	@Column() declare status: string;
	@Column() declare region: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, tier TEXT)");
	await conn.execute(
		"CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT, status TEXT, region TEXT)",
	);
	await conn.execute("INSERT INTO users VALUES ('u1','gold'),('u2','silver')");
	await conn.execute(
		"INSERT INTO orders VALUES ('o1','u1','paid','eu'),('o2','u1','pending','eu'),('o3','u2','paid','us')",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > join onVal binds a value end-to-end (NAPI param channel)", () => {
	it("onVal filters in the JOIN and its param is ordered before the WHERE param", async () => {
		// JOIN param 'paid' ($1) comes before WHERE param 'eu' ($2) — the whole
		// point of threading join params through the compiler before the WHERE.
		const rows = await Order.query()
			.innerJoin("users", (j) =>
				j.on("users.id", "orders.user_id").andOnVal("orders.status", "paid"),
			)
			.where("region", "eu")
			.exec();
		// Only o1 matches: paid (bound in the JOIN) AND region eu (bound in the
		// WHERE). o2 is pending, o3 is us. Exactly one row proves BOTH params bound
		// correctly and in the right order.
		expect(rows.length).toBe(1);
		// `id` is NO LONGER clobbered by users.id: with a join + default select the
		// projection is scoped to orders' own columns, so the model hydrates cleanly.
		expect(rows[0].id).toBe("o1");
		expect(rows[0].status).toBe("paid");
		expect(rows[0].region).toBe("eu");
		expect(rows[0].userId).toBe("u1");
	});
});
