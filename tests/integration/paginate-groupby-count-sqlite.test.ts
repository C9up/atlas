/**
 * paginate() must count the number of GROUPS when the query groups, not the size
 * of the first group. A flat `SELECT COUNT(*) … GROUP BY x` returns one row per
 * group; the old code read `rows[0].count` (the first group's size). Lucid counts
 * via a subquery — we wrap the grouped query and count its rows.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class PgOrder extends BaseModel {
	static override table = "pg_orders";
	@PrimaryKey() declare id: string;
	@Column() declare userId: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE pg_orders (id TEXT PRIMARY KEY, user_id TEXT)",
	);
	// 3 distinct users → 3 groups. user u1 has 3 orders (the largest group), so a
	// broken flat COUNT would report 3 instead of 3 groups... make the first
	// group's size DIFFER from the group count to be discriminating: u1 x3, u2 x1,
	// u3 x1 → 3 groups, first group size 3. Add a 4th group so counts diverge.
	await conn.execute(
		"INSERT INTO pg_orders (id, user_id) VALUES " +
			"('o1','u1'),('o2','u1'),('o3','u1'),('o4','u2'),('o5','u3'),('o6','u4')",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > paginate() total with groupBy counts groups (Lucid subquery count)", () => {
	it("reports the number of groups, not the first group's size", async () => {
		const page = await PgOrder.query()
			.select("user_id")
			.groupBy("user_id")
			.paginate(1, 10);
		// 4 distinct users → 4 groups. The first group (u1) has 3 rows; the broken
		// flat count would have returned 3.
		expect(page.meta.total).toBe(4);
	});

	it("still counts rows correctly without groupBy", async () => {
		const page = await PgOrder.query().paginate(1, 10);
		expect(page.meta.total).toBe(6);
	});
});
