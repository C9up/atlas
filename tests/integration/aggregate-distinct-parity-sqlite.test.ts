/**
 * Every aggregate answers the same shape, because upstream gives all eight the
 * same one.
 *
 * `count`, `countDistinct`, `min`, `max`, `sum`, `sumDistinct`, `avg` and
 * `avgDistinct` are typed `Aggregate<this>` there: each takes a column (with an
 * optional alias, inline or as a second argument, or an object of them) and
 * returns the BUILDER. The value comes back as a column of the result row.
 *
 * atlas used to split them by argument — an aliased expression projected and
 * chained, a bare column ran the query and answered a number — and the split
 * was not applied consistently: `count('appid')` chained while
 * `countDistinct('appid')` resolved to a number. Code written from one shape
 * failed on the other, and the failure was a 500, not a type error.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import db, { clearDb, setDb } from "../../src/services/db.js";

class Sale extends BaseModel {
	static override table = "sales";
	@PrimaryKey() declare id: number;
	@Column() declare appid: number;
	@Column() declare amount: number;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE sales (id INTEGER PRIMARY KEY, appid INTEGER, amount INTEGER)",
	);
	// Three rows, two distinct appids.
	await conn.execute("INSERT INTO sales VALUES (1,10,5),(2,10,7),(3,20,9)");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn.close();
});

describe("atlas > every aggregate is a projection", () => {
	it("returns the builder for a bare column, exactly as count does", async () => {
		// The asymmetry that cost a 500: these two used to answer different
		// types for the same argument shape.
		expect(await db.from("sales").count("appid")).toEqual([
			{ "COUNT(appid)": 3 },
		]);
		expect(await db.from("sales").countDistinct("appid")).toEqual([
			{ "COUNT(DISTINCT appid)": 2 },
		]);
	});

	it("takes the inline alias", async () => {
		// This is the call that used to compile to `COUNT(DISTINCT appid as n)`.
		expect(await db.from("sales").countDistinct("appid as n")).toEqual([
			{ n: 2 },
		]);
	});

	it("takes the alias as a second argument", async () => {
		expect(await db.from("sales").countDistinct("appid", "n")).toEqual([
			{ n: 2 },
		]);
	});

	it("takes an object of aliases", async () => {
		expect(await db.from("sales").count({ rows: "*", ids: "appid" })).toEqual([
			{ rows: 3, ids: 3 },
		]);
	});

	it("takes several columns at once", async () => {
		expect(
			await db.from("sales").min(["amount as smallest", "appid as first_app"]),
		).toEqual([{ smallest: 5, first_app: 10 }]);
	});

	it("puts DISTINCT inside the parentheses and the alias outside", () => {
		const { sql } = db.from("sales").countDistinct("appid as n").toSQL();
		expect(sql).toContain("COUNT(DISTINCT appid) AS n");
	});

	it("gives sumDistinct and avgDistinct the same shape", async () => {
		expect(await db.from("sales").sumDistinct("appid as total")).toEqual([
			{ total: 30 },
		]);
		expect(await db.from("sales").avgDistinct("appid as mean")).toEqual([
			{ mean: 15 },
		]);
	});

	it("holds on the model builder too, where the value lands in $extras", async () => {
		const [row] = await Sale.query().countDistinct("appid as n");
		expect(Number(row?.$extras.n)).toBe(2);

		const [both] = await Sale.query().count({ rows: "*" }).sum("amount as sum");
		expect(Number(both?.$extras.rows)).toBe(3);
		expect(Number(both?.$extras.sum)).toBe(21);
	});

	it("keeps chaining after an aggregate, on either builder", async () => {
		expect(
			await db
				.from("sales")
				.countDistinct("appid as n")
				.where("amount", ">", 5),
		).toEqual([{ n: 2 }]);
	});
});
