/**
 * `countDistinct` must answer the same shapes as `count`.
 *
 * Upstream splits the ` as ` alias in its shared aggregate compiler, after
 * applying DISTINCT, so every aggregate there takes the same `col as alias`
 * string. Here the `*Distinct` trio took the column verbatim:
 * `countDistinct('appid as n')` compiled to `COUNT(DISTINCT appid as n)` and
 * the server refused it, while the very same string through `count` worked.
 *
 * Writing one from the other therefore gave a SQL error — or, worse, a silent
 * `NaN`, when a caller expecting a number got the builder back and read the
 * alias off a row that was never fetched.
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

describe("atlas > countDistinct answers the same shapes as count", () => {
	it("returns a number for a bare column, like count()", async () => {
		expect(await db.from("sales").count()).toBe(3);
		expect(await db.from("sales").countDistinct("appid")).toBe(2);
	});

	it("takes the alias form count() takes", async () => {
		// This is the call that used to compile to `COUNT(DISTINCT appid as n)`.
		const rows = await db.from("sales").countDistinct("appid as n");
		expect(rows).toEqual([{ n: 2 }]);
	});

	it("requires the column — a named deviation", () => {
		// Upstream defaults it to `*`, but `COUNT(DISTINCT *)` is not valid SQL
		// on any engine here: that default only ever produced a statement the
		// server refused. Asking for the column names the mistake at the call
		// site instead of at the database.
		// @ts-expect-error — the column is not optional
		expect(() => db.from("sales").countDistinct()).toBeTruthy();
	});

	it("puts DISTINCT inside the parentheses and the alias outside", () => {
		const { sql } = db.from("sales").countDistinct("appid as n").toSQL();
		expect(sql).toContain("COUNT(DISTINCT appid) AS n");
	});

	it("gives sumDistinct and avgDistinct the same two shapes", async () => {
		expect(await db.from("sales").sumDistinct("appid")).toBe(30);
		expect(await db.from("sales").sumDistinct("appid as total")).toEqual([
			{ total: 30 },
		]);
		expect(await db.from("sales").avgDistinct("appid")).toBe(15);
	});

	it("holds on the model builder too", async () => {
		expect(await Sale.query().count()).toBe(3);
		expect(await Sale.query().countDistinct("appid")).toBe(2);

		// Lucid's `Aggregate<this>` form: the builder comes back and the value
		// lands in `$extras`.
		const [row] = await Sale.query().countDistinct("appid as n");
		expect(Number(row?.$extras.n)).toBe(2);
	});

	it("keeps chaining after the aliased form, like count does", async () => {
		const rows = await db
			.from("sales")
			.countDistinct("appid as n")
			.where("amount", ">", 5);
		expect(rows).toEqual([{ n: 2 }]);
	});
});
