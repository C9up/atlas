import { describe, expect, it } from "vitest";
import { Database } from "../../src/testing/TestDatabase.js";

/**
 * Binding an array as ONE parameter used to reach the binder's catch-all and
 * go out as the JSON text `["a","b"]`. On Postgres, `WHERE id = ANY($1)` makes
 * the server read those text bytes as a binary array header and report a
 * dimension count read out of whatever was there — a different number on every
 * run, and nothing pointing at the parameter.
 */
describe("atlas > array parameters", () => {
	it("refuses an array and names the expansion to use", async () => {
		const db = await Database.memory();
		await db.execute("CREATE TABLE a (id TEXT)");

		await expect(
			db.queryWithParams("SELECT * FROM a WHERE id = ?", [["x", "y"]]),
		).rejects.toThrowError(/E_ARRAY_PARAM.*whereIn/s);

		await db.close();
	});

	it("names WHICH parameter, so a long list is not a hunt", async () => {
		const db = await Database.memory();
		await db.execute("CREATE TABLE a (id TEXT, n INT)");

		await expect(
			db.queryWithParams("SELECT * FROM a WHERE id = ? AND n = ?", [
				"x",
				[1, 2],
			]),
		).rejects.toThrowError(/Parameter \$2 is an array/);

		await db.close();
	});

	it("leaves a nested array alone — that is a JSON value, not a bind list", async () => {
		const db = await Database.memory();
		await db.execute("CREATE TABLE a (doc TEXT)");

		// The array is INSIDE an object bound to one column, which is what a
		// json/jsonb column receives. Only the top level is a bind list.
		await db.execute("INSERT INTO a (doc) VALUES (?)", [
			JSON.stringify({ tags: ["x", "y"] }),
		]);
		const rows = await db.queryWithParams("SELECT doc FROM a", []);
		expect(JSON.parse(String(rows[0]?.doc))).toEqual({ tags: ["x", "y"] });

		await db.close();
	});
});
