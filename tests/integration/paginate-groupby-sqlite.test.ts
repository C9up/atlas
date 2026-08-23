/**
 * `paginate()` on a grouped query counted with a flat `SELECT COUNT(*) … GROUP
 * BY`, which returns one row PER GROUP. The total was therefore the FIRST
 * group's size — a pager that reports 2 pages when there are 7, silently.
 * ModelQuery.paginate already wrapped the grouped query in a subquery; the
 * database builder did not.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { DatabaseQueryBuilder } from "../../src/query/DatabaseQueryBuilder.js";

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)",
	);
	// 3 regions: north has 5 rows, south 1, east 1 → 7 rows, 3 groups.
	const rows: Array<[number, string, number]> = [
		[1, "north", 10],
		[2, "north", 20],
		[3, "north", 30],
		[4, "north", 40],
		[5, "north", 50],
		[6, "south", 60],
		[7, "east", 70],
	];
	for (const [id, region, amount] of rows) {
		await conn.execute("INSERT INTO sales VALUES (?, ?, ?)", [
			id,
			region,
			amount,
		]);
	}
});

afterAll(async () => {
	await conn?.close();
});

const builder = () => new DatabaseQueryBuilder(conn, "sqlite").from("sales");

describe("atlas > paginate with groupBy", () => {
	it("counts the GROUPS, not the first group's rows", async () => {
		const page = await builder()
			.select("region")
			.groupBy("region")
			.paginate(1, 2);
		// 3 groups → 2 pages. The old count returned 5 (north's row count).
		expect(page.total).toBe(3);
		expect(page.lastPage).toBe(2);
	});

	it("still counts rows when there is no grouping", async () => {
		const page = await builder().paginate(1, 3);
		expect(page.total).toBe(7);
		expect(page.all()).toHaveLength(3);
	});

	it("leaves the builder unsliced for the next run", async () => {
		const b = builder();
		await b.paginate(2, 2);
		// A builder left carrying LIMIT/OFFSET silently truncated the next read.
		expect(await b).toHaveLength(7);
	});
});
