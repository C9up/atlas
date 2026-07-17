/**
 * #4: the napi JSON boundary must not corrupt data.
 *  - integers beyond JS's safe range (±2^53−1) come back as exact STRINGS
 *    (matching the pg/mysql driver convention) instead of rounded f64 numbers;
 *  - a `BigInt` param binds losslessly (a plain JSON.stringify THROWS on BigInt);
 *  - a `Uint8Array` param round-trips through a BLOB column as a `Uint8Array`
 *    (a plain JSON.stringify turns it into a useless index-map).
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, big INTEGER, small INTEGER, blob BLOB)",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > napi transport preserves bigint + binary (#4)", () => {
	it("reads an integer beyond 2^53 as an exact string; small ints stay numbers", async () => {
		await conn.execute(
			"INSERT INTO t (id, big, small) VALUES (1, 9223372036854775807, 42)",
		);
		const rows = await conn.query<{ big: unknown; small: unknown }>(
			"SELECT big, small FROM t WHERE id = 1",
		);
		// i64::MAX would round to 9223372036854775808 as an f64 — here it is exact.
		expect(rows[0]?.big).toBe("9223372036854775807");
		expect(rows[0]?.small).toBe(42);
	});

	it("binds a BigInt param losslessly", async () => {
		const huge = 9223372036854775806n;
		await conn.execute("INSERT INTO t (id, big) VALUES (?, ?)", [2, huge]);
		const rows = await conn.query<{ big: unknown }>(
			"SELECT big FROM t WHERE id = ?",
			[2],
		);
		expect(rows[0]?.big).toBe("9223372036854775806");
	});

	it("round-trips binary through a BLOB column as a Uint8Array", async () => {
		const bytes = new Uint8Array([0, 1, 2, 255, 128]);
		await conn.execute("INSERT INTO t (id, blob) VALUES (?, ?)", [3, bytes]);
		const rows = await conn.query<{ blob: unknown }>(
			"SELECT blob FROM t WHERE id = ?",
			[3],
		);
		const blob = rows[0]?.blob;
		expect(blob).toBeInstanceOf(Uint8Array);
		if (!(blob instanceof Uint8Array)) throw new Error("expected Uint8Array");
		expect(Array.from(blob)).toEqual([0, 1, 2, 255, 128]);
	});
});
