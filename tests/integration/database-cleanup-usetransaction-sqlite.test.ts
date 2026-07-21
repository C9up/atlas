/**
 * `DatabaseCleanup.useTransaction` (Adonis Lucid parity): a test wraps its work
 * in a pinned interactive transaction; `rollback()` reverts every write on that
 * one connection, so the test leaves no trace — no cross-pool savepoint scatter.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { useTransaction } from "../../src/testing/DatabaseCleanup.js";

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
});

afterEach(async () => {
	await conn?.close();
});

describe("atlas > DatabaseCleanup.useTransaction (pinned rollback)", () => {
	it("rolls back writes made through the pinned trx, isolating the test", async () => {
		const { trx, rollback } = await useTransaction(conn);
		await trx.execute("INSERT INTO items (name) VALUES ('a'), ('b')");
		// Visible inside the transaction...
		const inside = await trx.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM items",
		);
		expect(inside[0]?.n).toBe(2);

		await rollback();

		// ...and gone afterwards: the connection is back in the pool, empty.
		const after = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM items",
		);
		expect(after[0]?.n).toBe(0);
	});
});
