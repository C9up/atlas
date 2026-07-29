/**
 * `testUtils.db()` (AdonisJS parity): truncate returns a teardown that empties
 * user tables; withGlobalTransaction hands back a pinned trx + rollback. Run
 * against an in-memory SQLite connection.
 */

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";
import { testUtils } from "../../src/testing/TestUtils.js";

let conn: Awaited<ReturnType<typeof createNapiConnection>>;

beforeEach(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)",
	);
});

afterEach(async () => {
	await conn.close();
});

describe("atlas > testUtils.db()", () => {
	it("truncate() returns a teardown that empties user tables", async () => {
		await conn.execute("INSERT INTO widgets (name) VALUES ('a'), ('b')");
		const before = await conn.query<{ c: number }>(
			"SELECT COUNT(*) AS c FROM widgets",
		);
		expect(Number(before[0]?.c)).toBe(2);

		const teardown = testUtils(conn).db().truncate();
		await teardown();

		const after = await conn.query<{ c: number }>(
			"SELECT COUNT(*) AS c FROM widgets",
		);
		expect(Number(after[0]?.c)).toBe(0);
	});

	it("withGlobalTransaction() rolls back the test's writes on the pinned trx", async () => {
		const { trx, rollback } = await testUtils(conn)
			.db()
			.withGlobalTransaction();
		// Writes go through the pinned trx (atlas requires explicit trx use).
		await trx.execute("INSERT INTO widgets (name) VALUES ('temp')");
		await rollback();

		const rows = await conn.query<{ c: number }>(
			"SELECT COUNT(*) AS c FROM widgets",
		);
		expect(Number(rows[0]?.c)).toBe(0);
	});

	it("seed() without a seedersDir throws a clear error", async () => {
		await expect(testUtils(conn).db().seed()).rejects.toThrow(/seedersDir/);
	});
});
