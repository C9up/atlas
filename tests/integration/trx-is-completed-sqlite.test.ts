/**
 * A helper handed a transaction could not tell whether it was still usable —
 * issuing a statement on a finished one failed at the driver with an error
 * that says nothing about the transaction (Lucid `trx.isCompleted`).
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
});
afterAll(async () => {
	await conn?.close();
});

describe("atlas > trx.isCompleted", () => {
	it("is false while open, true after commit", async () => {
		const trx = await conn.transaction?.();
		if (!trx) throw new Error("expected a transaction");
		expect(trx.isCompleted).toBe(false);
		await trx.commit();
		expect(trx.isCompleted).toBe(true);
	});

	it("is true after rollback too", async () => {
		const trx = await conn.transaction?.();
		if (!trx) throw new Error("expected a transaction");
		await trx.rollback();
		expect(trx.isCompleted).toBe(true);
	});

	it("tracks a savepoint independently of its parent", async () => {
		const trx = await conn.transaction?.();
		if (!trx) throw new Error("expected a transaction");
		const sp = await trx.transaction();
		await sp.commit();
		expect(sp.isCompleted).toBe(true);
		// Releasing a savepoint does not end the transaction that holds it.
		expect(trx.isCompleted).toBe(false);
		await trx.rollback();
	});
});
