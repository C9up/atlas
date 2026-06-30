import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { transaction } from "../../src/Transaction.js";

// transaction is optional on AsyncDatabaseConnection (test doubles may omit it),
// but a real napi connection always provides it. Narrow the optional member
// without a cast for the direct-call tests below.
function dbTransaction(c: AsyncDatabaseConnection) {
	if (!c.transaction) {
		throw new Error("napi connection must provide transaction()");
	}
	return c.transaction;
}

// A pool of exactly ONE connection is the tightest proof of pinning: an
// interactive transaction holds that single connection, so its inner
// query/execute MUST reuse the held connection rather than re-acquire from the
// pool (which would block forever). The old BEGIN/COMMIT-over-the-pool path
// could never satisfy this.
let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE counters (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
	);
	await conn.execute("INSERT INTO counters (id, n) VALUES (1, 0)");
});

afterAll(async () => {
	await conn.close();
});

describe("atlas > interactive transaction (real napi sqlite)", () => {
	it("exposes transaction() on the napi connection", () => {
		expect(typeof conn.transaction).toBe("function");
	});

	it("commits a read-then-decide-then-write atomically on the pinned connection", async () => {
		const next = await transaction(conn, async (trx) => {
			const [row] = await trx.query<{ n: number }>(
				"SELECT n FROM counters WHERE id = 1",
			);
			const n = row.n + 1;
			await trx.execute("UPDATE counters SET n = ? WHERE id = 1", [n]);
			return n;
		});
		expect(next).toBe(1);

		const [row] = await conn.query<{ n: number }>(
			"SELECT n FROM counters WHERE id = 1",
		);
		expect(row.n).toBe(1);
	});

	it("rolls back on a thrown callback — no partial write survives", async () => {
		await expect(
			transaction(conn, async (trx) => {
				await trx.execute("UPDATE counters SET n = 999 WHERE id = 1");
				throw new Error("abort");
			}),
		).rejects.toThrow("abort");

		const [row] = await conn.query<{ n: number }>(
			"SELECT n FROM counters WHERE id = 1",
		);
		expect(row.n).toBe(1); // the committed value, not the rolled-back 999
	});

	it("does not poison the pool: a write trx commits and the next acquire succeeds", async () => {
		await transaction(conn, async (trx) => {
			await trx.execute("UPDATE counters SET n = n + 1 WHERE id = 1");
		});
		// The connection was handed back on commit, so this acquire returns
		// immediately instead of timing out on a stranded lock.
		const [row] = await conn.query<{ n: number }>(
			"SELECT n FROM counters WHERE id = 1",
		);
		expect(row.n).toBe(2);
	});

	it("manual form: db.transaction() returns a handle you commit yourself", async () => {
		const trx = await dbTransaction(conn)();
		await trx.execute("UPDATE counters SET n = 7 WHERE id = 1");
		await trx.commit();
		const [row] = await conn.query<{ n: number }>(
			"SELECT n FROM counters WHERE id = 1",
		);
		expect(row.n).toBe(7);
	});

	it("accepts an isolationLevel option (ignored on sqlite, no error)", async () => {
		const out = await dbTransaction(conn)(
			async (trx) => {
				await trx.execute("UPDATE counters SET n = n WHERE id = 1");
				return "done";
			},
			{ isolationLevel: "serializable" },
		);
		expect(out).toBe("done");
	});
});
