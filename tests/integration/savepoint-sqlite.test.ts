/**
 * Nested transactions via SAVEPOINT on SQLite + the EventEmitter-style
 * `trx.on('commit'|'rollback')` semantics (dialect-agnostic, exercised here).
 * Cross-dialect SAVEPOINT behaviour (incl. MySQL, which routes through the text
 * protocol) lives in `savepoint-crossdialect.test.ts`.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

let c: AsyncDatabaseConnection;

/** `transaction` is optional on the interface; napi connections always have it. */
function begin(conn: AsyncDatabaseConnection) {
	if (!conn.transaction) throw new Error("connection lacks transaction()");
	return conn.transaction();
}

beforeEach(async () => {
	c = await createNapiConnection("sqlite::memory:", 1, 1);
	await c.execute("CREATE TABLE sp_t (id INTEGER PRIMARY KEY)");
});
afterEach(async () => {
	await c?.close();
});

describe("atlas > nested transactions (SAVEPOINT) + trx.on()", () => {
	it("manual trx.transaction(): rollback undoes the savepoint, keeps root work", async () => {
		const trx = await begin(c);
		await trx.execute("INSERT INTO sp_t (id) VALUES (1)", []);
		const sp = await trx.transaction();
		await sp.execute("INSERT INTO sp_t (id) VALUES (2)", []);
		await sp.rollback();
		await trx.commit();
		const rows = await c.query<{ id: number }>(
			"SELECT id FROM sp_t ORDER BY id",
			[],
		);
		expect(rows.map((r) => r.id)).toEqual([1]);
	});

	it("managed trx.transaction(cb) commits the savepoint; trx.on('commit') fires on root commit", async () => {
		let committed = false;
		const trx = await begin(c);
		expect(
			trx.on("commit", () => {
				committed = true;
			}),
		).toBe(trx); // chainable
		await trx.transaction(async (sp) => {
			await sp.execute("INSERT INTO sp_t (id) VALUES (5)", []);
		});
		expect(committed).toBe(false); // not until the ROOT commits
		await trx.commit();
		expect(committed).toBe(true);
		const rows = await c.query<{ id: number }>("SELECT id FROM sp_t", []);
		expect(rows.map((r) => r.id)).toEqual([5]);
	});

	it("trx.on('commit') is a real EventEmitter: synchronous, once(), off()", async () => {
		const trx = await begin(c);
		let onCount = 0;
		let onceCount = 0;
		trx.on("commit", () => {
			onCount++;
		});
		trx.once("commit", () => {
			onceCount++;
		});
		const offCb = () => {
			onCount += 100;
		};
		trx.on("commit", offCb);
		trx.off("commit", offCb); // unsubscribed before commit
		await trx.commit();
		expect(onCount).toBe(1); // the off'd listener didn't run
		expect(onceCount).toBe(1);
	});

	it("on() fires per-client: a savepoint's event is distinct from the root's", async () => {
		const trx = await begin(c);
		let rootFired = false;
		let spFired = false;
		trx.on("commit", () => {
			rootFired = true;
		});
		const sp = await trx.transaction();
		sp.on("commit", () => {
			spFired = true;
		});
		await sp.commit();
		expect(spFired).toBe(true); // the savepoint's own commit fired its listener
		expect(rootFired).toBe(false); // the root hasn't committed yet
		await trx.commit();
		expect(rootFired).toBe(true);
	});

	it("managed trx.transaction(cb) rolls the savepoint back on throw, keeps root", async () => {
		const trx = await begin(c);
		await trx.execute("INSERT INTO sp_t (id) VALUES (7)", []);
		await expect(
			trx.transaction(async (sp) => {
				await sp.execute("INSERT INTO sp_t (id) VALUES (8)", []);
				throw new Error("boom");
			}),
		).rejects.toThrow("boom");
		await trx.commit();
		const rows = await c.query<{ id: number }>(
			"SELECT id FROM sp_t ORDER BY id",
			[],
		);
		expect(rows.map((r) => r.id)).toEqual([7]);
	});
});
