/**
 * Nested transactions via SAVEPOINT — Lucid `const sp = await trx.transaction()`
 * (manual + managed) and the EventEmitter-style `trx.on('commit'|'rollback')`.
 * SQLite + Postgres support SAVEPOINT; MySQL's prepared-statement protocol
 * rejects SAVEPOINT (documented driver limitation), so this suite is SQLite.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

let c: AsyncDatabaseConnection;
beforeEach(async () => {
	c = await createNapiConnection("sqlite::memory:", 1, 1);
	await c.execute("CREATE TABLE sp_t (id INTEGER PRIMARY KEY)");
});
afterEach(async () => {
	await c?.close();
});

describe("atlas > nested transactions (SAVEPOINT) + trx.on()", () => {
	it("manual trx.transaction(): rollback undoes the savepoint, keeps root work", async () => {
		const trx = await c.transaction();
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
		const trx = await c.transaction();
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

	it("managed trx.transaction(cb) rolls the savepoint back on throw, keeps root", async () => {
		const trx = await c.transaction();
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
