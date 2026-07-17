/**
 * A nested savepoint that COMMITS (RELEASE) folds its work into the parent. If the
 * parent later rolls back, that work is undone too — so the savepoint's
 * `after('rollback')` hooks must still fire. Before the fix, `commit()` forwarded
 * only the commit hooks and DROPPED the rollback hooks, stranding any in-memory
 * restoration a nested caller registered.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { transaction } from "../../src/Transaction.js";

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > nested savepoint forwards rollback hooks to the parent", () => {
	it("fires a committed savepoint's rollback hook when the OUTER rolls back", async () => {
		let innerRollbackFired = false;
		let innerCommitFired = false;

		await expect(
			transaction(conn, async (outer) => {
				await transaction(outer, async (inner) => {
					inner.after("rollback", () => {
						innerRollbackFired = true;
					});
					inner.after("commit", () => {
						innerCommitFired = true;
					});
					// inner returns normally → inner COMMITs (RELEASE SAVEPOINT).
				});
				// Now force the OUTER (root) to roll back.
				throw new Error("outer boom");
			}),
		).rejects.toThrow("outer boom");

		// The inner work was rolled back with the parent → its rollback hook runs,
		// its commit hook does NOT (the root never committed).
		expect(innerRollbackFired).toBe(true);
		expect(innerCommitFired).toBe(false);
	});

	it("fires a committed savepoint's commit hook when the OUTER commits", async () => {
		let innerRollbackFired = false;
		let innerCommitFired = false;

		await transaction(conn, async (outer) => {
			await transaction(outer, async (inner) => {
				inner.after("rollback", () => {
					innerRollbackFired = true;
				});
				inner.after("commit", () => {
					innerCommitFired = true;
				});
			});
			// outer returns normally → root COMMITs.
		});

		expect(innerCommitFired).toBe(true);
		expect(innerRollbackFired).toBe(false);
	});
});
