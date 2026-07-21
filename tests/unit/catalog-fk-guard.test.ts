/**
 * `runWithoutForeignKeys` is fail-closed on MySQL: suspending FK checks needs an
 * interactive `transaction()` to restore them in a `finally` (a session-level
 * `SET` can't be rolled back). Without one it REFUSES rather than risk leaving a
 * pooled connection with FK checks disabled.
 */
import { describe, expect, it } from "vitest";
import {
	type CatalogConnection,
	runWithoutForeignKeys,
} from "../../src/schema/catalog.js";

/** A minimal adapter with runInTransaction but NO interactive transaction(). */
function fakeNoTx(): { db: CatalogConnection; ran: string[] } {
	const ran: string[] = [];
	const db: CatalogConnection = {
		async query<T>(): Promise<T[]> {
			return [];
		},
		async execute(sql: string) {
			ran.push(sql);
			return { rowsAffected: 0 };
		},
		async runInTransaction(batch) {
			for (const stmt of batch) ran.push(stmt.sql);
			return batch.length;
		},
	};
	return { db, ran };
}

describe("atlas > catalog > runWithoutForeignKeys (MySQL fail-closed)", () => {
	it("refuses MySQL FK suspension when the adapter has no transaction()", async () => {
		const { db, ran } = fakeNoTx();
		await expect(
			runWithoutForeignKeys(db, "mysql", [{ sql: "DELETE FROM users" }]),
		).rejects.toThrow(/transaction\(\)/i);
		// Nothing ran — it never toggled FK checks off, so the connection is clean.
		expect(ran).toHaveLength(0);
	});

	it("runs SQLite via the pinned batch (no transaction() needed there)", async () => {
		const { db, ran } = fakeNoTx();
		await runWithoutForeignKeys(db, "sqlite", [{ sql: "DELETE FROM users" }]);
		expect(ran).toContain("PRAGMA defer_foreign_keys = ON");
		expect(ran).toContain("DELETE FROM users");
	});
});
