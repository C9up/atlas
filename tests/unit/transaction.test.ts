import { describe, expect, it } from "vitest";
import type { DatabaseConnection } from "../../src/BaseRepository.js";
import { transaction } from "../../src/Transaction.js";
import { TRANSACTION_BRAND } from "../../src/utils/transactionBrand.js";

interface SqlEvent {
	sql: string;
}

function makeRecorder(opts?: { failOn?: RegExp }): {
	events: SqlEvent[];
	db: DatabaseConnection;
} {
	const events: SqlEvent[] = [];
	const db: DatabaseConnection = {
		execute(sql: string) {
			events.push({ sql });
			if (opts?.failOn?.test(sql)) {
				return Promise.reject(
					new Error(`recorder: forced failure on ${sql}`),
				);
			}
			return Promise.resolve({ rowsAffected: 0 });
		},
		query() {
			return Promise.resolve([]);
		},
	};
	return { events, db };
}

describe("atlas > transaction (top-level)", () => {
	it("wraps the callback in BEGIN/COMMIT on success", async () => {
		const { events, db } = makeRecorder();
		const result = await transaction(db, () => 42);
		expect(result).toBe(42);
		expect(events.map((e) => e.sql)).toEqual(["BEGIN", "COMMIT"]);
	});

	it("rolls back on a thrown callback and re-throws the original error", async () => {
		const { events, db } = makeRecorder();
		await expect(
			transaction(db, () => {
				throw new Error("boom");
			}),
		).rejects.toThrow("boom");
		expect(events.map((e) => e.sql)).toEqual(["BEGIN", "ROLLBACK"]);
	});

	it("supports an async callback that resolves", async () => {
		const { events, db } = makeRecorder();
		const result = await transaction(db, async () => "ok");
		expect(result).toBe("ok");
		expect(events.map((e) => e.sql)).toEqual(["BEGIN", "COMMIT"]);
	});

	it("swallows ROLLBACK failures (best-effort) but still propagates the original error", async () => {
		// Recorder fails the second prepared statement (ROLLBACK).
		const { db } = makeRecorder({ failOn: /^ROLLBACK$/ });
		await expect(
			transaction(db, () => {
				throw new Error("original");
			}),
		).rejects.toThrow("original");
	});

	it("exposes the trx with isNested=false on a top-level transaction", async () => {
		const { db } = makeRecorder();
		await transaction(db, (trx) => {
			expect(trx.isNested).toBe(false);
			expect(trx[TRANSACTION_BRAND]).toBe(true);
		});
	});
});

describe("atlas > transaction (nested via savepoint)", () => {
	function brandedRecorder(opts?: { failOn?: RegExp }): {
		events: SqlEvent[];
		db: DatabaseConnection;
	} {
		const inner = makeRecorder(opts);
		const branded = Object.assign(inner.db, { [TRANSACTION_BRAND]: true });
		return { events: inner.events, db: branded };
	}

	it("uses SAVEPOINT/RELEASE when called on an existing TransactionClient", async () => {
		const { events, db } = brandedRecorder();
		await transaction(db, () => "ok");
		expect(events).toHaveLength(2);
		expect(events[0]?.sql).toMatch(/^SAVEPOINT sp_[0-9a-f]+$/);
		expect(events[1]?.sql).toMatch(/^RELEASE SAVEPOINT sp_[0-9a-f]+$/);
	});

	it("rolls back to the savepoint on inner failure", async () => {
		const { events, db } = brandedRecorder();
		await expect(
			transaction(db, () => {
				throw new Error("inner");
			}),
		).rejects.toThrow("inner");
		expect(events[0]?.sql).toMatch(/^SAVEPOINT/);
		expect(events[events.length - 1]?.sql).toMatch(/^ROLLBACK TO SAVEPOINT/);
	});

	it("exposes the nested trx with isNested=true", async () => {
		const { db } = brandedRecorder();
		await transaction(db, (trx) => {
			expect(trx.isNested).toBe(true);
		});
	});
});
