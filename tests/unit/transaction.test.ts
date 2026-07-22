import { describe, expect, it } from "vitest";
import type { DatabaseConnection } from "../../src/BaseRepository.js";
import { makeTransactionQueryBuilders } from "../../src/query/DatabaseQueryBuilder.js";
import { type TransactionClient, transaction } from "../../src/Transaction.js";
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
				return Promise.reject(new Error(`recorder: forced failure on ${sql}`));
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

describe("atlas > transaction (pinned via db.transaction)", () => {
	function makePinned(): {
		poolEvents: string[];
		trxEvents: string[];
		state: { committed: boolean; rolledBack: boolean };
		db: DatabaseConnection;
	} {
		const poolEvents: string[] = [];
		const trxEvents: string[] = [];
		const state = { committed: false, rolledBack: false };
		const commitHooks: Array<() => void | Promise<void>> = [];
		const rollbackHooks: Array<() => void | Promise<void>> = [];
		const trxBase = {
			execute(sql: string) {
				trxEvents.push(sql);
				return Promise.resolve({ rowsAffected: 1 });
			},
			query() {
				return Promise.resolve([]);
			},
			async commit() {
				state.committed = true;
				for (const h of commitHooks) await h();
			},
			async rollback() {
				state.rolledBack = true;
				for (const h of rollbackHooks) await h();
			},
			after(event: "commit" | "rollback", cb: () => void | Promise<void>) {
				(event === "commit" ? commitHooks : rollbackHooks).push(cb);
			},
			isNested: false,
			[TRANSACTION_BRAND]: true as const,
		};
		const trxClient: TransactionClient = Object.assign(
			trxBase,
			makeTransactionQueryBuilders(trxBase, "sqlite"),
		);
		const db: DatabaseConnection = {
			execute(sql: string) {
				poolEvents.push(sql);
				return Promise.resolve({ rowsAffected: 0 });
			},
			query() {
				return Promise.resolve([]);
			},
			// Managed form (Lucid): run the callback on the pinned client, auto
			// commit on success / rollback on throw.
			async transaction<T>(
				callback: (trx: TransactionClient) => Promise<T> | T,
			): Promise<T> {
				try {
					const result = await callback(trxClient);
					await trxClient.commit();
					return result;
				} catch (err) {
					await trxClient.rollback();
					throw err;
				}
			},
		};
		return { poolEvents, trxEvents, state, db };
	}

	it("delegates to db.transaction() — never pulls BEGIN/COMMIT through the pool", async () => {
		const { poolEvents, trxEvents, state, db } = makePinned();
		const result = await transaction(db, async (trx) => {
			await trx.execute("INSERT INTO t VALUES (1)");
			return "ok";
		});
		expect(result).toBe("ok");
		expect(state.committed).toBe(true);
		expect(state.rolledBack).toBe(false);
		// Statements ran on the pinned handle…
		expect(trxEvents).toEqual(["INSERT INTO t VALUES (1)"]);
		// …and NOTHING was issued through the pool (the broken scatter path).
		expect(poolEvents).toEqual([]);
	});

	it("rolls back through the pinned handle when the callback throws", async () => {
		const { state, db } = makePinned();
		await expect(
			transaction(db, () => {
				throw new Error("boom");
			}),
		).rejects.toThrow("boom");
		expect(state.rolledBack).toBe(true);
		expect(state.committed).toBe(false);
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
		// ROLLBACK TO unwinds the work; RELEASE then pops the savepoint off the
		// connection so it doesn't stay stacked through a long outer transaction.
		const sqls = events.map((e) => e.sql);
		expect(
			sqls.some((s) => /^ROLLBACK TO SAVEPOINT sp_[0-9a-f]+$/.test(s)),
		).toBe(true);
		expect(events[events.length - 1]?.sql).toMatch(
			/^RELEASE SAVEPOINT sp_[0-9a-f]+$/,
		);
	});

	it("exposes the nested trx with isNested=true", async () => {
		const { db } = brandedRecorder();
		await transaction(db, (trx) => {
			expect(trx.isNested).toBe(true);
		});
	});
});
