/**
 * Transaction — wraps database operations in BEGIN/COMMIT/ROLLBACK.
 *
 * @implements MISS-1
 */

import { randomBytes } from "node:crypto";
import type { TransactionOptions } from "./adapters/NapiDbAdapter.js";
import type { DatabaseConnection } from "./BaseRepository.js";
import {
	makeTransactionQueryBuilders,
	type TransactionQueryBuilders,
} from "./query/DatabaseQueryBuilder.js";
import { getAtlasDialect } from "./query/native.js";
import {
	isTransactionClient,
	TRANSACTION_BRAND,
} from "./utils/transactionBrand.js";

/** A post-commit / post-rollback side effect (Lucid `trx.after(...)`). */
export type AfterHook = () => void | Promise<void>;

export interface TransactionClient
	extends Omit<DatabaseConnection, "query">,
		TransactionQueryBuilders {
	commit(): Promise<void>;
	rollback(): Promise<void>;
	/**
	 * Register a side effect to run AFTER the transaction is durable (Lucid
	 * `trx.after('commit' | 'rollback', cb)`). A `commit` hook fires only once the
	 * ROOT transaction commits — inside a nested (SAVEPOINT) transaction it is
	 * forwarded to the parent, so a later outer rollback never runs it. Errors
	 * thrown by a hook are swallowed (the caller already saw the transaction
	 * succeed).
	 */
	after(event: "commit" | "rollback", cb: AfterHook): void;
	readonly isNested: boolean;
	readonly [TRANSACTION_BRAND]: true;
}

/**
 * Run every registered after-hook, swallowing errors — a post-commit side
 * effect must never surface a failure on a transaction the caller already saw
 * commit (Lucid parity). Shared with the napi adapter's pinned-transaction path.
 */
export async function runAfterHooks(hooks: AfterHook[]): Promise<void> {
	for (const hook of hooks) {
		try {
			await hook();
		} catch {
			/* swallowed — the transaction already succeeded */
		}
	}
}

export async function transaction<T>(
	db: DatabaseConnection,
	callback: (trx: TransactionClient) => Promise<T> | T,
	options?: TransactionOptions,
): Promise<T> {
	if (isTransactionClient(db)) {
		const parent = db;
		const name = `sp_${randomBytes(6).toString("hex")}`;
		await parent.execute(`SAVEPOINT ${name}`, []);

		const commitHooks: AfterHook[] = [];
		const rollbackHooks: AfterHook[] = [];

		const base = {
			execute: parent.execute.bind(parent),
			query: parent.query.bind(parent),
			async commit() {
				await parent.execute(`RELEASE SAVEPOINT ${name}`, []);
				// A nested commit is NOT durable until the root commits — forward BOTH
				// hook sets to the parent. Commit hooks fire on the real (root) commit
				// and drop if the outer later rolls back. Rollback hooks must ALSO
				// forward: this savepoint's released work is folded into the parent, so
				// an outer rollback undoes it too — dropping them here would strand any
				// in-memory restoration a nested caller registered on `after('rollback')`
				// (the work IS rolled back, just by the parent). The parent fires exactly
				// one of its two hook sets, so no double-run.
				for (const hook of commitHooks) parent.after("commit", hook);
				for (const hook of rollbackHooks) parent.after("rollback", hook);
			},
			async rollback() {
				await parent.execute(`ROLLBACK TO SAVEPOINT ${name}`, []);
				// ROLLBACK TO leaves the savepoint ESTABLISHED — release it so it doesn't
				// stay stacked on the connection through a long outer transaction with
				// many nested failures. Best-effort: the rollback already unwound the
				// work, so a RELEASE failure must not surface. (PG/MySQL/SQLite all
				// accept RELEASE after ROLLBACK TO.)
				try {
					await parent.execute(`RELEASE SAVEPOINT ${name}`, []);
				} catch {
					/* best-effort — the savepoint is already logically unwound */
				}
				await runAfterHooks(rollbackHooks);
			},
			after(event: "commit" | "rollback", cb: AfterHook) {
				(event === "commit" ? commitHooks : rollbackHooks).push(cb);
			},
			isNested: true,
			[TRANSACTION_BRAND]: true as const,
		};
		const trx: TransactionClient = Object.assign(
			base,
			makeTransactionQueryBuilders(base, getAtlasDialect()),
		);

		try {
			const result = await callback(trx);
			await trx.commit();
			return result;
		} catch (err) {
			try {
				await trx.rollback();
			} catch {
				/* best-effort */
			}
			throw err;
		}
	}

	// Pinned interactive transaction (napi-backed): db.transaction() acquires ONE
	// connection and issues BEGIN on it; the managed form commits on success /
	// rolls back on throw, every statement on that same connection. This is the
	// CORRECT path. The BEGIN/COMMIT-over-the-pool fallback below is broken on a
	// pool (each db.execute() picks a different connection, so BEGIN/COMMIT and
	// the statements scatter — no atomicity); it's kept only for minimal
	// single-connection connections that lack db.transaction().
	if (typeof db.transaction === "function") {
		return db.transaction(callback, options);
	}

	await db.execute("BEGIN", []);

	const commitHooks: AfterHook[] = [];
	const rollbackHooks: AfterHook[] = [];

	const base = {
		execute: db.execute.bind(db),
		query: db.query.bind(db),
		async commit() {
			await db.execute("COMMIT", []);
			await runAfterHooks(commitHooks);
		},
		async rollback() {
			await db.execute("ROLLBACK", []);
			await runAfterHooks(rollbackHooks);
		},
		after(event: "commit" | "rollback", cb: AfterHook) {
			(event === "commit" ? commitHooks : rollbackHooks).push(cb);
		},
		isNested: false,
		[TRANSACTION_BRAND]: true as const,
	};
	const trx: TransactionClient = Object.assign(
		base,
		makeTransactionQueryBuilders(base, getAtlasDialect()),
	);

	try {
		const result = await callback(trx);
		await trx.commit();
		return result;
	} catch (err) {
		try {
			await trx.rollback();
		} catch {
			/* best-effort */
		}
		throw err;
	}
}
