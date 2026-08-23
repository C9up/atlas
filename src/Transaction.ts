/**
 * Transaction — wraps database operations in BEGIN/COMMIT/ROLLBACK.
 *
 * @implements MISS-1
 */

import { randomBytes } from "node:crypto";
import { EventEmitter } from "node:events";
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
	 * Whether this transaction has already committed or rolled back (Lucid
	 * `trx.isCompleted`).
	 *
	 * A helper handed a transaction cannot otherwise tell whether it is still
	 * usable, and issuing a statement on a finished one fails at the driver with
	 * an error that says nothing about the transaction.
	 */
	readonly isCompleted: boolean;
	/**
	 * Register a side effect to run AFTER the transaction is durable (Lucid
	 * `trx.after('commit' | 'rollback', cb)`). A `commit` hook fires only once the
	 * ROOT transaction commits — inside a nested (SAVEPOINT) transaction it is
	 * forwarded to the parent, so a later outer rollback never runs it. Errors
	 * thrown by a hook are swallowed (the caller already saw the transaction
	 * succeed).
	 */
	after(event: "commit" | "rollback", cb: AfterHook): void;
	/**
	 * Subscribe to the client's `commit`/`rollback` EventEmitter (Lucid — the trx
	 * client IS a Node EventEmitter). These are SYNCHRONOUS, pre-hook notifications
	 * fired the moment this client commits/rolls back — DISTINCT from {@link after},
	 * which are durable post-commit hooks that forward to the root when nested.
	 */
	on(event: "commit" | "rollback", cb: AfterHook): this;
	/** One-shot {@link on} (Lucid/Node EventEmitter `once`). */
	once(event: "commit" | "rollback", cb: AfterHook): this;
	/** Remove a listener added via {@link on}/{@link once} (Node EventEmitter `off`). */
	off(event: "commit" | "rollback", cb: AfterHook): this;
	/**
	 * Open a nested transaction (Lucid `const sp = await trx.transaction()`),
	 * implemented as a SAVEPOINT on the same pinned connection. Managed when given
	 * a callback (auto RELEASE / ROLLBACK TO), manual otherwise. Works on all three
	 * dialects — MySQL's SAVEPOINT statements (which can't be prepared) route
	 * through the text protocol in the napi layer.
	 */
	transaction(): Promise<TransactionClient>;
	transaction(options: TransactionOptions): Promise<TransactionClient>;
	transaction<T>(
		callback: (trx: TransactionClient) => Promise<T> | T,
		options?: TransactionOptions,
	): Promise<T>;
	readonly isNested: boolean;
	readonly [TRANSACTION_BRAND]: true;
}

/**
 * Build the overloaded `transaction()` method for a client. Managed when given a
 * callback (auto RELEASE / ROLLBACK TO), manual otherwise — both open a SAVEPOINT
 * on `getParent()`. The impl signature is deliberately broader than the overloads.
 */
export function makeNestedTransactionFn(getParent: () => TransactionClient) {
	function tx(): Promise<TransactionClient>;
	function tx(options: TransactionOptions): Promise<TransactionClient>;
	function tx<T>(
		callback: (trx: TransactionClient) => Promise<T> | T,
		options?: TransactionOptions,
	): Promise<T>;
	function tx(
		arg1?:
			| TransactionOptions
			| ((trx: TransactionClient) => Promise<unknown> | unknown),
		arg2?: TransactionOptions,
	): Promise<unknown> {
		const parent = getParent();
		const callback = typeof arg1 === "function" ? arg1 : undefined;
		const options = typeof arg1 === "function" ? arg2 : arg1;
		return callback
			? runManagedSavepoint(parent, callback, options)
			: openSavepoint(parent);
	}
	return tx;
}

/** Managed nested savepoint: open, run the callback, RELEASE on success / ROLLBACK TO on throw. */
async function runManagedSavepoint<T>(
	parent: TransactionClient,
	callback: (trx: TransactionClient) => Promise<T> | T,
	_options?: TransactionOptions,
): Promise<T> {
	const sp = await openSavepoint(parent);
	try {
		const result = await callback(sp);
		await sp.commit();
		return result;
	} catch (err) {
		try {
			await sp.rollback();
		} catch {
			/* best-effort */
		}
		throw err;
	}
}

/**
 * Open a SAVEPOINT-backed nested transaction client on `parent`. Shared by the
 * standalone {@link transaction} helper, `trx.transaction()`, and the napi pinned
 * client. Commit RELEASEs the savepoint and forwards hooks to the parent (a
 * nested commit isn't durable until the root commits); rollback does ROLLBACK TO
 * + RELEASE and fires the local rollback hooks.
 */
export async function openSavepoint(
	parent: TransactionClient,
): Promise<TransactionClient> {
	const name = `sp_${randomBytes(6).toString("hex")}`;
	await parent.execute(`SAVEPOINT ${name}`, []);
	const commitHooks: AfterHook[] = [];
	const rollbackHooks: AfterHook[] = [];
	const evt = makeTrxEvents();
	// Flipped by whichever of commit/rollback runs first, so a helper handed
	// this client can tell whether it is still usable (Lucid `isCompleted`).
	let completed = false;
	const base = {
		execute: parent.execute.bind(parent),
		query: parent.query.bind(parent),
		get isCompleted(): boolean {
			return completed;
		},
		async commit(): Promise<void> {
			await parent.execute(`RELEASE SAVEPOINT ${name}`, []);
			completed = true;
			evt.emit("commit"); // synchronous EventEmitter notification (this savepoint)
			for (const hook of commitHooks) parent.after("commit", hook);
			for (const hook of rollbackHooks) parent.after("rollback", hook);
		},
		async rollback(): Promise<void> {
			await parent.execute(`ROLLBACK TO SAVEPOINT ${name}`, []);
			completed = true;
			try {
				await parent.execute(`RELEASE SAVEPOINT ${name}`, []);
			} catch {
				/* best-effort — the savepoint is already logically unwound */
			}
			evt.emit("rollback");
			await runAfterHooks(rollbackHooks);
		},
		after(event: "commit" | "rollback", cb: AfterHook): void {
			(event === "commit" ? commitHooks : rollbackHooks).push(cb);
		},
		on: evt.on,
		once: evt.once,
		off: evt.off,
		isNested: true,
		[TRANSACTION_BRAND]: true as const,
	};
	const trx: TransactionClient = Object.assign(
		base,
		makeTransactionQueryBuilders(base, getAtlasDialect()),
		{ transaction: makeNestedTransactionFn(() => trx) },
	);
	return trx;
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

/**
 * The `on`/`once`/`off` EventEmitter surface for a transaction client, plus an
 * internal `emit` the client fires at commit/rollback. Distinct from `after`
 * hooks: these are synchronous, per-client, pre-hook notifications (Lucid — the
 * trx client is a Node EventEmitter). `on`/`once`/`off` return the client for
 * chaining via their `this` binding.
 */
export function makeTrxEvents(): {
	on(
		this: TransactionClient,
		e: "commit" | "rollback",
		cb: AfterHook,
	): TransactionClient;
	once(
		this: TransactionClient,
		e: "commit" | "rollback",
		cb: AfterHook,
	): TransactionClient;
	off(
		this: TransactionClient,
		e: "commit" | "rollback",
		cb: AfterHook,
	): TransactionClient;
	emit(e: "commit" | "rollback"): void;
} {
	const ee = new EventEmitter();
	return {
		on(e, cb) {
			ee.on(e, cb);
			return this;
		},
		once(e, cb) {
			ee.once(e, cb);
			return this;
		},
		off(e, cb) {
			ee.off(e, cb);
			return this;
		},
		emit(e) {
			ee.emit(e);
		},
	};
}

export async function transaction<T>(
	db: DatabaseConnection,
	callback: (trx: TransactionClient) => Promise<T> | T,
	options?: TransactionOptions,
): Promise<T> {
	// Nested (the caller passed a live trx) → a SAVEPOINT-backed managed savepoint.
	if (isTransactionClient(db)) {
		return runManagedSavepoint(db, callback, options);
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
	const evt = makeTrxEvents();

	let completed = false;
	const base = {
		execute: db.execute.bind(db),
		query: db.query.bind(db),
		get isCompleted(): boolean {
			return completed;
		},
		async commit() {
			await db.execute("COMMIT", []);
			completed = true;
			evt.emit("commit"); // synchronous EventEmitter notification
			await runAfterHooks(commitHooks);
		},
		async rollback() {
			await db.execute("ROLLBACK", []);
			completed = true;
			evt.emit("rollback");
			await runAfterHooks(rollbackHooks);
		},
		after(event: "commit" | "rollback", cb: AfterHook) {
			(event === "commit" ? commitHooks : rollbackHooks).push(cb);
		},
		on: evt.on,
		once: evt.once,
		off: evt.off,
		isNested: false,
		[TRANSACTION_BRAND]: true as const,
	};
	const trx: TransactionClient = Object.assign(
		base,
		makeTransactionQueryBuilders(base, getAtlasDialect()),
		{ transaction: makeNestedTransactionFn(() => trx) },
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
