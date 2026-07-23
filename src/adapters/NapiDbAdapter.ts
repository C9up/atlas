/**
 * NapiDbAdapter — bridges the Rust atlas-db NAPI binding to Atlas.
 */

import { emitDbQuery, hasDbQueryListeners } from "../events.js";
import { makeTransactionQueryBuilders } from "../query/DatabaseQueryBuilder.js";
import {
	type AfterHook,
	runAfterHooks,
	type TransactionClient,
} from "../Transaction.js";
import { dialectFromUrl } from "../utils/dialectFromUrl.js";
import { TRANSACTION_BRAND } from "../utils/transactionBrand.js";

/**
 * JSON replacer for the napi boundary. `BigInt` throws in a plain
 * `JSON.stringify` and a `Uint8Array`/`Buffer` serializes to a useless
 * index-map, so both are wrapped in envelopes the Rust side decodes and binds
 * losslessly: `{"$bigint": "123"}` → i64, `{"$bytes": "<base64>"}` → BLOB/BYTEA.
 */
function napiReplacer(_key: string, value: unknown): unknown {
	if (typeof value === "bigint") return { $bigint: value.toString() };
	if (value instanceof Uint8Array) {
		return { $bytes: Buffer.from(value).toString("base64") };
	}
	return value;
}

/**
 * JSON reviver for napi result sets. Rebuilds `{"$bytes": …}` envelopes (emitted
 * by the Rust decoder for BLOB/BYTEA columns) into a `Uint8Array`. Integers
 * beyond JS's safe range arrive pre-stringified by Rust — no precision loss — so
 * they stay strings, matching the pg/mysql driver convention.
 */
function napiReviver(_key: string, value: unknown): unknown {
	if (
		typeof value === "object" &&
		value !== null &&
		!Array.isArray(value) &&
		"$bytes" in value &&
		Object.keys(value).length === 1
	) {
		const bytes = value.$bytes;
		if (typeof bytes === "string") {
			return Uint8Array.from(Buffer.from(bytes, "base64"));
		}
	}
	return value;
}

/** One `(sql, params)` pair passed to `runInTransaction`. */
export interface BatchStatement {
	sql: string;
	params?: unknown[];
}

/**
 * Transaction isolation level — Lucid-compatible names. Applied via
 * `SET TRANSACTION ISOLATION LEVEL` on Postgres / MySQL; ignored on SQLite
 * (serializable by default, no such statement — same as Lucid).
 */
export type IsolationLevel =
	| "read uncommitted"
	| "read committed"
	| "repeatable read"
	| "serializable";

/** Options for {@link AsyncDatabaseConnection.transaction}. Mirrors Lucid. */
export interface TransactionOptions {
	isolationLevel?: IsolationLevel;
}

/**
 * Context a caller can attach to a statement so the `db:query` event can say
 * where it came from. Optional everywhere — a connection that ignores it stays
 * a valid `AsyncDatabaseConnection`, which is what lets test doubles skip it.
 */
export interface QueryMeta {
	/** Entity class name, when the statement came from a repository/model. */
	model?: string;
	/** The call that produced it (`exec`, `first`, `paginate`, …). */
	method?: string;
	/** True for schema statements. */
	ddl?: boolean;
	/**
	 * Force emission for this statement even when the connection has
	 * `debug: false` — this is what `ModelQuery.debug()` sets.
	 */
	debug?: boolean;
	/**
	 * Arbitrary metadata attached to the `db:query` event (Adonis Lucid
	 * `reporterData`) — request id, user id, feature flag, … — for listeners.
	 */
	reporterData?: Record<string, unknown>;
}

/** Per-connection observability settings (Lucid's `debug` connection option). */
export interface ObservabilityOptions {
	/**
	 * Emit a `db:query` event for every statement on this connection. Off by
	 * default: it costs a timing pair per query, and nothing is emitted anyway
	 * unless something subscribed via `onDbQuery`.
	 */
	debug?: boolean;
	/** Connection name, reported on each event so multi-connection apps can tell them apart. */
	connectionName?: string;
}

/** Async database connection backed by Rust (sqlx). */
export interface AsyncDatabaseConnection {
	/** The dialect this connection targets — derived from the URL scheme at connect time. */
	readonly dialect: "sqlite" | "postgres" | "mysql";
	query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<T[]>;
	execute(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<{ rowsAffected: number }>;
	/**
	 * Run every statement in `batch` atomically inside a single sqlx transaction.
	 * Either every statement commits or none do — used by MigrationRunner to
	 * wrap `up()` / `down()` SQL together with the `ream_migrations` bookkeeping.
	 */
	runInTransaction(batch: readonly BatchStatement[]): Promise<number>;
	/**
	 * Open an INTERACTIVE transaction pinned to one pooled connection — Lucid's
	 * `db.transaction`. Manual when called with no callback (you drive
	 * `commit()`/`rollback()`), managed when given one (auto commit on success,
	 * rollback on throw). Every `execute`/`query` on the trx runs on that same
	 * connection, so a read-then-decide-then-write (gap-free numbering,
	 * multi-statement create/update/delete) is genuinely atomic — unlike pulling
	 * BEGIN/COMMIT through the pool, which scatters statements across connections.
	 *
	 * Optional — mirrors `DatabaseConnection.transaction?`. Real napi connections
	 * (`createNapiConnection`) always provide it; the standalone `transaction()`
	 * helper guards on `typeof db.transaction === "function"` at runtime, so the
	 * member is optional by design. Keeping it optional lets test doubles satisfy
	 * the interface without forging a `TransactionClient` (whose brand symbol is
	 * intentionally not exported, so it can't be constructed outside the package).
	 */
	transaction?(): Promise<TransactionClient>;
	transaction?(options: TransactionOptions): Promise<TransactionClient>;
	transaction?<T>(
		callback: (trx: TransactionClient) => Promise<T> | T,
		options?: TransactionOptions,
	): Promise<T>;
	close(): Promise<void>;
	ping(): Promise<void>;
}

/** Shape of the NAPI ReamTransaction handle returned by `ReamDatabase.begin()`. */
interface NapiReamTransaction {
	query(sql: string, paramsJson: string): Promise<string>;
	execute(sql: string, paramsJson: string): Promise<number>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
}

/** Shape of the NAPI ReamDatabase class. */
interface NapiReamDatabase {
	query(sql: string, paramsJson: string): Promise<string>;
	execute(sql: string, paramsJson: string): Promise<number>;
	runInTransaction(batchJson: string): Promise<number>;
	begin(isolationLevel?: string): Promise<NapiReamTransaction>;
	close(): Promise<void>;
	ping(): Promise<void>;
	poolSize(): number;
}

interface NapiModule {
	ReamDatabase: {
		connect(
			url: string,
			min: number,
			max: number,
			pragmas?: Array<[string, string]>,
			connectRetries?: number,
			connectBackoffMs?: number,
			connectTimeoutMs?: number,
		): Promise<NapiReamDatabase>;
	};
}

/** Connection retry / timeout knobs (see {@link createNapiConnection}). */
export interface ConnectRetryOptions {
	/** Extra attempts if the initial connect fails (default 0 — single attempt). */
	retries?: number;
	/** Base backoff in ms between attempts; grows exponentially, capped at 30s (default 200). */
	backoffMs?: number;
	/**
	 * Per-attempt acquire timeout in ms (sqlx `acquire_timeout`). sqlx already
	 * retries connection establishment internally up to this window (~30s by
	 * default), so lower it to make each retry give up faster.
	 */
	timeoutMs?: number;
}

/**
 * Connect to a database via the Rust NAPI driver.
 *
 *   const db = await createNapiConnection('sqlite:data/app.db')
 *   const db = await createNapiConnection('postgres://user:pass@host/db')
 *   const db = await createNapiConnection('mysql://user:pass@host/db')
 */
export async function createNapiConnection(
	url: string,
	poolMin = 1,
	poolMax = 10,
	pragmas?: Record<string, string | number>,
	retry?: ConnectRetryOptions,
	observability: ObservabilityOptions = {},
): Promise<AsyncDatabaseConnection> {
	const { debug = false, connectionName } = observability;
	// Throws with the underlying cause if the binary can't be loaded.
	const native = await loadNativeDb();

	// Validate sqlite pragmas before crossing the NAPI boundary.
	// PRAGMA syntax doesn't take bound parameters — the Rust side will
	// interpolate the (key, value) pair into the statement, so the
	// alphabet is locked to `[A-Za-z0-9_]+` on both sides. Failing
	// here gives a clearer error than a Rust panic in `after_connect`.
	const dialect = dialectFromUrl(url);
	if (dialect === "sqlite" && pragmas) {
		const safe = /^[A-Za-z0-9_]+$/;
		for (const [key, value] of Object.entries(pragmas)) {
			if (!safe.test(key)) {
				throw new Error(
					`[ATLAS] Invalid sqlite pragma key: ${JSON.stringify(key)}`,
				);
			}
			if (!safe.test(String(value))) {
				throw new Error(
					`[ATLAS] Invalid sqlite pragma value for '${key}': ${JSON.stringify(value)}`,
				);
			}
		}
	}
	// Pragmas are pushed THROUGH the NAPI boundary so the Rust side can
	// wire them into `SqliteConnectOptions::pragma()`. That's the only
	// path that guarantees every connection sqlx opens for the pool
	// starts in the requested journal_mode / synchronous state — running
	// PRAGMA from the query path can silently no-op when the first
	// pooled connection already claimed the journal in another mode.
	const pragmaList: Array<[string, string]> | undefined =
		dialect === "sqlite" && pragmas
			? Object.entries(pragmas).map(
					([k, v]) => [k, String(v)] as [string, string],
				)
			: undefined;

	const db = await native.ReamDatabase.connect(
		url,
		poolMin,
		poolMax,
		pragmaList,
		retry?.retries,
		retry?.backoffMs,
		retry?.timeoutMs,
	);

	// Build a TransactionClient bound to a freshly-acquired, pinned connection.
	// `db.begin()` acquires ONE connection and issues BEGIN on it; every call
	// here routes to that same connection until commit/rollback hands it back.
	async function openPinned(
		isolationLevel?: IsolationLevel,
	): Promise<TransactionClient> {
		const native = await db.begin(isolationLevel);
		// Root (non-nested) transaction: after-hooks fire once the underlying
		// COMMIT / ROLLBACK is durable (Lucid `trx.after(...)`), errors swallowed.
		const commitHooks: AfterHook[] = [];
		const rollbackHooks: AfterHook[] = [];
		const base = {
			async execute(
				sql: string,
				params: unknown[] = [],
			): Promise<{ rowsAffected: number }> {
				const affected = await native.execute(
					sql,
					JSON.stringify(params, napiReplacer),
				);
				return { rowsAffected: affected };
			},
			async query<T = Record<string, unknown>>(
				sql: string,
				params: unknown[] = [],
			): Promise<T[]> {
				const json = await native.query(
					sql,
					JSON.stringify(params, napiReplacer),
				);
				return JSON.parse(json, napiReviver) as T[];
			},
			async commit(): Promise<void> {
				await native.commit();
				await runAfterHooks(commitHooks);
			},
			async rollback(): Promise<void> {
				await native.rollback();
				await runAfterHooks(rollbackHooks);
			},
			after(event: "commit" | "rollback", cb: AfterHook): void {
				(event === "commit" ? commitHooks : rollbackHooks).push(cb);
			},
			isNested: false,
			[TRANSACTION_BRAND]: true as const,
		};
		// Lucid: the transaction client is also a query-builder entry point
		// (trx.table()/from()/insertQuery()), routed through THIS pinned connection.
		return Object.assign(base, makeTransactionQueryBuilders(base, dialect));
	}

	// Lucid-compatible `transaction`: managed when given a callback (auto
	// commit / rollback), manual when not (caller drives commit/rollback).
	function transaction(): Promise<TransactionClient>;
	function transaction(options: TransactionOptions): Promise<TransactionClient>;
	function transaction<T>(
		callback: (trx: TransactionClient) => Promise<T> | T,
		options?: TransactionOptions,
	): Promise<T>;
	async function transaction<T>(
		arg1?: TransactionOptions | ((trx: TransactionClient) => Promise<T> | T),
		arg2?: TransactionOptions,
	): Promise<TransactionClient | T> {
		const callback = typeof arg1 === "function" ? arg1 : undefined;
		const options = typeof arg1 === "function" ? arg2 : arg1;
		const trx = await openPinned(options?.isolationLevel);
		if (!callback) return trx;
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

	/**
	 * Run `fn`, emitting a `db:query` event around it when observation is on.
	 *
	 * The fast path is a single boolean pair: with no listeners, or with debug
	 * off and no per-query override, this adds nothing but the check. The event
	 * is emitted on failure too — a slow query that then throws is exactly the
	 * one worth seeing.
	 */
	async function observed<T>(
		sql: string,
		params: unknown[],
		meta: QueryMeta | undefined,
		fn: () => Promise<T>,
	): Promise<T> {
		if (!(debug || meta?.debug) || !hasDbQueryListeners()) return fn();

		const startedAt = performance.now();
		try {
			const result = await fn();
			emitDbQuery({
				sql,
				bindings: params,
				duration: performance.now() - startedAt,
				connection: connectionName,
				model: meta?.model,
				method: meta?.method,
				ddl: meta?.ddl,
				inTransaction: false,
				reporterData: meta?.reporterData,
			});
			return result;
		} catch (error) {
			emitDbQuery({
				sql,
				bindings: params,
				duration: performance.now() - startedAt,
				connection: connectionName,
				model: meta?.model,
				method: meta?.method,
				ddl: meta?.ddl,
				inTransaction: false,
				error: error instanceof Error ? error : new Error(String(error)),
				reporterData: meta?.reporterData,
			});
			throw error;
		}
	}

	return {
		dialect,
		transaction,
		async query<T = Record<string, unknown>>(
			sql: string,
			params: unknown[] = [],
			meta?: QueryMeta,
		): Promise<T[]> {
			return observed(sql, params, meta, async () => {
				const json = await db.query(sql, JSON.stringify(params, napiReplacer));
				return JSON.parse(json, napiReviver) as T[];
			});
		},

		async execute(
			sql: string,
			params: unknown[] = [],
			meta?: QueryMeta,
		): Promise<{ rowsAffected: number }> {
			return observed(sql, params, meta, async () => {
				const affected = await db.execute(
					sql,
					JSON.stringify(params, napiReplacer),
				);
				return { rowsAffected: affected };
			});
		},

		async runInTransaction(batch: readonly BatchStatement[]): Promise<number> {
			// Rust side expects `[[sql, params], ...]`
			const payload = batch.map((s) => [s.sql, s.params ?? []]);
			return db.runInTransaction(JSON.stringify(payload, napiReplacer));
		},

		async close(): Promise<void> {
			await db.close();
		},

		async ping(): Promise<void> {
			await db.ping();
		},
	};
}

// dialectFromUrl moved to ../utils/dialectFromUrl.ts — single source of truth
// shared with AtlasProvider.

/** Load the native DB binding from the prebuilt `.node` binary in the package root. */
async function loadNativeDb(): Promise<NapiModule> {
	const platform = process.platform;
	const arch = process.arch;
	// Same naming convention as napi-rs / src/query/native.ts: the build emits
	// `db.win32-x64-msvc.node` and `db.linux-x64-gnu.node`, so win32 needs the
	// `-msvc` ABI tag and linux the `-gnu` one — a bare `win32-x64` misses the file.
	const suffix =
		platform === "linux"
			? `${platform}-${arch}-gnu`
			: platform === "win32"
				? `${platform}-${arch}-msvc`
				: `${platform}-${arch}`;
	const binaryName = `db.${suffix}.node`;

	try {
		const { createRequire } = await import("node:module");
		const { fileURLToPath } = await import("node:url");
		const { dirname, join } = await import("node:path");
		const require = createRequire(import.meta.url);
		const here = dirname(fileURLToPath(import.meta.url));
		// The binary lives at the package root (../../db.<suffix>.node from src/adapters/)
		const binaryPath = join(here, "..", "..", binaryName);
		return require(binaryPath) as NapiModule;
	} catch (err) {
		// Surface the real cause (missing file, ABI mismatch, dlopen error) — a
		// bare `return null` previously erased it and left the caller throwing a
		// generic "not available" message that was impossible to debug.
		throw new Error(
			`[ATLAS] Failed to load Rust DB driver '${binaryName}' for ${platform}-${arch}. ` +
				"Build with: cargo build --release",
			{ cause: err },
		);
	}
}
