/**
 * NapiDbAdapter — bridges the Rust atlas-db NAPI binding to Atlas.
 */

import type { TransactionClient } from "../Transaction.js";
import { dialectFromUrl } from "../utils/dialectFromUrl.js";
import { TRANSACTION_BRAND } from "../utils/transactionBrand.js";

/** One `(sql, params)` pair passed to `runInTransaction`. */
export interface BatchStatement {
	sql: string;
	params?: unknown[];
}

/** Async database connection backed by Rust (sqlx). */
export interface AsyncDatabaseConnection {
	/** The dialect this connection targets — derived from the URL scheme at connect time. */
	readonly dialect: "sqlite" | "postgres" | "mysql";
	query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
	): Promise<T[]>;
	execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>;
	/**
	 * Run every statement in `batch` atomically inside a single sqlx transaction.
	 * Either every statement commits or none do — used by MigrationRunner to
	 * wrap `up()` / `down()` SQL together with the `ream_migrations` bookkeeping.
	 */
	runInTransaction(batch: readonly BatchStatement[]): Promise<number>;
	/**
	 * Open an INTERACTIVE transaction pinned to one pooled connection. Every
	 * `execute`/`query` on the returned client runs on that same connection, so
	 * a read-then-decide-then-write (gap-free numbering, multi-statement
	 * create/update/delete) is genuinely atomic — unlike pulling BEGIN/COMMIT
	 * through the pool, which scatters statements across connections.
	 */
	begin(): Promise<TransactionClient>;
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
	begin(): Promise<NapiReamTransaction>;
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
): Promise<AsyncDatabaseConnection> {
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

	return {
		dialect,
		async query<T = Record<string, unknown>>(
			sql: string,
			params: unknown[] = [],
		): Promise<T[]> {
			const json = await db.query(sql, JSON.stringify(params));
			return JSON.parse(json) as T[];
		},

		async execute(
			sql: string,
			params: unknown[] = [],
		): Promise<{ rowsAffected: number }> {
			const affected = await db.execute(sql, JSON.stringify(params));
			return { rowsAffected: affected };
		},

		async runInTransaction(batch: readonly BatchStatement[]): Promise<number> {
			// Rust side expects `[[sql, params], ...]`
			const payload = batch.map((s) => [s.sql, s.params ?? []]);
			return db.runInTransaction(JSON.stringify(payload));
		},

		async begin(): Promise<TransactionClient> {
			// `db.begin()` acquires ONE connection and issues BEGIN on it. Every
			// call below routes to that same pinned connection until commit/rollback
			// hands it back to the pool — the whole point of an interactive trx.
			const native = await db.begin();
			return {
				async execute(
					sql: string,
					params: unknown[] = [],
				): Promise<{ rowsAffected: number }> {
					const affected = await native.execute(sql, JSON.stringify(params));
					return { rowsAffected: affected };
				},
				async query<T = Record<string, unknown>>(
					sql: string,
					params: unknown[] = [],
				): Promise<T[]> {
					const json = await native.query(sql, JSON.stringify(params));
					return JSON.parse(json) as T[];
				},
				async commit(): Promise<void> {
					await native.commit();
				},
				async rollback(): Promise<void> {
					await native.rollback();
				},
				isNested: false,
				[TRANSACTION_BRAND]: true,
			};
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
