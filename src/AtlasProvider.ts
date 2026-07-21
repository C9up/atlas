/**
 * AtlasProvider — Ream provider for the Atlas ORM.
 *
 * Connects to one or more databases via the Rust ream-db driver
 * (SQLite/PostgreSQL/MySQL). Runs migrations on boot for the default
 * connection.
 *
 * Multi-connection support (story 32.9):
 *
 *     // config/database.ts
 *     export default {
 *       default: 'primary',
 *       connections: {
 *         primary: { url: 'postgres://.../primary' },
 *         tenant1: { url: 'postgres://.../tenant1' },
 *       },
 *     }
 *
 *     // Code
 *     const primary = app.container.resolve('db')              // default
 *     const tenant1 = app.container.resolve('db:tenant1')     // named
 */

import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "./adapters/NapiDbAdapter.js";
import { clearCastRegistry, setAtlasDialect } from "./query/native.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "./schema/MigrationRunner.js";
import { dialectFromUrl } from "./utils/dialectFromUrl.js";

/**
 * Structural slice of the host framework's app context — only the surface
 * AtlasProvider actually uses. Declared locally so atlas does NOT import
 * `@c9up/ream`; any framework whose context exposes a `config.get(key)`
 * reader and a `container.singleton(token, factory)` binder satisfies this
 * contract via TypeScript structural compatibility.
 *
 * The `token` type mirrors ream's `ServiceToken` union (`string | symbol |
 * ctor`) so AtlasProvider can grow into class-as-token / Symbol-keyed
 * bindings without re-coupling to `@c9up/ream`. AtlasProvider itself only
 * uses `string` tokens today.
 */
export interface AtlasAppContext {
	container: {
		singleton(
			token: string | symbol | (new (...args: never[]) => unknown),
			factory: () => unknown,
		): void;
	};
	config: { get<T = unknown>(key: string): T | undefined };
}

/**
 * Canonical sqlite production pragma recipe.
 *
 *   journal_mode = WAL        // writes go to a side-log, readers don't block
 *   synchronous  = NORMAL     // one fsync per commit (on the WAL); recovery
 *                             // still rebuilds a consistent DB after crash
 *
 * Drops INSERT latency by ~5–10x vs sqlite's default
 * (`journal_mode=delete` + `synchronous=FULL`) without sacrificing
 * durability. Spread the constant into `pragmas` so app-specific overrides
 * stay literal:
 *
 *   pragmas: { ...SQLITE_PROD_PRAGMAS, foreign_keys: "ON" }
 */
export const SQLITE_PROD_PRAGMAS = Object.freeze({
	journal_mode: "WAL",
	synchronous: "NORMAL",
} as const);

/** One connection's settings. */
export interface ConnectionConfig {
	/** Connection URL: "sqlite:data/app.db", "postgres://...", "mysql://..." */
	url: string;
	/** Minimum pool connections (default: 1) */
	poolMin?: number;
	/** Maximum pool connections (default: 10) */
	poolMax?: number;
	/**
	 * Emit a `db:query` event for every statement on this connection (Lucid's
	 * `debug` connection option). Off by default — subscribe with `onDbQuery`
	 * to receive them; with no subscriber nothing is emitted either way.
	 */
	debug?: boolean;
	/**
	 * Connection-level pragmas (sqlite only). Each entry becomes a
	 * `PRAGMA <key> = <value>;` issued before the first query.
	 *
	 * Most apps want `{ journal_mode: "WAL", synchronous: "NORMAL" }` —
	 * disk-backed, durable, and ~5–10x faster than the default
	 * `journal_mode=delete` + `synchronous=FULL` (two fsyncs per
	 * commit). Ignored for postgres / mysql URLs.
	 */
	pragmas?: Record<string, string | number>;
	/**
	 * Retry the INITIAL connection if it fails — useful when the DB starts a
	 * moment after the app (docker-compose / k8s) or for a transient boot blip.
	 * Extra attempts beyond the first (default 0 — single attempt).
	 */
	connectRetries?: number;
	/** Base backoff in ms between connect attempts (exponential, capped 30s; default 200). */
	connectBackoffMs?: number;
	/**
	 * Per-attempt acquire timeout in ms. sqlx already retries connection
	 * establishment internally up to this window (~30s default), so lower it to
	 * make each `connectRetries` attempt give up faster (e.g. 5 × 2s).
	 */
	connectTimeoutMs?: number;
}

/** Full database config — single-connection (legacy) OR multi-connection. */
export interface AtlasDatabaseConfig extends ConnectionConfig {
	/** Name of the default connection when `connections` is set. Defaults to `"primary"`. */
	default?: string;
	/** Named connections. When present, top-level `url` is treated as `connections[default].url`. */
	connections?: Record<string, ConnectionConfig>;
	migrations?: {
		path?: string;
		/**
		 * Custom name for the migrations tracking table. Defaults to `"ream_migrations"`.
		 * Must match `/^[A-Za-z_][A-Za-z0-9_]*$/` — the `MigrationRunner` constructor
		 * throws `AtlasError("MIGRATION_INVALID_TABLE_NAME")` otherwise.
		 */
		table?: string;
		/**
		 * Allow the boot-time auto-migrate in production. OFF by default:
		 * starting the app should NOT silently mutate the schema in prod (Adonis
		 * Lucid runs migrations only via the explicit `migration:run` command).
		 * In non-production, boot auto-migrate stays on for dev convenience. The
		 * CLI's `REAM_SKIP_BOOT_MIGRATE=1` always wins.
		 */
		autoRunInProduction?: boolean;
	};
	/**
	 * Boot-time schema verification. When set, atlas reconciles each listed
	 * model's `@Column` metadata against the LIVE database schema after boot and
	 * reports drift (typo'd / missing columns, type mismatches, unmapped NOT NULL
	 * columns). `mode: "throw"` (default) fails the boot on drift; `"warn"` logs
	 * and continues. Atlas has no global entity registry by design — list your
	 * models here (mirrors Lucid, where `ace` is pointed at your models dir).
	 */
	verifySchema?: {
		entities: ReadonlyArray<new (...args: unknown[]) => unknown>;
		mode?: "throw" | "warn";
	};
}

export default class AtlasProvider {
	/** Map of connection name → open connection. Populated at boot. */
	#connections = new Map<string, AsyncDatabaseConnection>();
	#defaultName = "primary";

	constructor(protected app: AtlasAppContext) {}

	/** Resolve a live connection by name, or throw — never hand out a closed one
	 * (used by the `db`/`db:<name>` container factories so a failed-boot or
	 * post-shutdown lookup fails loudly instead of returning a dead handle). */
	#requireConnection(name: string): AsyncDatabaseConnection {
		const conn = this.#connections.get(name);
		if (!conn) {
			throw new Error(
				`AtlasProvider: connection '${name}' is not available (boot failed or the provider was shut down).`,
			);
		}
		return conn;
	}

	register() {}

	async boot() {
		const config = this.app.config.get<AtlasDatabaseConfig>("database");
		if (!config) return;

		// Normalize: if `connections` is not set, build a single-entry map from top-level config.
		const { connections, defaultName } = this.#resolveConnections(config);
		this.#defaultName = defaultName;

		// Import the services/db proxy BEFORE opening any pool: if this import ever
		// failed (packaging/bundle issue), we must not have leaked open connections,
		// and the cleanup catch below needs it available.
		const dbServices = await import("./services/db.js");

		// Open every connection in parallel — multi-database apps with slow-to-
		// handshake drivers (Postgres over TLS, RDS proxies) previously paid the
		// sum of the round-trip times on boot; now it's the max.
		//
		// `Promise.allSettled` lets us distinguish successes from failures without
		// losing the already-opened connections. If any connection rejected, we
		// close every successful one before rethrowing so a partial boot never
		// leaks pools/sockets.
		const entries = Object.entries(connections);
		const results = await Promise.allSettled(
			entries.map(([name, settings]) =>
				createNapiConnection(
					settings.url,
					settings.poolMin ?? 1,
					settings.poolMax ?? 10,
					settings.pragmas,
					{
						retries: settings.connectRetries,
						backoffMs: settings.connectBackoffMs,
						timeoutMs: settings.connectTimeoutMs,
					},
					{ debug: settings.debug ?? false, connectionName: name },
				),
			),
		);
		const failures: Array<{ name: string; error: unknown }> = [];
		const successes: Array<{ name: string; conn: AsyncDatabaseConnection }> =
			[];
		results.forEach((r, i) => {
			const [name] = entries[i];
			if (r.status === "fulfilled") successes.push({ name, conn: r.value });
			else failures.push({ name, error: r.reason });
		});
		if (failures.length > 0) {
			// Tear down the successes so we don't leak any pool that the runtime
			// has already opened. Closures run in parallel with allSettled so a
			// stuck close doesn't block the rollback path.
			await Promise.allSettled(successes.map((s) => s.conn.close()));
			const first = failures[0];
			const others = failures
				.slice(1)
				.map((f) => `${f.name}: ${String(f.error)}`)
				.join("; ");
			throw new Error(
				`AtlasProvider: failed to open ${failures.length} connection(s) — ` +
					`'${first.name}' failed: ${String(first.error)}` +
					(others ? ` (also: ${others})` : ""),
			);
		}
		// Everything past this point runs AFTER the pools are open — an invalid
		// default connection, a boot-time migration crash, etc. must not leak the
		// sockets/pools we just opened. On any failure, tear them all down and
		// undo the registry/service bindings before rethrowing.
		try {
			// Validate the default connection opened BEFORE binding anything: a bad
			// default must fail while the container/registry are still untouched, so
			// we never leave handles pointing at connections we're about to close.
			const defaultEntry = successes.find((s) => s.name === defaultName);
			if (!defaultEntry) {
				throw new Error(
					`AtlasProvider: default connection '${defaultName}' is not defined in config.database.connections`,
				);
			}
			const defaultConn = defaultEntry.conn;

			// Bind connections into the container AND the named-connection registry
			// (the latter is what `BaseModel.connection` / `Factory.connection()` /
			// `getConnection(name)` read). The container factories resolve through
			// `#connections` (not a captured handle) so that if boot fails after
			// binding — the catch clears `#connections` — or after shutdown, resolving
			// `db`/`db:<name>` throws instead of handing out a CLOSED connection.
			for (const { name, conn } of successes) {
				this.#connections.set(name, conn);
				dbServices.registerConnection(name, conn);
				this.app.container.singleton(`db:${name}`, () =>
					this.#requireConnection(name),
				);
			}

			// Expose the default under the short aliases `db` and `db.connection`.
			this.app.container.singleton("db", () =>
				this.#requireConnection(this.#defaultName),
			);
			this.app.container.singleton("db.connection", () =>
				this.#requireConnection(this.#defaultName),
			);

			// Populate the `@c9up/atlas/services/db` proxy so apps can
			// `import db from '@c9up/atlas/services/db'` from anywhere.
			dbServices.setDb(defaultConn);

			// The dialect set module-wide is the DEFAULT connection's dialect.
			// Per-connection dialect (when a user hits a non-default) is read from
			// the connection URL at query time by each call site that cares.
			setAtlasDialect(dialectFromUrl(connections[defaultName]?.url));

			// Auto-run migrations on boot — but NOT in production unless explicitly
			// opted in: starting the app should not silently mutate the schema in
			// prod (Adonis Lucid only migrates via `migration:run`). Skipped too
			// when a CLI migration command booted us (`REAM_SKIP_BOOT_MIGRATE=1`),
			// which drives migrations explicitly.
			const inProduction = process.env.NODE_ENV === "production";
			const autoMigrateAllowed =
				!inProduction || config.migrations?.autoRunInProduction === true;
			if (
				config.migrations?.path &&
				process.env.REAM_SKIP_BOOT_MIGRATE !== "1" &&
				autoMigrateAllowed
			) {
				await this.#runMigrations(
					config.migrations.path,
					connections[defaultName]?.url,
					defaultConn,
					config.migrations.table,
				);
			}
		} catch (err) {
			await Promise.allSettled(successes.map((s) => s.conn.close()));
			for (const { name, conn } of successes) {
				dbServices.clearDb(conn);
				dbServices.unregisterConnection(name, conn);
			}
			this.#connections.clear();
			throw err;
		}
	}

	async shutdown() {
		// Close every connection in parallel — same reasoning as boot. We use
		// `allSettled` so a single driver failing to close doesn't prevent the
		// rest from shutting down, BUT we surface the failures afterwards:
		//
		//   - the map is cleared unconditionally (the process is shutting down
		//     and we don't want to hand out closed handles)
		//   - any rejection is aggregated into a single `AggregateError` thrown
		//     at the end so supervisors / health-checks see a non-zero exit
		//     signal instead of a silent "everything is fine" shutdown
		const named = [...this.#connections.entries()];
		const results = await Promise.allSettled(named.map(([, c]) => c.close()));
		this.#connections.clear();

		// Release the module-level singletons this provider populated at boot so a
		// re-boot starts clean and `db.*` can't dereference a now-closed handle.
		// `clearDb` is ownership-guarded, so clearing every connection only unbinds
		// the one still owning the singleton. Also drop the named-connection
		// registry entries (boot registered them), so `getConnection(name)` /
		// `BaseModel.connection` / `Factory.connection()` can't hand out a CLOSED
		// handle after shutdown.
		const { clearDb, unregisterConnection } = await import("./services/db.js");
		for (const [name, conn] of named) {
			clearDb(conn);
			unregisterConnection(name, conn);
		}
		clearCastRegistry();
		const errors = results
			.map((r, i) =>
				r.status === "rejected" ? { name: named[i][0], error: r.reason } : null,
			)
			.filter((x): x is { name: string; error: unknown } => x !== null);
		if (errors.length > 0) {
			const summary = errors
				.map((e) => `'${e.name}': ${String(e.error)}`)
				.join("; ");
			throw new AggregateError(
				errors.map((e) => e.error),
				`AtlasProvider: ${errors.length} connection(s) failed to close — ${summary}`,
			);
		}
	}

	async start() {
		// Boot-time schema verification (opt-in via `database.verifySchema`).
		// Runs AFTER boot so the default connection is open. Reconciles models
		// against the live DB and throws/warns on drift before requests serve.
		const config = this.app.config.get<AtlasDatabaseConfig>("database");
		const verify = config?.verifySchema;
		if (!verify || verify.entities.length === 0) return;
		const db = this.#connections.get(this.#defaultName);
		if (!db) return;
		const [{ verifySchema }, { getAtlasDialect }] = await Promise.all([
			import("./schema/SchemaCheck.js"),
			import("./query/native.js"),
		]);
		await verifySchema(verify.entities, db, getAtlasDialect(), {
			mode: verify.mode,
		});
	}
	async ready() {}

	/** Normalize the config into a `{ name → ConnectionConfig }` map + default name. */
	#resolveConnections(config: AtlasDatabaseConfig): {
		connections: Record<string, ConnectionConfig>;
		defaultName: string;
	} {
		if (config.connections && Object.keys(config.connections).length > 0) {
			return {
				connections: config.connections,
				defaultName: config.default ?? "primary",
			};
		}
		// Legacy single-connection shape — promote to multi-connection under
		// "primary". Carry EVERY top-level option through: dropping debug /
		// connectRetries / connectBackoffMs / connectTimeoutMs here silently
		// disabled retry/timeout/query-debug for anyone still on the flat config.
		return {
			connections: {
				primary: {
					url: config.url,
					poolMin: config.poolMin,
					poolMax: config.poolMax,
					pragmas: config.pragmas,
					debug: config.debug,
					connectRetries: config.connectRetries,
					connectBackoffMs: config.connectBackoffMs,
					connectTimeoutMs: config.connectTimeoutMs,
				},
			},
			defaultName: "primary",
		};
	}

	async #runMigrations(
		migrationsPath: string,
		url: string,
		db: AsyncDatabaseConnection,
		tableName: string | undefined,
	): Promise<void> {
		const { existsSync } = await import("node:fs");
		if (!existsSync(migrationsPath)) return;

		const adapter: DatabaseAdapter = {
			execute: async (sql, params) => {
				await db.execute(sql, params);
			},
			query: <T>(sql: string, params?: unknown[]) =>
				db.query(sql, params) as Promise<T[]>,
			close: () => db.close(),
			// Thread the transactional path through so MigrationRunner takes the
			// atomic branch (a mid-migration failure rolls back both the SQL and the
			// `ream_migrations` bookkeeping row together). Without this, the runner
			// silently falls back to non-transactional execution.
			runInTransaction: async (batch) => db.runInTransaction(batch),
			// Managed interactive transaction, so this.defer() migrations run
			// atomically (schema + deferred + tracking committed together).
			transaction: db.transaction?.bind(db),
		};
		const runner = new MigrationRunner(adapter, {
			migrationsDir: migrationsPath,
			dialect: dialectFromUrl(url),
			tableName,
		});
		await runner.migrate();
	}
}
