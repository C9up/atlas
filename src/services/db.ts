/**
 * Default `db` singleton — Adonis Lucid–style ergonomic access to the
 * configured database connection.
 *
 *   import db from '@c9up/atlas/services/db'
 *
 *   const rows = await db.query('SELECT * FROM users WHERE id = ?', [id])
 *
 * Populated by `AtlasProvider.boot()`. The instance is whatever the
 * `database.connections[default]` config block resolves to (typically
 * a `NapiDbAdapter` wrapping the Rust sqlite driver, but apps can
 * swap in a custom `AsyncDatabaseConnection` through the provider's
 * container hooks).
 */

import type { ConnectionConfig } from "../AtlasProvider.js";
import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import { ConnectionManager } from "../ConnectionManager.js";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../query/DatabaseQueryBuilder.js";
import type { AtlasDialect } from "../query/native.js";
import { RawSql } from "../query/QueryBuilder.js";
import {
	RawQueryBuilder,
	resolveRawBindings,
} from "../query/RawQueryBuilder.js";
import { truncateAll } from "../testing/DatabaseCleanup.js";

/** Options accepted by the Lucid query-builder entry points. */
export interface DbQueryOptions {
	/** Route the query through this transaction client (Lucid `{ client: trx }`). */
	client?: QueryExecutor;
	/** `'read'` rejects writes on this builder (Lucid `db.query({ mode: 'read' })`). */
	mode?: "read" | "write";
}

/** Options for {@link DbService.connection} (Lucid read/write replica routing). */
export interface ConnectionOptions {
	/**
	 * `'read'` scopes the returned service to reads: its query builders reject
	 * writes (insert/update/delete/increment/decrement). `'write'` (default) is
	 * unrestricted. Atlas has no replica pool, so `mode` is a write-guard rather
	 * than a routing hint — the guard is the security-relevant half of Lucid's
	 * read/write modes.
	 */
	mode?: "read" | "write";
}

/**
 * The `db` service surface — Adonis Lucid's `Database` service. Exposes the
 * query builders (`query`/`from`/`table`/`insertQuery`), raw execution
 * (`rawQuery`), the `raw()` fragment builder, connection scoping (`connection`),
 * and the transaction/DDL methods forwarded from the bound connection.
 */
export interface DbService {
	/** A connection-level query builder (Lucid `db.query()`), optionally on a trx. */
	query(options?: DbQueryOptions): DatabaseQueryBuilder;
	/** Query builder with the table pre-selected (Lucid `db.from(table)`). */
	from(table: string): DatabaseQueryBuilder;
	/**
	 * Query builder on a derived-table source (Lucid `db.from(subquery, alias)`) —
	 * a builder OR a callback that builds one.
	 */
	from(
		subquery: DatabaseQueryBuilder | ((query: DatabaseQueryBuilder) => void),
		alias?: string,
	): DatabaseQueryBuilder;
	/** Insert/write builder with the table pre-selected (Lucid `db.table(table)`). */
	table(table: string): DatabaseQueryBuilder;
	/** An insert builder (Lucid `db.insertQuery()`), optionally on a trx. */
	insertQuery(options?: DbQueryOptions): DatabaseQueryBuilder;
	/**
	 * A chainable raw query (Lucid `db.rawQuery(sql, bindings)`). Thenable — can be
	 * awaited directly — and exposes `toSQL`/`toQuery`/`debug`/`timeout`/
	 * `reporterData`. Bindings may be positional (`?`/`??`) or named (`:name`/`:name:`).
	 */
	rawQuery<T = Record<string, unknown>>(
		sql: string,
		bindings?: unknown[] | Record<string, unknown>,
	): RawQueryBuilder<T>;
	/**
	 * Scope the service to a connection (Lucid `db.connection(name)`). Called with
	 * no name it returns the default connection's service (Lucid `db.connection()`).
	 * Pass `{ mode: 'read' }` to reject writes on the returned service (Lucid
	 * `db.connection(name, { mode: 'read' })`).
	 */
	connection(name?: string, options?: ConnectionOptions): DbService;
	/**
	 * Query builder for a model whose class is resolved at runtime (Lucid
	 * `db.modelQuery(Model)`). For static code prefer `Model.query()` directly.
	 */
	modelQuery<Q>(model: { query(): Q }): Q;
	/**
	 * Build a raw SQL expression — AdonisJS `db.raw()`. For query fragments and
	 * column defaults that are SQL expressions:
	 *
	 *   t.uuid('id').defaultTo(db.raw('gen_random_uuid()'))
	 *
	 * Bindings may be positional (`?`/`??`) or named (`:name`/`:name:`).
	 */
	raw(sql: string, params?: unknown[] | Record<string, unknown>): RawSql;
	/**
	 * A column reference — Adonis Lucid `db.ref('posts.created_at')`. Use it where
	 * a value position must be read as a column (e.g. `orderBy(db.ref(col), 'desc')`).
	 * The identifier is validated and dialect-quoted; it is NOT a value binding.
	 */
	ref(column: string): RawSql;
	/** Run a statement for effect (forwarded to the connection). */
	execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>;
	/** Managed/manual interactive transaction (forwarded, Lucid `db.transaction`). */
	transaction: AsyncDatabaseConnection["transaction"];
	/** Atomic batch (forwarded). */
	runInTransaction: AsyncDatabaseConnection["runInTransaction"];
	/**
	 * Empty a table (Lucid `truncate`). `TRUNCATE TABLE` on Postgres/MySQL (with
	 * `CASCADE` when `cascade` is set, Postgres only); `DELETE FROM` on SQLite,
	 * which has no `TRUNCATE`.
	 */
	truncate(table: string, cascade?: boolean): Promise<void>;
	/**
	 * Empty every user table (Lucid `truncateAllTables`). Framework tables
	 * (`ream_*`) and dialect internals are left alone; pass `ignoreTables` to spare
	 * more. Foreign keys are suspended so delete order doesn't matter.
	 */
	truncateAllTables(ignoreTables?: readonly string[]): Promise<void>;
	/**
	 * Try to acquire a session-level advisory lock, non-blocking (Lucid
	 * `getAdvisoryLock`). Postgres `pg_try_advisory_lock`, MySQL `GET_LOCK(key, 0)`.
	 * A string key is hashed to the integer Postgres requires. Returns whether the
	 * lock was acquired. **Throws on SQLite** (no advisory locks — Lucid parity).
	 */
	getAdvisoryLock(key: string | number): Promise<boolean>;
	/** Release an advisory lock taken with {@link getAdvisoryLock}. Throws on SQLite. */
	releaseAdvisoryLock(key: string | number): Promise<boolean>;
	/** The full Lucid connection manager (`add`/`connect`/`patch`/`release`/nodes/events). */
	readonly manager: ConnectionManager;
	/** The bound connection's dialect. */
	readonly dialect: AtlasDialect;
	ping(): Promise<void>;
	close(): Promise<void>;
}

let instance: AsyncDatabaseConnection | undefined;

/** @internal Bind the singleton (called by AtlasProvider). */
export function setDb(connection: AsyncDatabaseConnection): void {
	instance = connection;
}

/**
 * @internal Unbind the singleton IF it still points at `connection` (called by
 * `AtlasProvider.shutdown()`). Ownership-guarded: when a second provider rebound
 * the singleton, the older provider's shutdown must not clear the newer binding.
 * Without this, `db.*` after shutdown would dereference a closed connection.
 */
export function clearDb(connection: AsyncDatabaseConnection): void {
	if (instance === connection) instance = undefined;
}

/** @internal Read the singleton (or `undefined` pre-boot). */
export function getDb(): AsyncDatabaseConnection | undefined {
	return instance;
}

// The shared connection manager (Lucid `db.manager`) — the single owner of named
// connections. Backs `BaseModel.connection = 'analytics'` so a model resolves a
// non-default connection from a plain import (AdonisJS `static connection`).
const manager = new ConnectionManager();

/**
 * @internal Register an already-open named connection (called by AtlasProvider,
 * which opens connections itself). Records the config on the node too.
 */
export function registerConnection(
	name: string,
	connection: AsyncDatabaseConnection,
	config: ConnectionConfig = {},
): void {
	manager.register(name, config, connection);
}

/** @internal Unregister a named connection IF it still points at `connection` (no close). */
export function unregisterConnection(
	name: string,
	connection: AsyncDatabaseConnection,
): void {
	manager.deregister(name, connection);
}

/** @internal Resolve a live named connection (for `BaseModel.connection`), or `undefined`. */
export function getConnection(
	name: string,
): AsyncDatabaseConnection | undefined {
	return manager.connection(name);
}

/** @internal The shared connection manager (for the AtlasProvider lifecycle-event bridge). */
export function connectionManager(): ConnectionManager {
	return manager;
}

/**
 * Coerce an advisory-lock key to the integer Postgres `pg_*_advisory_lock`
 * requires. Numeric keys pass through; string keys are hashed deterministically
 * (FNV-1a, 32-bit) so lock and unlock agree. Stable within a process is all that
 * matters — atlas never shares a lock namespace with Knex.
 */
function advisoryLockKey(key: string | number): number {
	if (typeof key === "number") return Math.trunc(key);
	let hash = 0x811c9dc5;
	for (let i = 0; i < key.length; i++) {
		hash ^= key.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193);
	}
	return hash | 0; // signed 32-bit — fits Postgres int/bigint
}

/** Validate + dialect-quote a (dot-qualified) identifier. Rejects anything unsafe. */
function quoteIdent(name: string, dialect: AtlasDialect): string {
	const q = dialect === "mysql" ? "`" : '"';
	return name
		.split(".")
		.map((seg) => {
			if (seg === "*") return seg;
			if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(seg)) {
				throw new Error(
					`atlas: invalid identifier segment '${seg}' in '${name}'`,
				);
			}
			return `${q}${seg}${q}`;
		})
		.join(".");
}

/** Build a {@link DbService} over a resolver that yields the live connection. */
export function createDbService(
	resolve: () => AsyncDatabaseConnection,
	readOnly = false,
): DbService {
	const opts = readOnly ? { readOnly } : undefined;
	// Per-call `{ mode: 'read' }` also yields a write-guarded builder, even on an
	// unscoped service (Lucid `db.query({ mode: 'read' })`).
	const optsFor = (options?: DbQueryOptions) =>
		readOnly || options?.mode === "read" ? { readOnly: true } : undefined;
	return {
		query(options) {
			const conn = resolve();
			return new DatabaseQueryBuilder(
				options?.client ?? conn,
				conn.dialect,
				"",
				optsFor(options),
			);
		},
		from(
			source:
				| string
				| DatabaseQueryBuilder
				| ((query: DatabaseQueryBuilder) => void),
			alias?: string,
		) {
			const conn = resolve();
			const builder = new DatabaseQueryBuilder(conn, conn.dialect, "", opts);
			return typeof source === "string"
				? builder.from(source)
				: builder.from(source, alias);
		},
		table(table) {
			const conn = resolve();
			return new DatabaseQueryBuilder(conn, conn.dialect, table, opts);
		},
		insertQuery(options) {
			const conn = resolve();
			return new DatabaseQueryBuilder(
				options?.client ?? conn,
				conn.dialect,
				"",
				optsFor(options),
			);
		},
		rawQuery(sql, bindings = []) {
			const conn = resolve();
			return new RawQueryBuilder(conn, conn.dialect, sql, bindings);
		},
		connection(name, connOptions) {
			// No name → the default connection's service (Lucid `db.connection()`).
			if (name === undefined) {
				return createDbService(resolve, connOptions?.mode === "read");
			}
			return createDbService(() => {
				const conn = getConnection(name);
				if (!conn) {
					throw new Error(
						`[atlas] no connection registered under '${name}'. Is it in config/database.ts connections?`,
					);
				}
				return conn;
			}, connOptions?.mode === "read");
		},
		modelQuery(model) {
			return model.query();
		},
		raw(sql, params = []) {
			// Resolve `??`/named bindings only when present, so the common
			// positional/no-binding path (and Postgres `::casts`) is untouched.
			const hasNamed = !Array.isArray(params);
			const hasIdent = typeof sql === "string" && sql.includes("??");
			if (!hasNamed && !hasIdent) {
				return new RawSql(sql, params as unknown[]);
			}
			const resolved = resolveRawBindings(sql, params, resolve().dialect);
			return new RawSql(resolved.sql, resolved.params);
		},
		ref(column) {
			return new RawSql(quoteIdent(column, resolve().dialect), []);
		},
		async truncate(table, cascade) {
			const conn = resolve();
			const t = quoteIdent(table, conn.dialect);
			// SQLite has no TRUNCATE — DELETE clears the table (Lucid does the same).
			const sql =
				conn.dialect === "sqlite"
					? `DELETE FROM ${t}`
					: conn.dialect === "postgres"
						? `TRUNCATE TABLE ${t}${cascade ? " CASCADE" : ""}`
						: `TRUNCATE TABLE ${t}`;
			await conn.execute(sql, []);
		},
		truncateAllTables(ignoreTables = []) {
			return truncateAll(resolve(), ignoreTables);
		},
		async getAdvisoryLock(key) {
			const conn = resolve();
			if (conn.dialect === "sqlite") {
				throw new Error(
					"[atlas] advisory locks are not supported on SQLite (Postgres/MySQL only).",
				);
			}
			if (conn.dialect === "postgres") {
				const rows = await conn.query<{ locked: boolean }>(
					"SELECT pg_try_advisory_lock($1) AS locked",
					[advisoryLockKey(key)],
				);
				return Boolean(rows[0]?.locked);
			}
			// MySQL: GET_LOCK(name, 0) → non-blocking try (1 acquired, 0 busy).
			const rows = await conn.query<{ locked: number }>(
				"SELECT GET_LOCK(?, 0) AS locked",
				[String(key)],
			);
			return Number(rows[0]?.locked) === 1;
		},
		async releaseAdvisoryLock(key) {
			const conn = resolve();
			if (conn.dialect === "sqlite") {
				throw new Error(
					"[atlas] advisory locks are not supported on SQLite (Postgres/MySQL only).",
				);
			}
			if (conn.dialect === "postgres") {
				const rows = await conn.query<{ released: boolean }>(
					"SELECT pg_advisory_unlock($1) AS released",
					[advisoryLockKey(key)],
				);
				return Boolean(rows[0]?.released);
			}
			const rows = await conn.query<{ released: number }>(
				"SELECT RELEASE_LOCK(?) AS released",
				[String(key)],
			);
			return Number(rows[0]?.released) === 1;
		},
		get manager() {
			return manager;
		},
		execute(sql, params) {
			return resolve().execute(sql, params);
		},
		get transaction() {
			return resolve().transaction?.bind(resolve());
		},
		runInTransaction(batch) {
			return resolve().runInTransaction(batch);
		},
		get dialect() {
			return resolve().dialect;
		},
		ping() {
			return resolve().ping();
		},
		close() {
			return resolve().close();
		},
	};
}

const db: DbService = createDbService(() => {
	if (!instance) {
		throw new Error(
			"[atlas] db singleton accessed before AtlasProvider.boot() ran. " +
				"Check that `@c9up/atlas/provider` is listed in your reamrc.ts " +
				"providers and that `config/database.ts` defines at least one " +
				"connection.",
		);
	}
	return instance;
});

export default db;
