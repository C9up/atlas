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

import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
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

/** Options accepted by the Lucid query-builder entry points. */
export interface DbQueryOptions {
	/** Route the query through this transaction client (Lucid `{ client: trx }`). */
	client?: QueryExecutor;
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
	 * Scope the service to a named connection (Lucid `db.connection(name)`).
	 * Pass `{ mode: 'read' }` to reject writes on the returned service (Lucid
	 * `db.connection(name, { mode: 'read' })`).
	 */
	connection(name: string, options?: ConnectionOptions): DbService;
	/**
	 * Build a raw SQL expression — AdonisJS `db.raw()`. For query fragments and
	 * column defaults that are SQL expressions:
	 *
	 *   t.uuid('id').defaultTo(db.raw('gen_random_uuid()'))
	 *
	 * Bindings may be positional (`?`/`??`) or named (`:name`/`:name:`).
	 */
	raw(sql: string, params?: unknown[] | Record<string, unknown>): RawSql;
	/** Run a statement for effect (forwarded to the connection). */
	execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>;
	/** Managed/manual interactive transaction (forwarded, Lucid `db.transaction`). */
	transaction: AsyncDatabaseConnection["transaction"];
	/** Atomic batch (forwarded). */
	runInTransaction: AsyncDatabaseConnection["runInTransaction"];
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

// Named-connection registry — backs `BaseModel.connection = 'analytics'` so a
// model can resolve a non-default connection from a plain import (AdonisJS
// `static connection`). Populated by AtlasProvider for every opened connection.
const namedConnections = new Map<string, AsyncDatabaseConnection>();

/** @internal Register a named connection (called by AtlasProvider.boot per connection). */
export function registerConnection(
	name: string,
	connection: AsyncDatabaseConnection,
): void {
	namedConnections.set(name, connection);
}

/** @internal Unregister a named connection IF it still points at `connection`. */
export function unregisterConnection(
	name: string,
	connection: AsyncDatabaseConnection,
): void {
	if (namedConnections.get(name) === connection) namedConnections.delete(name);
}

/** @internal Resolve a named connection (for `BaseModel.connection`), or `undefined`. */
export function getConnection(
	name: string,
): AsyncDatabaseConnection | undefined {
	return namedConnections.get(name);
}

/** Build a {@link DbService} over a resolver that yields the live connection. */
export function createDbService(
	resolve: () => AsyncDatabaseConnection,
	readOnly = false,
): DbService {
	const opts = readOnly ? { readOnly } : undefined;
	return {
		query(options) {
			const conn = resolve();
			return new DatabaseQueryBuilder(
				options?.client ?? conn,
				conn.dialect,
				"",
				opts,
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
				opts,
			);
		},
		rawQuery(sql, bindings = []) {
			const conn = resolve();
			return new RawQueryBuilder(conn, conn.dialect, sql, bindings);
		},
		connection(name, connOptions) {
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
