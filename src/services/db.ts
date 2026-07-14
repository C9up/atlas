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
import { RawSql } from "../query/QueryBuilder.js";

/** The `db` singleton surface: the bound connection plus the AdonisJS-style `db.raw()` builder. */
export interface DbService extends AsyncDatabaseConnection {
	/**
	 * Build a raw SQL expression — AdonisJS `db.raw()` / `Database.raw()`. Use it
	 * for query fragments and for column defaults that are SQL expressions:
	 *
	 *   t.uuid('id').defaultTo(db.raw('gen_random_uuid()'))
	 */
	raw(sql: string, params?: unknown[]): RawSql;
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

const db: DbService = new Proxy({} as DbService, {
	get(_target, prop) {
		// `raw` is a pure builder (no connection needed) — available pre-boot too.
		if (prop === "raw") {
			return (sql: string, params: unknown[] = []) => new RawSql(sql, params);
		}
		if (!instance) {
			throw new Error(
				"[atlas] db singleton accessed before AtlasProvider.boot() ran. " +
					"Check that `@c9up/atlas/provider` is listed in your reamrc.ts " +
					"providers and that `config/database.ts` defines at least one " +
					"connection.",
			);
		}
		const value = Reflect.get(instance, prop, instance);
		return typeof value === "function" ? value.bind(instance) : value;
	},
});

export default db;
