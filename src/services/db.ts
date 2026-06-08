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

let instance: AsyncDatabaseConnection | undefined;

/** @internal Bind the singleton (called by AtlasProvider). */
export function setDb(connection: AsyncDatabaseConnection): void {
	instance = connection;
}

/** @internal Read the singleton (or `undefined` pre-boot). */
export function getDb(): AsyncDatabaseConnection | undefined {
	return instance;
}

const db: AsyncDatabaseConnection = new Proxy({} as AsyncDatabaseConnection, {
	get(_target, prop) {
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
