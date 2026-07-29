/**
 * `testUtils.db()` — the AdonisJS `testUtils.db()` facade for test state
 * management, wired to atlas's migration/seed/truncate/transaction bricks:
 *
 *   const t = testUtils(connection, { migrationsDir, seedersDir });
 *   test.group("users", (group) => {
 *     group.each.setup(() => t.db().truncate());   // teardown truncates
 *   });
 *
 * Each method returns the TEARDOWN function a Japa/helix hook uses (Adonis
 * parity): `group.each.setup(() => testUtils.db().truncate())`.
 *
 * NOTE (named atlas deviation): Lucid's `withGlobalTransaction()` transparently
 * routes EVERY query through a hidden connection-global transaction. atlas
 * deliberately does NOT bind a global transaction (it caused silent pool-scatter
 * bugs — see `useTransaction`); instead it hands back a PINNED trx the test
 * drives explicitly. So `withGlobalTransaction()` returns `{ trx, rollback }` —
 * run the test's queries through `trx`, not the pooled connection.
 */

import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../schema/MigrationRunner.js";
import { runSeederDirectory } from "../schema/Seeder.js";
import {
	type TestTransaction,
	truncateAll,
	useTransaction,
} from "./DatabaseCleanup.js";

/**
 * Adapt a real connection to the {@link DatabaseAdapter} shape MigrationRunner
 * needs — the only mismatch is `execute` (the connection returns row counts,
 * the adapter wants `void`). Forwards the interactive-transaction hooks so
 * transactional migrations + FK suspension keep working.
 */
function connToAdapter(conn: AsyncDatabaseConnection): DatabaseAdapter {
	const adapter: DatabaseAdapter = {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: (sql, params) => conn.query(sql, params),
		close: () => conn.close(),
	};
	if (conn.runInTransaction) {
		adapter.runInTransaction = (batch) => conn.runInTransaction(batch);
	}
	// `conn.transaction()` is the MANUAL (pinned) form; DatabaseAdapter.transaction
	// is the MANAGED (callback) form — different shapes, so it isn't forwarded.
	// runInTransaction covers atomic migration batches; the managed hook is only
	// needed for `defer()`/MySQL-FK migrations, which a bare test connection skips.
	return adapter;
}

/** Where migrations/seeders live, for `migrate()`/`seed()`. */
export interface DbTestUtilsOptions {
	/** Directory of migration files (Adonis `database/migrations`). */
	migrationsDir?: string;
	/** Directory of seeder files (Adonis `database/seeders`). */
	seedersDir?: string;
	/** Tables `truncate()` leaves alone (in addition to `ream_*` internals). */
	ignoreTables?: readonly string[];
}

/** The `testUtils.db()` surface. */
export interface DbTestUtils {
	/** Run migrations now; returns a teardown that rolls them back. */
	migrate(): Promise<() => Promise<void>>;
	/** Returns a teardown that empties every user table (test isolation). */
	truncate(): () => Promise<void>;
	/** Run seeders now (optionally a `files` subset). */
	seed(files?: readonly string[]): Promise<void>;
	/**
	 * Begin a pinned transaction for the test; returns `{ trx, rollback }`. Run
	 * the test's queries through `trx` (atlas has no transparent global trx).
	 */
	withGlobalTransaction(): Promise<TestTransaction>;
}

/** AdonisJS `testUtils` facade (db slice) bound to a connection. */
export function testUtils(
	conn: AsyncDatabaseConnection,
	options: DbTestUtilsOptions = {},
): { db(): DbTestUtils } {
	return {
		db(): DbTestUtils {
			return {
				async migrate() {
					const runner = new MigrationRunner(connToAdapter(conn), {
						migrationsDir: options.migrationsDir,
						dialect: conn.dialect,
					});
					await runner.migrate();
					return async () => {
						await runner.rollback();
					};
				},
				truncate() {
					return async () => {
						await truncateAll(conn, options.ignoreTables);
					};
				},
				async seed(files) {
					if (!options.seedersDir) {
						throw new Error(
							"testUtils.db().seed() needs a `seedersDir` (pass it to testUtils()).",
						);
					}
					await runSeederDirectory(
						options.seedersDir,
						conn,
						files ? { files } : undefined,
					);
				},
				withGlobalTransaction() {
					return useTransaction(conn);
				},
			};
		},
	};
}
