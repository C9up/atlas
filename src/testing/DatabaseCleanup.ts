/**
 * Database cleanup utilities for testing.
 *
 * @implements MISS-19
 */

import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import { compileStatementNative } from "../query/native.js";
import { listUserTables, runWithoutForeignKeys } from "../schema/catalog.js";

/**
 * Wrap a test in a savepoint that is rolled back after.
 */
export async function useTransaction(
	db: AsyncDatabaseConnection,
): Promise<() => Promise<void>> {
	await db.execute("SAVEPOINT test_savepoint");
	return async () => {
		await db.execute("ROLLBACK TO SAVEPOINT test_savepoint");
		await db.execute("RELEASE SAVEPOINT test_savepoint");
	};
}

/**
 * Empty every user table (leaves `ream_*` framework tables and dialect
 * internals alone). Works on all three dialects — the table list comes from the
 * shared dialect-aware catalog helper, not a SQLite-only `sqlite_master` query.
 *
 * Foreign keys are suspended for the duration so the delete order doesn't
 * matter. The FK toggle and every DELETE run on ONE pinned connection (via
 * `runWithoutForeignKeys` → `runInTransaction`), so a connection pool can't
 * scatter the connection-local `PRAGMA`/`SET` away from the deletes. The row
 * removal goes through the Rust compiler (`DELETE` — cross-dialect, unlike
 * `TRUNCATE` which auto-commits on MySQL).
 */
export async function truncateAll(db: AsyncDatabaseConnection): Promise<void> {
	// The connection's own dialect, not the module default — correct even when
	// an app runs several connections on different engines.
	const dialect = db.dialect;
	const tables = await listUserTables(db, dialect);
	if (tables.length === 0) return;
	const statements = tables.map((name) => {
		const compiled = compileStatementNative(
			{ kind: "delete", table: name, wheres: [] },
			dialect,
		);
		return { sql: compiled.statements[0], params: compiled.params };
	});
	await runWithoutForeignKeys(db, dialect, statements);
}
