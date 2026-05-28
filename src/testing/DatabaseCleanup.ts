/**
 * Database cleanup utilities for testing.
 *
 * @implements MISS-19
 */

import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import { compileStatementNative, getAtlasDialect } from "../query/native.js";

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
 * Truncate all user tables (excludes _migrations and internal tables).
 *
 * The SELECT on `sqlite_master` is SQLite-specific introspection and is kept
 * as raw SQL intentionally — it's not a user query. The resulting DELETEs
 * go through the Rust compiler.
 */
export async function truncateAll(db: AsyncDatabaseConnection): Promise<void> {
	// SQL LIKE `_` is a single-char wildcard, not a literal underscore.
	// `NOT LIKE '_%'` without `ESCAPE` matches NOTHING (every non-empty name
	// matches `_%`). The `ESCAPE '\'` clause makes `\_` a literal underscore,
	// so the exclusion targets only names starting with `_` (the convention
	// for framework-private tables, including `_migrations`).
	const tables = await db.query(
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%'",
	);
	const dialect = getAtlasDialect();
	for (const row of tables) {
		const name = row.name;
		if (typeof name !== "string") continue;
		const compiled = compileStatementNative(
			{ kind: "delete", table: name, wheres: [] },
			dialect,
		);
		await db.execute(compiled.statements[0], compiled.params);
	}
}
