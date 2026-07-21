/**
 * Database cleanup utilities for testing.
 *
 * @implements MISS-19
 */

import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import { AtlasError } from "../errors.js";
import { compileStatementNative } from "../query/native.js";
import { listUserTables, runWithoutForeignKeys } from "../schema/catalog.js";
import type { TransactionClient } from "../Transaction.js";

/** What {@link useTransaction} hands back: the pinned trx plus its teardown. */
export interface TestTransaction {
	/**
	 * The interactive transaction, pinned to ONE pooled connection. Run every
	 * query the test makes through THIS handle (`trx.query` / `trx.execute`, or
	 * `repo.useTransaction(trx)`), never the pooled `db` — that is what makes the
	 * rollback isolate the test.
	 */
	trx: TransactionClient;
	/** Roll everything the test did back and return the connection to the pool. */
	rollback: () => Promise<void>;
}

/**
 * Isolate a test by running it inside a transaction that is rolled back after
 * (Adonis Lucid parity — Lucid's tests use `db.transaction()` the same way).
 *
 * The earlier implementation issued `SAVEPOINT` / `ROLLBACK` through the pooled
 * `db`, so with `poolMax > 1` the savepoint and its rollback — and the test's
 * own queries — could each land on a DIFFERENT pooled connection, and the
 * "isolation" silently did nothing. This pins a single interactive transaction
 * instead: every query on the returned `trx` runs on that one connection, and
 * `rollback()` reverts them together, correctly on any pool size.
 */
export async function useTransaction(
	db: AsyncDatabaseConnection,
): Promise<TestTransaction> {
	if (typeof db.transaction !== "function") {
		throw new AtlasError(
			"E_NO_INTERACTIVE_TRANSACTION",
			"useTransaction() needs a connection with an interactive transaction() (a real napi connection); this adapter has none.",
			{
				hint: "Use createNapiConnection(), or drive rollback yourself if your adapter can't pin a connection.",
			},
		);
	}
	// Manual (callback-less) mode: a pinned TransactionClient the test drives and
	// tears down via rollback() — no cross-pool savepoint scatter.
	const trx = await db.transaction();
	return { trx, rollback: () => trx.rollback() };
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

	// Postgres: a plain DELETE respects FKs immediately (atlas FKs aren't
	// DEFERRABLE), so deleting a parent before its children raises 23503 — and
	// nothing orders the tables. `TRUNCATE … CASCADE` is transactional on pg,
	// order-independent, and needs no session-level FK toggle to leak. (Names
	// come from the DB catalog; quote them for identifiers with embedded quotes.)
	if (dialect === "postgres") {
		const list = tables.map((t) => `"${t.replace(/"/g, '""')}"`).join(", ");
		await db.execute(`TRUNCATE ${list} RESTART IDENTITY CASCADE`);
		return;
	}

	// MySQL/SQLite: DELETE (TRUNCATE auto-commits on MySQL, breaking test
	// transaction isolation), with FK suspension on ONE pinned connection.
	const statements = tables.map((name) => {
		const compiled = compileStatementNative(
			{ kind: "delete", table: name, wheres: [] },
			dialect,
		);
		return { sql: compiled.statements[0], params: compiled.params };
	});
	await runWithoutForeignKeys(db, dialect, statements);
}
