/**
 * Migration Runner — discovers, executes, and tracks database migrations.
 *
 * @implements FR34
 */

import { randomUUID } from "node:crypto";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { AtlasError } from "../errors.js";
import { type AtlasDialect, compileStatementNative } from "../query/native.js";
import {
	assertPathInsideBase,
	assertSafeName,
	pathExists,
} from "../utils/safePath.js";
import {
	type CatalogConnection,
	columnExists,
	listUserTables,
	runWithoutForeignKeys,
	tableExists,
} from "./catalog.js";
import type { DeferredMigrationCallback, Migration } from "./Migration.js";
import { readSchemaDumpManifest } from "./SchemaDumper.js";

const DEFAULT_TABLE = "ream_migrations";
const TABLE_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** The single lock row's fixed primary key — the lock is always this one row. */
const LOCK_ROW_ID = 1;

/**
 * Split a schema-dump `.sql` file into executable statements. Statements are
 * `;`-terminated (the dump format atlas writes); chunks that are empty or only
 * SQL comments once trimmed are dropped. Leading `--` comment lines on a real
 * statement are kept (every dialect parses a comment before a statement).
 */
function splitSqlStatements(sql: string): string[] {
	return sql
		.split(";")
		.map((s) => s.trim())
		.filter((s) => s.replace(/^\s*--.*$/gm, "").trim().length > 0);
}

function validateTrackingTableName(name: string): string {
	if (!TABLE_NAME_PATTERN.test(name)) {
		throw new AtlasError(
			"MIGRATION_INVALID_TABLE_NAME",
			`Invalid tracking-table name: ${JSON.stringify(name)}. ` +
				"Must match /^[A-Za-z_][A-Za-z0-9_]*$/ (letters, digits, underscores; not starting with a digit).",
		);
	}
	return name;
}

/** Compile a statement + run it via the adapter. */
async function runStmt(
	db: DatabaseAdapter,
	dialect: AtlasDialect,
	spec: object,
): Promise<void> {
	const compiled = compileStatementNative(spec, dialect);
	for (const sql of compiled.statements) {
		await db.execute(sql, compiled.params);
	}
}

async function queryStmt<T>(
	db: DatabaseAdapter,
	dialect: AtlasDialect,
	spec: object,
): Promise<T[]> {
	const compiled = compileStatementNative(spec, dialect);
	return db.query<T>(compiled.statements[0], compiled.params);
}

export interface BatchStmt {
	sql: string;
	params?: unknown[];
}

export interface DatabaseAdapter {
	/** Execute a SQL statement with optional parameterized values. */
	execute(sql: string, params?: unknown[]): Promise<void>;
	/** Query rows with optional parameterized values. */
	query<T>(sql: string, params?: unknown[]): Promise<T[]>;
	/**
	 * Run every statement in `batch` atomically in a single transaction.
	 * Required for migrations (used by `migrate()`/`rollback()` — see story
	 * "Migrations non transactionnelles" in the remediation audit).
	 */
	runInTransaction?(batch: readonly BatchStmt[]): Promise<number>;
	/**
	 * Optional MANAGED interactive transaction pinned to one connection (Lucid
	 * `db.transaction(cb)`): commit on success, rollback on throw. Real
	 * connections (`createNapiConnection`), the provider and the CLI all supply
	 * it. It is REQUIRED for the two operations whose strong guarantee cannot be
	 * faked without it: `this.defer()` (its callbacks must share the migration's
	 * transaction) and MySQL FK suspension (a session-level `SET` needs a real
	 * `finally` to restore). Both REJECT rather than silently degrade when it is
	 * absent — plain migrations without either feature don't need it.
	 */
	transaction?<T>(callback: (trx: CatalogConnection) => Promise<T>): Promise<T>;
	/** Close the connection. */
	close(): Promise<void>;
}

export interface MigrationRecord {
	name: string;
	batch: number;
	executed_at: string;
}

export type MigrationState = "applied" | "pending";

export interface MigrationStatus {
	name: string;
	status: MigrationState;
	batch?: number;
}

/**
 * Runs migrations against a database.
 */
export class MigrationRunner {
	#db: DatabaseAdapter;
	#migrationsDir: string;
	#dialect: AtlasDialect;
	#tableName: string;

	/**
	 * @param db Underlying database adapter.
	 * @param options.migrationsDir Directory containing migration files. Defaults to `database/migrations`.
	 * @param options.dialect SQL dialect of the adapter. Defaults to `sqlite`.
	 * @param options.tableName Custom name for the migrations tracking table.
	 *   Defaults to `"ream_migrations"`. Must match `/^[A-Za-z_][A-Za-z0-9_]*$/` —
	 *   throws `AtlasError("MIGRATION_INVALID_TABLE_NAME")` synchronously otherwise.
	 *
	 *   Cleanup coupling: `DatabaseCleanup.truncateAll` (`src/testing/DatabaseCleanup.ts`)
	 *   skips tables whose name starts with `ream_` (filtered in JS by the shared
	 *   catalog helper, `src/schema/catalog.ts`).
	 *   The default `"ream_migrations"` is therefore auto-protected. Choosing a name
	 *   without the `ream_` prefix (e.g. `"schema_versions"`) opts the tracking table
	 *   out of that protection — `truncateAll` will wipe it alongside user tables,
	 *   forcing a re-`init()` and replay of every migration on next test run.
	 *
	 *   Rename caveat: `init()` emits `CREATE TABLE IF NOT EXISTS <tableName>` —
	 *   it does NOT auto-migrate from a previous tracking table. A project that
	 *   has been running with `ream_migrations` and then changes `tableName` to
	 *   `schema_versions` will get a fresh empty `schema_versions` on next boot,
	 *   read zero applied migrations, and re-run every past migration (likely
	 *   crashing on `CREATE TABLE <user-table>` already-exists). To rename
	 *   safely, copy the history manually before switching:
	 *
	 *     INSERT INTO schema_versions SELECT * FROM ream_migrations;
	 *     -- (then DROP TABLE ream_migrations once verified)
	 */
	#disableRollbacksInProduction: boolean;
	#disableLocks: boolean;
	#disableTransactions: boolean;
	#naturalSort: boolean;
	#lockTableName: string;
	/** Token identifying the lock WE hold, so release only clears our own lock. */
	#lockToken: string | undefined;

	constructor(
		db: DatabaseAdapter,
		options?: {
			migrationsDir?: string;
			dialect?: AtlasDialect;
			tableName?: string;
			/**
			 * Refuse destructive ops (rollback/reset/refresh/fresh/wipe) when
			 * `NODE_ENV === 'production'` (Adonis Lucid `disableRollbacksInProduction`)
			 * — a guard against dropping production data by accident. **Defaults to
			 * ON**, matching Lucid: a destructive migration command in production
			 * throws unless `{ force: true }` (CLI `--force`). Pass `false` to opt out.
			 */
			disableRollbacksInProduction?: boolean;
			/**
			 * Skip the migration lock (Adonis Lucid `--disable-locks`). The lock is
			 * a `<tableName>_lock` table with an `is_locked` flag (Knex mechanism),
			 * so it prevents concurrent migrations on every dialect, SQLite too.
			 */
			disableLocks?: boolean;
			/**
			 * Run EVERY migration outside a transaction (Adonis Lucid global
			 * `migrations.disableTransactions`). A single migration can also opt out
			 * on its own with `static disableTransactions = true`; the effective value
			 * is the OR of the two. Defaults to `false` — migrations are wrapped in a
			 * transaction for clean, all-or-nothing rollbacks.
			 */
			disableTransactions?: boolean;
			/**
			 * Sort migration files with a numeric-aware comparator (Adonis Lucid
			 * `migrations.naturalSort`), so `2_x` orders before `10_x`. Defaults to
			 * `false` — plain lexicographic order, which is already correct for the
			 * fixed-width `Date.now()` prefixes `make:migration` generates.
			 */
			naturalSort?: boolean;
		},
	) {
		this.#db = db;
		this.#migrationsDir = options?.migrationsDir ?? "database/migrations";
		this.#dialect = options?.dialect ?? "sqlite";
		this.#tableName =
			options?.tableName === undefined
				? DEFAULT_TABLE
				: validateTrackingTableName(options.tableName);
		// Defaults ON (Lucid parity): destructive ops are guarded in production
		// unless the caller explicitly opts out or forces per call.
		this.#disableRollbacksInProduction =
			options?.disableRollbacksInProduction ?? true;
		this.#disableLocks = options?.disableLocks ?? false;
		this.#disableTransactions = options?.disableTransactions ?? false;
		this.#naturalSort = options?.naturalSort ?? false;
		this.#lockTableName = `${this.#tableName}_lock`;
	}

	/**
	 * Ensure the lock table exists and holds exactly one row. Mirrors the Knex /
	 * AdonisJS Lucid migration-lock mechanism: a `<tableName>_lock` table with an
	 * `is_locked` flag (NOT a Postgres advisory lock), so it works identically on
	 * every dialect, SQLite included.
	 */
	async #ensureLockTable(): Promise<void> {
		await runStmt(this.#db, this.#dialect, {
			kind: "createTable",
			table: this.#lockTableName,
			ifNotExists: true,
			columns: [
				// A FIXED single-row identity (id = LOCK_ROW_ID, PK). Not
				// auto-increment: every acquire/release/seed targets `WHERE id = 1`,
				// so a duplicate row is impossible (the PK rejects it). That removes
				// any need for a "recover from multi-row" DELETE that could wipe an
				// active lock held by another process.
				{
					name: "id",
					kind: "integer",
					nullable: false,
					primary: true,
					unique: false,
					default: null,
					references: null,
					length: null,
					precision: null,
					scale: null,
				},
				{
					name: "is_locked",
					kind: "integer",
					nullable: false,
					primary: false,
					unique: false,
					default: null,
					references: null,
					length: null,
					precision: null,
					scale: null,
				},
				{
					name: "locked_by",
					kind: "string",
					nullable: true,
					primary: false,
					unique: false,
					default: null,
					references: null,
					length: null,
					precision: null,
					scale: null,
				},
			],
			indexes: [],
		});
		// Upgrade path: a lock table created by an earlier atlas (id + is_locked,
		// no `locked_by`) is left untouched by CREATE TABLE IF NOT EXISTS, so the
		// token UPDATE/SELECT below would hit a missing column. Add it if absent.
		if (
			!(await columnExists(
				this.#db,
				this.#dialect,
				this.#lockTableName,
				"locked_by",
			))
		) {
			try {
				await this.#db.execute(
					`ALTER TABLE ${this.#lockTableName} ADD COLUMN locked_by TEXT`,
				);
			} catch {
				// TOCTOU: two concurrent first-boots on a legacy lock table can both
				// see the column absent and both ALTER; the loser hits a
				// duplicate-column error (no dialect has ADD COLUMN IF NOT EXISTS on
				// all three). Swallow it — the column now exists either way; a real
				// failure surfaces on the very next statement (seed/UPDATE).
			}
		}
		// Seed the single lock row idempotently. INSERT-or-ignore on the PK means
		// concurrent seeders and re-runs never create a second row and never touch
		// an existing (possibly held) lock. The lock-table name derives from the
		// validated tracking-table name — a safe identifier, no injection surface.
		await this.#db.execute(this.#seedLockRowSql());
	}

	/** Dialect-specific idempotent seed of the single `id = 1` lock row. */
	#seedLockRowSql(): string {
		const t = this.#lockTableName;
		const values = `(${LOCK_ROW_ID}, 0)`;
		switch (this.#dialect) {
			case "sqlite":
				return `INSERT OR IGNORE INTO ${t} (id, is_locked) VALUES ${values}`;
			case "mysql":
				return `INSERT IGNORE INTO ${t} (id, is_locked) VALUES ${values}`;
			case "postgres":
				return `INSERT INTO ${t} (id, is_locked) VALUES ${values} ON CONFLICT (id) DO NOTHING`;
		}
	}

	/** Dialect placeholder for a single bound parameter. */
	get #ph(): string {
		return this.#dialect === "postgres" ? "$1" : "?";
	}

	/**
	 * Acquire the migration lock so two processes cannot migrate concurrently
	 * (Adonis Lucid / Knex parity — a lock TABLE, not an advisory lock).
	 *
	 * ATOMIC: a single conditional `UPDATE … WHERE is_locked = 0` stamps our
	 * token — the database serialises concurrent updates on that row, so only ONE
	 * writer flips 0→1; every other writer's `WHERE` no longer matches. The
	 * token read-back tells us whether WE won. This avoids the check-then-set
	 * race of a separate SELECT + UPDATE. Throws `E_MIGRATION_LOCKED` otherwise.
	 */
	async #acquireLock(): Promise<void> {
		if (this.#disableLocks) return;
		await this.#ensureLockTable();
		const token = randomUUID();
		await this.#db.execute(
			`UPDATE ${this.#lockTableName} SET is_locked = 1, locked_by = ${this.#ph} WHERE id = ${LOCK_ROW_ID} AND is_locked = 0`,
			[token],
		);
		const rows = await this.#db.query<{ locked_by: unknown }>(
			`SELECT locked_by FROM ${this.#lockTableName} WHERE id = ${LOCK_ROW_ID}`,
		);
		if (rows[0]?.locked_by !== token) {
			throw new AtlasError(
				"E_MIGRATION_LOCKED",
				"Could not acquire the migration lock — another migration is already running.",
				{
					hint: `Wait for it to finish, clear the ${this.#lockTableName} table if it is stuck, or pass disableLocks.`,
				},
			);
		}
		this.#lockToken = token;
	}

	/** Run `fn` while holding the migration lock; always release, even on throw. */
	async #withLock<T>(fn: () => Promise<T>): Promise<T> {
		await this.#acquireLock();
		try {
			return await fn();
		} finally {
			await this.#releaseLock();
		}
	}

	/** Release the migration lock — only OUR token, so we never clear someone else's. */
	async #releaseLock(): Promise<void> {
		if (this.#disableLocks || this.#lockToken === undefined) return;
		const token = this.#lockToken;
		this.#lockToken = undefined;
		await this.#db.execute(
			`UPDATE ${this.#lockTableName} SET is_locked = 0, locked_by = NULL WHERE id = ${LOCK_ROW_ID} AND locked_by = ${this.#ph}`,
			[token],
		);
	}

	/**
	 * Force-clear a stuck migration lock (Adonis Lucid `migration:unlock`). A
	 * process killed mid-migrate leaves `is_locked = 1` with a stale token and
	 * NO way for a later run to acquire — this unconditionally clears the row so
	 * migrations can proceed. Returns `true` if a held lock was cleared.
	 */
	async forceUnlock(): Promise<boolean> {
		if (!(await tableExists(this.#db, this.#dialect, this.#lockTableName))) {
			return false;
		}
		// Upgrade a legacy lock table (id + is_locked, no `locked_by`) BEFORE the
		// `locked_by = NULL` UPDATE below — otherwise it hits a missing column and
		// throws instead of clearing the stuck lock. `#ensureLockTable` is
		// idempotent (CREATE IF NOT EXISTS + ALTER-add + idempotent seed).
		await this.#ensureLockTable();
		const rows = await this.#db.query<{ is_locked: unknown }>(
			`SELECT is_locked FROM ${this.#lockTableName} WHERE id = ${LOCK_ROW_ID}`,
		);
		const wasLocked =
			rows[0]?.is_locked === 1 ||
			rows[0]?.is_locked === true ||
			rows[0]?.is_locked === "1";
		await this.#db.execute(
			`UPDATE ${this.#lockTableName} SET is_locked = 0, locked_by = NULL WHERE id = ${LOCK_ROW_ID}`,
		);
		return wasLocked;
	}

	/**
	 * Throw when destructive migration operations (rollback / reset / refresh /
	 * fresh / wipe) are disabled in production and this is a production run,
	 * unless the caller explicitly forces it.
	 */
	#assertRollbackAllowed(force: boolean): void {
		if (
			this.#disableRollbacksInProduction &&
			!force &&
			process.env.NODE_ENV === "production"
		) {
			throw new AtlasError(
				"E_ROLLBACK_DISABLED_IN_PRODUCTION",
				"Destructive migration operations are disabled in production. Pass { force: true } to override.",
				{
					hint: "This guard exists to prevent dropping production data by accident.",
				},
			);
		}
	}

	/** Ensure the ream_migrations tracking table exists. */
	async init(): Promise<void> {
		await runStmt(this.#db, this.#dialect, {
			kind: "createTable",
			table: this.#tableName,
			ifNotExists: true,
			columns: [
				// Auto-increment PK (Lucid's adonis_schema uses `increments`). The
				// Rust compiler emits the dialect-appropriate identity clause, so
				// the INSERT below omits `id` on every dialect.
				{
					name: "id",
					kind: "integer",
					autoIncrement: true,
					nullable: false,
					primary: true,
					unique: false,
					default: null,
					references: null,
					length: null,
					precision: null,
					scale: null,
				},
				{
					name: "name",
					kind: "string",
					length: 255,
					nullable: false,
					primary: false,
					unique: true,
					default: null,
					references: null,
					precision: null,
					scale: null,
				},
				{
					name: "batch",
					kind: "integer",
					nullable: false,
					primary: false,
					unique: false,
					default: null,
					references: null,
					length: null,
					precision: null,
					scale: null,
				},
				{
					name: "executed_at",
					kind: "timestamp",
					nullable: false,
					primary: false,
					unique: false,
					default: "CURRENT_TIMESTAMP",
					references: null,
					length: null,
					precision: null,
					scale: null,
				},
			],
			indexes: [],
		});
	}

	/** Get the status of all migrations. */
	async status(): Promise<MigrationStatus[]> {
		// READ-ONLY: do NOT init() (which would CREATE the tracking table). On a
		// never-migrated database the table is simply absent → everything pending,
		// no side effect (AdonisJS/Lucid `migration:status` parity, same guarantee
		// as dryRun).
		const applied: MigrationRecord[] = (await tableExists(
			this.#db,
			this.#dialect,
			this.#tableName,
		))
			? await queryStmt<MigrationRecord>(this.#db, this.#dialect, {
					kind: "select",
					table: this.#tableName,
					select: ["name", "batch"],
					wheres: [],
					orderBy: [{ column: "name", direction: "asc" }],
					groupBy: [],
					having: [],
					limit: null,
					offset: null,
					distinct: false,
					ctes: [],
					unions: [],
				})
			: [];
		const appliedMap = new Map(applied.map((r) => [r.name, r.batch]));

		const files = await this.#discoverFiles();
		return files.map((f) => ({
			name: f,
			status: appliedMap.has(f) ? "applied" : "pending",
			batch: appliedMap.get(f),
		}));
	}

	/** Run all pending migrations. */
	async migrate(options: { schemaPath?: string } = {}): Promise<string[]> {
		return this.#withLock(() => this.#migrateLocked(options.schemaPath));
	}

	async #migrateLocked(schemaPath?: string): Promise<string[]> {
		// Adonis Lucid `migration:run --schema-path`: when nothing is applied yet
		// and a dump exists, load it in place of replaying history, then run only
		// the migrations that postdate the dump (below, via the normal pending set).
		await this.#maybeLoadDump(schemaPath);
		await this.init();

		const applied = await queryStmt<MigrationRecord>(this.#db, this.#dialect, {
			kind: "select",
			table: this.#tableName,
			select: ["name"],
			wheres: [],
			orderBy: [],
			groupBy: [],
			having: [],
			limit: null,
			offset: null,
			distinct: false,
			ctes: [],
			unions: [],
		});
		const appliedNames = new Set(applied.map((r) => r.name));
		const files = await this.#discoverFiles();
		const pending = files.filter((f) => !appliedNames.has(f));

		if (pending.length === 0) {
			return [];
		}

		const batch = (await this.#currentBatch()) + 1;
		const executed: string[] = [];

		for (const name of pending) {
			this.#assertSafeName(name);
			const migration = await this.#loadMigration(name);
			const statements = await migration.getUpSQL(this.#db);
			const deferred = migration.consumeDeferred();
			// Effective opt-out: the global config OR this migration's own
			// `static disableTransactions = true`.
			const disableTx =
				this.#disableTransactions || migration.transactionsDisabled;

			// Compile the ream_migrations INSERT so we can include it in the same
			// transaction as the migration's own DDL/DML — either everything
			// commits or nothing does.
			const insertCompiled = compileStatementNative(
				{
					kind: "insert",
					table: this.#tableName,
					values: [
						["name", name],
						["batch", batch],
					],
				},
				this.#dialect,
			);

			const [insertSql] = insertCompiled.statements;
			if (insertSql === undefined) {
				throw new AtlasError(
					"MIGRATION_COMPILE_EMPTY",
					`compileStatementNative returned no statements for migration insert of '${name}'`,
				);
			}
			await this.#runStep(
				statements,
				deferred,
				{ sql: insertSql, params: insertCompiled.params },
				name,
				disableTx,
			);
			executed.push(name);
		}

		return executed;
	}

	/** Rollback the last batch of migrations. */
	async rollback(
		options: { batch?: number; force?: boolean } = {},
	): Promise<string[]> {
		this.#assertRollbackAllowed(options.force ?? false);
		return this.#withLock(() => this.#rollbackLocked(options));
	}

	async #rollbackLocked(options: {
		batch?: number;
		force?: boolean;
	}): Promise<string[]> {
		await this.init();

		const current = await this.#currentBatch();
		if (current === 0) {
			return [];
		}

		// Default: roll back only the latest batch. With `batch: N`, roll back
		// every migration applied AFTER batch N (Lucid's `--batch` — a target to
		// return to, not a count). `batch: 0` rolls the whole history back.
		const target = options.batch ?? current - 1;
		if (target >= current) {
			return [];
		}

		const toRollback = await queryStmt<MigrationRecord>(
			this.#db,
			this.#dialect,
			{
				kind: "select",
				table: this.#tableName,
				select: ["name"],
				wheres: [
					{ column: "batch", operator: ">", value: target, type: "and" },
				],
				// Reverse INSERTION order (auto-increment `id`), not name order — a
				// date- or hash-prefixed naming scheme would otherwise roll back in
				// the wrong sequence. `id DESC` is always the inverse of application.
				orderBy: [{ column: "id", direction: "desc" }],
				groupBy: [],
				having: [],
				limit: null,
				offset: null,
				distinct: false,
				ctes: [],
				unions: [],
			},
		);

		const rolled: string[] = [];

		for (const record of toRollback) {
			this.#assertSafeName(record.name);
			const migration = await this.#loadMigration(record.name);
			const statements = await migration.getDownSQL(this.#db);
			const deferred = migration.consumeDeferred();
			// `static disableTransactions` applies to down() too (Adonis parity).
			const disableTx =
				this.#disableTransactions || migration.transactionsDisabled;

			const deleteCompiled = compileStatementNative(
				{
					kind: "delete",
					table: this.#tableName,
					wheres: [
						{ column: "name", operator: "=", value: record.name, type: "and" },
					],
				},
				this.#dialect,
			);

			const [deleteSql] = deleteCompiled.statements;
			if (deleteSql === undefined) {
				throw new AtlasError(
					"MIGRATION_COMPILE_EMPTY",
					`compileStatementNative returned no statements for migration delete of '${record.name}'`,
				);
			}
			await this.#runStep(
				statements,
				deferred,
				{ sql: deleteSql, params: deleteCompiled.params },
				record.name,
				disableTx,
			);
			rolled.push(record.name);
		}

		return rolled;
	}

	/**
	 * Run one migration step: its schema statements plus the bookkeeping record
	 * write (INSERT for migrate, DELETE for rollback).
	 *
	 * With no deferred callbacks, schema + record commit together in one atomic
	 * batch. With `this.defer()` callbacks and an adapter that exposes an
	 * interactive `transaction()`, the schema, the deferred callbacks AND the
	 * record write all run in ONE transaction — a throwing callback rolls the
	 * schema back too (fully atomic on sqlite/postgres; MySQL auto-commits DDL, so
	 * only its tracking row is bound to the callbacks). An adapter with no
	 * `transaction()` cannot make defer atomic, so it is REJECTED with
	 * `E_DEFER_REQUIRES_TRANSACTION` rather than silently degrading to a
	 * schema-then-callbacks-then-record best effort.
	 *
	 * When `disableTransactions` is set (Adonis `static disableTransactions` / the
	 * global config — a DELIBERATE opt-out for txn-incompatible DDL), every
	 * statement runs OUTSIDE a transaction: schema, then deferred callbacks, then
	 * the record, each committed on its own. Non-atomic by design, so defer runs
	 * loose here and no `transaction()` is required.
	 */
	async #runStep(
		statements: string[],
		deferred: DeferredMigrationCallback[],
		recordStmt: BatchStmt,
		migrationName: string,
		disableTransactions: boolean,
	): Promise<void> {
		const schemaBatch: BatchStmt[] = statements.map((sql) => ({
			sql,
			params: [] as unknown[],
		}));
		if (disableTransactions) {
			// Deliberate opt-out (Adonis): run everything unwrapped. No transaction()
			// needed and no throw — the caller has accepted non-atomicity for DDL that
			// cannot run in a transaction (e.g. Postgres CREATE INDEX CONCURRENTLY).
			if (deferred.length > 0) {
				// The sharp edge: with the transaction gone, the schema, the deferred
				// callbacks and the tracking row commit SEPARATELY, so a failing
				// callback leaves the schema applied but the migration unrecorded.
				// Adonis doesn't warn here — a named safety nudge so the non-atomicity
				// of this exact combo is never a surprise. Make both the DDL and the
				// deferred work idempotent (guarded DDL + re-runnable seeds).
				console.warn(
					`[atlas] Migration '${migrationName}' combines this.defer() with disableTransactions — the schema, the deferred callbacks and the tracking row commit separately (non-atomic). Make the DDL and the deferred work idempotent.`,
				);
			}
			for (const { sql, params } of schemaBatch) {
				await this.#db.execute(sql, params);
			}
			for (const callback of deferred) {
				await callback(this.#db);
			}
			await this.#db.execute(recordStmt.sql, recordStmt.params);
			return;
		}
		if (deferred.length === 0) {
			await this.#runAtomic([...schemaBatch, recordStmt], migrationName);
			return;
		}
		// With deferred callbacks: run the schema, the callbacks, AND the tracking
		// row in ONE managed interactive transaction (Lucid runs defer inside the
		// migration transaction). A throwing callback then rolls back the schema
		// too, so the migration is genuinely all-or-nothing / re-runnable — not
		// left applied-but-unrecorded. Fully atomic on sqlite/postgres; MySQL
		// auto-commits DDL so its schema part can't roll back, but the tracking row
		// is still bound to the callbacks. Requires `transaction()` on the adapter.
		if (this.#db.transaction) {
			await this.#db.transaction(async (trx) => {
				for (const { sql, params } of schemaBatch) {
					await trx.execute(sql, params);
				}
				for (const callback of deferred) {
					await callback(trx);
				}
				await trx.execute(recordStmt.sql, recordStmt.params);
			});
			return;
		}
		// No interactive transaction() available: this.defer() CANNOT be atomic —
		// its callbacks must share the migration's transaction (Adonis wraps every
		// migration in a transaction by default, so defer runs inside it). Refuse
		// loudly rather than commit the schema and leave the migration half-applied
		// behind a warning: a strong guarantee must never silently degrade to a
		// weak one. Reached ONLY by a capability gap — an adapter that cannot do
		// transactions at all (real connections, the provider and the CLI always
		// can). This is NOT Adonis's `disableTransactions` opt-out (a deliberate
		// per-migration choice for txn-incompatible DDL); if that parity feature is
		// added, it must route its own non-atomic path, not trip this guard.
		throw new AtlasError(
			"E_DEFER_REQUIRES_TRANSACTION",
			`Migration '${migrationName}' uses this.defer(), which needs an interactive transaction() on the adapter to run atomically — this adapter has none.`,
			{
				hint: "Use a real connection (createNapiConnection) or an adapter that implements transaction(); otherwise remove this.defer().",
			},
		);
	}

	async #runAtomic(
		batch: readonly BatchStmt[],
		migrationName: string,
	): Promise<void> {
		if (this.#db.runInTransaction) {
			await this.#db.runInTransaction(batch);
			return;
		}
		// Fallback — log once and run sequentially. Not ideal; adapters should
		// implement `runInTransaction` for production use.
		console.warn(
			`[atlas] DatabaseAdapter does not support runInTransaction — running '${migrationName}' without atomicity.`,
		);
		for (const { sql, params } of batch) {
			await this.#db.execute(sql, params);
		}
	}

	/**
	 * Rollback + re-run a specific number of batches (or all if `steps` omitted).
	 * `migrate:refresh` in Lucid parlance.
	 *
	 * @implements Story 32.11
	 */
	async refresh(
		options: { force?: boolean } = {},
	): Promise<{ rolled: string[]; executed: string[] }> {
		this.#assertRollbackAllowed(options.force ?? false);
		// One lock held across the WHOLE rollback+re-migrate, so no other run can
		// slip into the free window between reset and migrate.
		return this.#withLock(async () => {
			const rolled = await this.#resetLocked();
			const executed = await this.#migrateLocked();
			return { rolled, executed };
		});
	}

	/**
	 * Drop EVERY user table directly (no `down()`), then run every migration from
	 * scratch. `migrate:fresh` in Lucid parlance.
	 *
	 * Unlike {@link refresh} (which rolls back by calling each migration's
	 * `down()`), `fresh` wipes the tables outright — so it succeeds even when a
	 * `down()` is broken or missing, and it also clears orphan tables no migration
	 * tracks. This matches Lucid's `migration:fresh`. Views, types and domains are
	 * left intact: Lucid gates those behind opt-in flags (`--drop-views` /
	 * `--drop-types` / `--drop-domains`), so tables-only is its default.
	 *
	 * `rolled` is always empty — fresh drops rather than rolls back.
	 */
	async fresh(
		options: { force?: boolean; schemaPath?: string } = {},
	): Promise<{ rolled: string[]; executed: string[] }> {
		// `fresh` DROPS every table — at least as destructive as rollback, so it
		// must honour the same production guard (Lucid runs it behind `--force` in
		// prod). Held under the migration lock for the whole drop+migrate.
		this.#assertRollbackAllowed(options.force ?? false);
		return this.#withLock(async () => {
			await this.#dropAllTables();
			// With `--schema-path`, rebuild from the dump instead of replaying every
			// migration file (Adonis Lucid `migration:fresh --schema-path`).
			const executed = await this.#migrateLocked(options.schemaPath);
			return { rolled: [], executed };
		});
	}

	/**
	 * Load a schema dump (Adonis Lucid `SchemaDumper` output) into the database —
	 * executes every statement in the `.sql` file in order, recreating the tables
	 * AND the migration bookkeeping rows. Used by `--schema-path`; also callable
	 * directly to seed a fresh database from a committed dump.
	 */
	async loadDump(sqlPath: string): Promise<void> {
		const sql = await fsp.readFile(sqlPath, "utf8");
		for (const statement of splitSqlStatements(sql)) {
			await this.#db.execute(statement, []);
		}
	}

	/** Load the dump only when `--schema-path` is set, it exists, and nothing is applied. */
	async #maybeLoadDump(schemaPath?: string): Promise<void> {
		if (!schemaPath) return;
		if (await this.#hasAppliedMigrations()) return;
		if (!(await pathExists(schemaPath))) return;
		// Validate the sidecar manifest first (throws on a corrupt one), then check
		// it matches THIS runner — a dump for another dialect can't be loaded, and a
		// different bookkeeping table would leave the runner unable to see the
		// embedded applied-migration rows.
		const manifest = await readSchemaDumpManifest(schemaPath);
		if (manifest) {
			if (manifest.dialect !== this.#dialect) {
				throw new AtlasError(
					"E_SCHEMA_DUMP_DIALECT_MISMATCH",
					`Schema dump is for '${manifest.dialect}', but this connection is '${this.#dialect}'.`,
				);
			}
			if (manifest.schemaTableName !== this.#tableName) {
				throw new AtlasError(
					"E_SCHEMA_DUMP_TABLE_MISMATCH",
					`Schema dump uses tracking table '${manifest.schemaTableName}', but this runner expects '${this.#tableName}'.`,
				);
			}
		}
		await this.loadDump(schemaPath);
	}

	/** True when the tracking table exists AND has at least one applied row. */
	async #hasAppliedMigrations(): Promise<boolean> {
		if (!(await tableExists(this.#db, this.#dialect, this.#tableName))) {
			return false;
		}
		const rows = await this.#db.query<{ n: number }>(
			`SELECT COUNT(*) AS n FROM ${this.#quoteTable(this.#tableName)}`,
		);
		return Number(rows[0]?.n ?? 0) > 0;
	}

	/** Quote the tracking-table name for the current dialect. */
	#quoteTable(name: string): string {
		return this.#dialect === "mysql" ? `\`${name}\`` : `"${name}"`;
	}

	/**
	 * Drop every user table — INCLUDING the migrations tracking table (Lucid's
	 * `db:wipe`). `fresh()` calls this and then re-migrates; a caller can use it
	 * directly to reset a database to empty.
	 *
	 * Inter-table foreign keys are handled per dialect: Postgres emits
	 * `DROP TABLE … CASCADE`; MySQL and SQLite suspend FK checks for the
	 * duration (they don't accept/respect CASCADE on `DROP TABLE`), restored
	 * even if a drop throws.
	 */
	async wipe(options: { force?: boolean } = {}): Promise<void> {
		// `db:wipe` drops every table — same production guard as rollback/fresh.
		this.#assertRollbackAllowed(options.force ?? false);
		// Drop everything WHILE STILL HOLDING the lock — including the lock table
		// itself as the last step — then null our token so the release is a no-op
		// (the table is gone). Doing it inside the critical section leaves NO window
		// for another process to acquire/recreate the lock and get dropped from
		// under it. Result: a truly empty database.
		await this.#acquireLock();
		try {
			await this.#dropAllTables();
			// Always drop the lock table (IF EXISTS) so wipe leaves a truly empty DB
			// — including when disableLocks is set and a PRIOR locked run created it.
			// Under a held lock this is the last step in the critical section; under
			// disableLocks we hold nothing, so dropping it is safe either way.
			const compiled = compileStatementNative(
				{ kind: "dropTable", table: this.#lockTableName, ifExists: true },
				this.#dialect,
			);
			for (const sql of compiled.statements) {
				await this.#db.execute(sql, compiled.params);
			}
			this.#lockToken = undefined;
		} finally {
			await this.#releaseLock();
		}
	}

	async #dropAllTables(): Promise<void> {
		// Include the tracking table: wipe/fresh reset to fully empty, and
		// fresh() re-creates it via init() on the next migrate(). The lock table
		// is deliberately KEPT — we're holding the lock through it, and dropping it
		// mid-operation would break the release. It is infrastructure, not data.
		const tables = (
			await listUserTables(this.#db, this.#dialect, {
				includeFrameworkTables: true,
			})
		).filter((t) => t !== this.#lockTableName);
		if (tables.length === 0) return;

		const isPg = this.#dialect === "postgres";
		const statements: Array<{ sql: string; params?: unknown[] }> = [];
		for (const table of tables) {
			const compiled = compileStatementNative(
				{ kind: "dropTable", table, ifExists: true, cascade: isPg },
				this.#dialect,
			);
			for (const sql of compiled.statements) {
				statements.push({ sql, params: compiled.params });
			}
		}
		// FK toggle + every DROP run on ONE pinned connection (not scattered
		// across the pool) — see `runWithoutForeignKeys`.
		await runWithoutForeignKeys(this.#db, this.#dialect, statements);
	}

	/**
	 * Rollback every applied batch (alias for `migrate:reset`).
	 * Returns the list of rolled-back migration names (in rollback order).
	 */
	async reset(options: { force?: boolean } = {}): Promise<string[]> {
		this.#assertRollbackAllowed(options.force ?? false);
		return this.#withLock(() => this.#resetLocked());
	}

	/** The reset loop, WITHOUT re-acquiring the lock — the caller holds it, so
	 * the whole reset is one atomic critical section (not lock-per-batch). */
	async #resetLocked(): Promise<string[]> {
		await this.init();
		const all: string[] = [];
		while ((await this.#currentBatch()) > 0) {
			const rolled = await this.#rollbackLocked({ batch: undefined });
			if (rolled.length === 0) break;
			all.push(...rolled);
		}
		return all;
	}

	/**
	 * Dry-run: compute the SQL statements that would be emitted by a migrate
	 * without executing them. Returns the full list of statements per pending
	 * migration file. Useful for CI pre-flight checks.
	 */
	async dryRun(): Promise<Array<{ name: string; sql: string[] }>> {
		// A dry-run must be side-effect-free: do NOT call init() (it would CREATE
		// the tracking table on an empty database). If the table doesn't exist
		// yet, nothing has been applied.
		const appliedNames = new Set<string>();
		if (await tableExists(this.#db, this.#dialect, this.#tableName)) {
			const applied = await queryStmt<MigrationRecord>(
				this.#db,
				this.#dialect,
				{
					kind: "select",
					table: this.#tableName,
					select: ["name"],
					wheres: [],
					orderBy: [],
					groupBy: [],
					having: [],
					limit: null,
					offset: null,
					distinct: false,
					ctes: [],
					unions: [],
				},
			);
			for (const r of applied) appliedNames.add(r.name);
		}
		const files = (await this.#discoverFiles()).filter(
			(f) => !appliedNames.has(f),
		);

		const result: Array<{ name: string; sql: string[] }> = [];
		for (const name of files) {
			this.#assertSafeName(name);
			const migration = await this.#loadMigration(name);
			// Adonis `this.dryRun`: let up()/down() branch on it while we only
			// collect SQL and never execute or run deferred callbacks.
			migration.dryRun = true;
			const statements = await migration.getUpSQL(this.#db);
			result.push({ name, sql: statements });
		}
		return result;
	}

	async #currentBatch(): Promise<number> {
		// MAX(batch) — select expression uses quote_select_expr allowlist which permits MAX.
		const rows = await queryStmt<{ max: number }>(this.#db, this.#dialect, {
			kind: "select",
			table: this.#tableName,
			select: ["MAX(batch) AS max"],
			wheres: [],
			orderBy: [],
			groupBy: [],
			having: [],
			limit: null,
			offset: null,
			distinct: false,
			ctes: [],
			unions: [],
		});
		return rows[0]?.max ?? 0;
	}

	/** Discover migration files sorted by name. Async — never blocks the event loop. */
	async #discoverFiles(): Promise<string[]> {
		try {
			const entries = await fsp.readdir(this.#migrationsDir);
			const files = entries.filter(
				(f) => f.endsWith(".ts") || f.endsWith(".js"),
			);
			// naturalSort (Lucid): numeric-aware so `2_x` < `10_x`. Default: plain
			// lexicographic (UTF-16), correct for fixed-width Date.now() prefixes.
			files.sort(
				this.#naturalSort
					? (a, b) =>
							a.localeCompare(b, undefined, {
								numeric: true,
								sensitivity: "base",
							})
					: undefined,
			);
			return files.map((f) => f.replace(/\.(ts|js)$/, ""));
		} catch (err) {
			// Missing directory → no migrations. Any other error propagates.
			if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
			throw err;
		}
	}

	/** Validate migration name — delegates to the shared safe-path helpers. */
	#assertSafeName(name: string): void {
		assertSafeName(name, "MIGRATION_INVALID", "migration");
	}

	/** Load and instantiate a migration class. */
	async #loadMigration(name: string): Promise<Migration> {
		this.#assertSafeName(name);
		await assertPathInsideBase(
			this.#migrationsDir,
			`${name}.ts`,
			"MIGRATION_INVALID",
			"Migration",
		);

		const tsPath = path.join(this.#migrationsDir, `${name}.ts`);
		const jsPath = path.join(this.#migrationsDir, `${name}.js`);
		const tsExists = await pathExists(tsPath);
		const filePath = tsExists ? tsPath : jsPath;

		if (!tsExists && !(await pathExists(jsPath))) {
			throw new AtlasError(
				"MIGRATION_NOT_FOUND",
				`Migration file not found: ${name}`,
			);
		}

		// ESM dynamic import on Windows REQUIRES a file:// URL — a bare
		// `C:\…` path triggers ERR_UNSUPPORTED_ESM_URL_SCHEME. `pathToFileURL`
		// normalizes both Windows and POSIX paths to a valid URL.
		const mod = await import(pathToFileURL(path.resolve(filePath)).href);
		const MigrationClass = mod.default;

		if (!MigrationClass || typeof MigrationClass !== "function") {
			throw new AtlasError(
				"MIGRATION_INVALID",
				`Migration ${name} must export a default class`,
			);
		}

		return new MigrationClass(this.#dialect);
	}
}
