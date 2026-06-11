/**
 * Migration Runner — discovers, executes, and tracks database migrations.
 *
 * @implements FR34
 */

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
import type { Migration } from "./Migration.js";

const DEFAULT_TABLE = "ream_migrations";
const TABLE_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

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
	 *   skips tables whose name starts with `ream_` (via `LIKE 'ream\_%' ESCAPE '\'`).
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
	constructor(
		db: DatabaseAdapter,
		options?: {
			migrationsDir?: string;
			dialect?: AtlasDialect;
			tableName?: string;
		},
	) {
		this.#db = db;
		this.#migrationsDir = options?.migrationsDir ?? "database/migrations";
		this.#dialect = options?.dialect ?? "sqlite";
		this.#tableName =
			options?.tableName === undefined
				? DEFAULT_TABLE
				: validateTrackingTableName(options.tableName);
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
		const applied = await queryStmt<MigrationRecord>(this.#db, this.#dialect, {
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
		});
		const appliedMap = new Map(applied.map((r) => [r.name, r.batch]));

		const files = await this.#discoverFiles();
		return files.map((f) => ({
			name: f,
			status: appliedMap.has(f) ? "applied" : "pending",
			batch: appliedMap.get(f),
		}));
	}

	/** Run all pending migrations. */
	async migrate(): Promise<string[]> {
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
			const statements = await migration.getUpSQL();

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
			const batchStatements: BatchStmt[] = [
				...statements.map((sql) => ({ sql, params: [] as unknown[] })),
				{ sql: insertSql, params: insertCompiled.params },
			];

			await this.#runAtomic(batchStatements, name);
			executed.push(name);
		}

		return executed;
	}

	/** Rollback the last batch of migrations. */
	async rollback(): Promise<string[]> {
		await this.init();

		const batch = await this.#currentBatch();
		if (batch === 0) {
			return [];
		}

		const toRollback = await queryStmt<MigrationRecord>(
			this.#db,
			this.#dialect,
			{
				kind: "select",
				table: this.#tableName,
				select: ["name"],
				wheres: [{ column: "batch", operator: "=", value: batch, type: "and" }],
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
			const statements = await migration.getDownSQL();

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
			const batchStatements: BatchStmt[] = [
				...statements.map((sql) => ({ sql, params: [] as unknown[] })),
				{ sql: deleteSql, params: deleteCompiled.params },
			];

			await this.#runAtomic(batchStatements, record.name);
			rolled.push(record.name);
		}

		return rolled;
	}

	/**
	 * Run a batch of statements in a single transaction if the adapter supports
	 * it; otherwise fall back to sequential execute (best-effort). Adapters that
	 * lack transaction support get a warning so integrators notice the risk.
	 */
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
	async refresh(): Promise<{ rolled: string[]; executed: string[] }> {
		const rolled = await this.reset();
		const executed = await this.migrate();
		return { rolled, executed };
	}

	/**
	 * Drop EVERY table the migrations created (via successive rollbacks) then
	 * run every migration from scratch. `migrate:fresh` in Lucid parlance.
	 *
	 * Distinct from `refresh()` only in intent — both end up with a fresh DB.
	 */
	async fresh(): Promise<{ rolled: string[]; executed: string[] }> {
		return this.refresh();
	}

	/**
	 * Rollback every applied batch (alias for `migrate:reset`).
	 * Returns the list of rolled-back migration names (in rollback order).
	 */
	async reset(): Promise<string[]> {
		await this.init();
		const all: string[] = [];
		// Roll back batches one by one until nothing remains.
		while ((await this.#currentBatch()) > 0) {
			const rolled = await this.rollback();
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
		const files = (await this.#discoverFiles()).filter(
			(f) => !appliedNames.has(f),
		);

		const result: Array<{ name: string; sql: string[] }> = [];
		for (const name of files) {
			this.#assertSafeName(name);
			const migration = await this.#loadMigration(name);
			const statements = await migration.getUpSQL();
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
			return entries
				.filter((f) => f.endsWith(".ts") || f.endsWith(".js"))
				.sort()
				.map((f) => f.replace(/\.(ts|js)$/, ""));
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
