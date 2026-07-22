/**
 * Migration console commands — the Ream-idiomatic CLI for running, rolling back,
 * inspecting, and wiping migrations. Same shape and contract as
 * {@link schemaCheckCommand}: plain `{ name, description, run }` objects
 * registered in `reamrc.commands` and dispatched by the console kernel.
 *
 * Each command resolves the live connection from atlas's OWN service locator
 * (`getDb`), never importing `@c9up/ream`, and drives the already-tested
 * {@link MigrationRunner}. Atlas has no global config registry (Lucid parity —
 * you pass your own paths), so every factory takes the `migrationsDir`.
 *
 * @example
 *   // commands/migrate.ts
 *   import { migrationRunCommand } from '@c9up/atlas'
 *   export default migrationRunCommand({ migrationsDir: 'database/migrations' })
 *
 *   // reamrc.ts → commands: [() => import('./commands/migrate.js')]
 *   // run:  <console-entry> migration:run
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../schema/MigrationRunner.js";
import { getDb } from "../services/db.js";
import { assertSafeName } from "../utils/safePath.js";
import type { AtlasCommand } from "./schemaCheckCommand.js";
import {
	generateSchemaFile,
	type SchemaGenerateOptions,
} from "./schemaGenerateCommand.js";

export interface MigrationCommandOptions {
	/** Directory holding the numbered migration files. */
	migrationsDir: string;
	/**
	 * Sort migration files numerically (`2_x` before `10_x`). Adonis Lucid
	 * `migrations.naturalSort`. Defaults to `false`.
	 */
	naturalSort?: boolean;
	/**
	 * Run every migration outside a transaction. Adonis Lucid
	 * `migrations.disableTransactions`. A migration can also opt out with
	 * `static disableTransactions = true`. Defaults to `false`.
	 */
	disableTransactions?: boolean;
	/**
	 * Regenerate the schema file after a mutating migration command
	 * (run/rollback/reset/refresh/fresh) — Adonis Lucid's post-migration
	 * `schema:generate`. Off unless an `outputPath` is given; suppress per-run
	 * with the `--no-schema-generate` flag.
	 */
	schemaGeneration?: SchemaGenerateOptions & { enabled?: boolean };
}

/**
 * Adapt the shared singleton connection to the runner's {@link DatabaseAdapter}.
 * `close()` is intentionally a no-op — the console kernel owns the connection's
 * lifecycle, so a command must not tear down a connection other commands share.
 */
function toAdapter(conn: AsyncDatabaseConnection): DatabaseAdapter {
	return {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: (sql, params) => conn.query(sql, params),
		runInTransaction: (batch) => conn.runInTransaction(batch),
		// Thread the connection's managed interactive transaction through so the
		// runner can make this.defer() atomic and restore MySQL FK checks.
		transaction: conn.transaction?.bind(conn),
		close: async () => {},
	};
}

/**
 * Resolve a runner from the live connection, or report and set a failing exit
 * code when no connection is registered (mirrors `schemaCheckCommand`).
 */
function resolveRunner(
	options: MigrationCommandOptions,
): MigrationRunner | undefined {
	const db = getDb();
	if (!db) {
		console.error(
			"[atlas] no database connection — is AtlasProvider registered?",
		);
		process.exitCode = 1;
		return undefined;
	}
	// Pass the connection's ACTUAL dialect — without it the runner defaults to
	// sqlite and emits SQLite SQL (lock table, FK handling, DDL) against a
	// Postgres/MySQL database.
	return new MigrationRunner(toAdapter(db), {
		migrationsDir: options.migrationsDir,
		dialect: db.dialect,
		naturalSort: options.naturalSort,
		disableTransactions: options.disableTransactions,
	});
}

/** `migration:run` — apply every pending migration. */
export function migrationRunCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:run",
		description: "Run all pending migrations",
		async run(_args, flags) {
			const runner = resolveRunner(options);
			if (!runner) return;
			const ran = await runner.migrate();
			console.log(
				ran.length ? `Migrated: ${ran.join(", ")}` : "Already up to date",
			);
			await maybeRegenSchema(options, flags);
		},
	};
}

/**
 * `migration:rollback` — undo the latest batch, or with `--batch=N` roll back
 * everything applied after batch N (`--batch=0` rolls back all).
 */
export function migrationRollbackCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:rollback",
		description: "Roll back the latest batch (or --batch=N; --force in prod)",
		async run(_args, flags) {
			const runner = resolveRunner(options);
			if (!runner) return;
			const batch = parseBatchFlag(flags.batch);
			if (batch === "invalid") {
				console.error("[atlas] --batch must be a non-negative integer");
				process.exitCode = 1;
				return;
			}
			const force = isForced(flags);
			const rolled = await runner.rollback(
				batch === undefined ? { force } : { batch, force },
			);
			console.log(
				rolled.length
					? `Rolled back: ${rolled.join(", ")}`
					: "Nothing to roll back",
			);
			await maybeRegenSchema(options, flags);
		},
	};
}

/** `migration:status` — list every migration and whether it is applied. */
export function migrationStatusCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:status",
		description: "Show applied and pending migrations",
		async run() {
			const runner = resolveRunner(options);
			if (!runner) return;
			const rows = await runner.status();
			if (rows.length === 0) {
				console.log("No migrations found");
				return;
			}
			for (const row of rows) {
				const batch = row.batch === undefined ? "" : ` (batch ${row.batch})`;
				console.log(`${row.status.padEnd(8)} ${row.name}${batch}`);
			}
		},
	};
}

/** `migration:reset` — roll back every applied migration. */
export function migrationResetCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:reset",
		description: "Roll back all migrations (--force to override prod guard)",
		async run(_args, flags) {
			const runner = resolveRunner(options);
			if (!runner) return;
			const rolled = await runner.reset({ force: isForced(flags) });
			console.log(
				rolled.length
					? `Rolled back: ${rolled.join(", ")}`
					: "Nothing to reset",
			);
			await maybeRegenSchema(options, flags);
		},
	};
}

/** `migration:unlock` — force-clear a stuck migration lock (Lucid `migration:unlock`). */
export function migrationUnlockCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:unlock",
		description: "Force-clear a stuck migration lock",
		async run() {
			const runner = resolveRunner(options);
			if (!runner) return;
			const cleared = await runner.forceUnlock();
			console.log(
				cleared ? "Migration lock cleared" : "No migration lock was held",
			);
		},
	};
}

/** `migration:fresh` — drop every table, then re-run all migrations (Lucid `migration:fresh`). */
export function migrationFreshCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:fresh",
		description:
			"Drop all tables, then re-run every migration (--force to override prod guard)",
		async run(_args, flags) {
			const runner = resolveRunner(options);
			if (!runner) return;
			const { executed } = await runner.fresh({ force: isForced(flags) });
			console.log(
				executed.length
					? `Dropped all tables, re-ran: ${executed.join(", ")}`
					: "Dropped all tables (no migrations to run)",
			);
			await maybeRegenSchema(options, flags);
		},
	};
}

/** `migration:refresh` — roll everything back, then re-run all migrations. */
export function migrationRefreshCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "migration:refresh",
		description:
			"Roll back all migrations, then re-run them (--force to override prod guard)",
		async run(_args, flags) {
			const runner = resolveRunner(options);
			if (!runner) return;
			const { rolled, executed } = await runner.refresh({
				force: isForced(flags),
			});
			console.log(`Rolled back: ${rolled.length}, re-ran: ${executed.length}`);
			await maybeRegenSchema(options, flags);
		},
	};
}

/** `db:wipe` — drop every table, including the migrations bookkeeping table. */
export function dbWipeCommand(options: MigrationCommandOptions): AtlasCommand {
	return {
		name: "db:wipe",
		description:
			"Drop all tables including the migrations table (--force to override prod guard)",
		async run(_args, flags) {
			const runner = resolveRunner(options);
			if (!runner) return;
			await runner.wipe({ force: isForced(flags) });
			console.log("Dropped all tables");
		},
	};
}

/** Scaffold body for a fresh migration (`make:migration`). */
const MIGRATION_STUB = `import { Migration } from '@c9up/atlas'

export default class extends Migration {
  async up() {
    // this.schema.createTable('table_name', (table) => {
    //   table.increments('id')
    // })
  }

  async down() {
    // this.schema.dropTable('table_name')
  }
}
`;

/**
 * `make:migration <name>` — scaffold a timestamped migration file in
 * `migrationsDir`. The `Date.now()` prefix keeps files in creation order under
 * the runner's lexicographic sort (same convention as AdonisJS/Lucid). The name
 * is validated (no path separators / traversal) and the file is written with
 * `wx` so an existing migration is never clobbered.
 */
export function makeMigrationCommand(
	options: MigrationCommandOptions,
): AtlasCommand {
	return {
		name: "make:migration",
		description: "Scaffold a new timestamped migration file",
		async run(args) {
			const name = args[0];
			if (!name) {
				console.error("[atlas] usage: make:migration <name>");
				process.exitCode = 1;
				return;
			}
			try {
				assertSafeName(name, "MIGRATION_INVALID", "migration");
			} catch {
				console.error(`[atlas] invalid migration name: ${name}`);
				process.exitCode = 1;
				return;
			}
			const fileName = `${Date.now()}_${name}.ts`;
			const filePath = path.join(options.migrationsDir, fileName);
			await fsp.mkdir(options.migrationsDir, { recursive: true });
			await fsp.writeFile(filePath, MIGRATION_STUB, { flag: "wx" });
			console.log(`Created ${filePath}`);
		},
	};
}

/** Whether `--force` was passed (bare `--force` or `--force=true`). */
function isForced(flags: Record<string, string | boolean>): boolean {
	return flags.force === true || flags.force === "true";
}

/**
 * Regenerate the schema file after a mutating migration command (Adonis Lucid's
 * post-migration `schema:generate`) — unless it's disabled, has no `outputPath`,
 * or the run passed `--no-schema-generate`.
 */
async function maybeRegenSchema(
	options: MigrationCommandOptions,
	flags: Record<string, string | boolean>,
): Promise<void> {
	const cfg = options.schemaGeneration;
	if (!cfg?.enabled || !cfg.outputPath) return;
	if (
		flags["no-schema-generate"] === true ||
		flags["no-schema-generate"] === "true"
	) {
		return;
	}
	const db = getDb();
	if (!db) return;
	const n = await generateSchemaFile(db, cfg);
	console.log(
		`Regenerated ${cfg.outputPath} (${n} table${n === 1 ? "" : "s"})`,
	);
}

/**
 * Parse the `--batch` flag: absent → undefined (default rollback), a
 * non-negative integer string → that number, anything else → `"invalid"`.
 */
function parseBatchFlag(
	value: string | boolean | undefined,
): number | undefined | "invalid" {
	if (value === undefined || value === true) return undefined;
	if (value === false) return "invalid";
	const n = Number(value);
	return Number.isInteger(n) && n >= 0 ? n : "invalid";
}
