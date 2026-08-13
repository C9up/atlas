/**
 * `schema:dump` — serialise the live database's schema to a `.sql` dump + a
 * `.meta.json` manifest (Adonis Lucid `schema:dump` / `SchemaDumper`), so a
 * fresh database can be rebuilt from the dump via `migration:run --schema-path`
 * instead of replaying every migration.
 *
 * @example
 *   // commands/schema-dump.ts
 *   import { schemaDumpCommand } from '@c9up/atlas'
 *   export default schemaDumpCommand({ migrationsDir: 'database/migrations' })
 *   // run:  <console-entry> schema:dump            → database/schema/default-schema.sql
 *          //                 schema:dump --prune   → also squashes the migration files
 */

import { SchemaDumper } from "../schema/SchemaDumper.js";
import { getConnection, getDb } from "../services/db.js";
import { type AtlasCommandClass, flag } from "./contract.js";

export interface SchemaDumpCommandOptions {
	/** Directory the dump + manifest are written to. Default `"database/schema"`. */
	outputDir?: string;
	/** Migration directory — required for `--prune` (the files it collapses). */
	migrationsDir?: string;
	/** Migration bookkeeping table name. Default `"ream_migrations"`. */
	schemaTableName?: string;
	/**
	 * Timestamp string for the manifest. `Date.now()`/`new Date()` are unavailable
	 * in some atlas contexts, so pass one in when you need a deterministic value.
	 */
	generatedAt?: string;
}

/** `schema:dump` — dump the schema; `--prune` squashes migrations, `--connection` targets one. */
export function schemaDumpCommand(
	options: SchemaDumpCommandOptions = {},
): AtlasCommandClass {
	return class SchemaDump {
		static commandName = "schema:dump";
		static description =
			"Dump the database schema to a .sql file + manifest (--prune, --connection, --path)";
		static options = { startApp: true };
		static flags = [
			flag("connection", "string", {
				description: "Named connection to dump (defaults to the primary one)",
			}),
			flag("prune", "boolean", {
				description: "Squash the migrations the dump replaces",
			}),
			flag("path", "string", {
				description: "Destination .sql file (not a directory)",
			}),
		];

		declare connection?: string;
		declare prune: boolean;
		declare path?: string;

		async run(): Promise<void> {
			const connName = this.connection;
			const db = connName ? getConnection(connName) : getDb();
			if (!db) {
				console.error(
					connName
						? `[atlas] no connection registered under '${connName}'`
						: "[atlas] no database connection — is AtlasProvider registered?",
				);
				process.exitCode = 1;
				return;
			}
			// Typed by the kernel now — no string/boolean coercion to redo here.
			const prune = this.prune === true;
			// `--path <file>` is the SQL dump FILE path (Adonis Lucid), not a dir.
			const dumpPath = this.path;
			const dumper = new SchemaDumper(db, {
				connectionName: connName ?? "default",
				dumpPath,
				outputDir: options.outputDir,
				migrationsDir: options.migrationsDir,
				schemaTableName: options.schemaTableName,
				prune,
				generatedAt: options.generatedAt,
			});
			await dumper.run();
			if (dumper.error) {
				console.error(`[atlas] schema:dump failed: ${dumper.error.message}`);
				process.exitCode = 1;
				return;
			}
			const r = dumper.result;
			console.log(
				`Dumped ${r?.tableCount ?? 0} table(s) → ${r?.dumpPath}` +
					(prune ? " (migrations squashed)" : ""),
			);
		}
	};
}
