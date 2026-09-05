/**
 * Seeder console commands — the Ream-idiomatic CLI for scaffolding and running
 * database seeders (Adonis Lucid `make:seeder` / `db:seed`). Same shape as the
 * migration commands: plain `{ name, description, run }` objects registered in
 * `reamrc.commands` and dispatched by the console kernel.
 *
 * Atlas has no global config registry (Lucid parity — you pass your own paths),
 * so every factory takes the `seedersDir`.
 *
 * @example
 *   // commands/seed.ts
 *   import { dbSeedCommand } from '@c9up/atlas'
 *   export default dbSeedCommand({ seedersDir: 'database/seeders' })
 *   // run:  <console-entry> db:seed  (or --files=UserSeeder,PostSeeder)
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { runSeederDirectory } from "../schema/Seeder.js";
import { getConnection, getDb } from "../services/db.js";
import { assertSafeName } from "../utils/safePath.js";
import { type AtlasCommandClass, argument, flag } from "./contract.js";
import { resolveDir } from "./projectConfig.js";

export interface SeederCommandOptions {
	/** Directory holding the seeder files. */
	seedersDir: string;
	/** Sort files numerically (`2_x` before `10_x`) — Adonis Lucid `naturalSort`. */
	naturalSort?: boolean;
	/**
	 * Current environment (`development`/`testing`/`production`), used to skip a
	 * seeder whose `static environment` excludes it. Falls back to `NODE_ENV`;
	 * override per-run with `--environment`.
	 */
	environment?: string;
}

/** Scaffold body for a fresh seeder (`make:seeder`). */
const SEEDER_STUB = `import { BaseSeeder } from '@c9up/atlas'

export default class extends BaseSeeder {
  async run() {
    // const repo = new BaseRepository(Model, this.db)
    // await repo.upsert([{ /* ... */ }], ['uniqueColumn'], ['columnToUpdate'])
  }
}
`;

/**
 * `make:seeder <name>` — scaffold a timestamped seeder file in `seedersDir`. The
 * `Date.now()` prefix keeps files in creation order under the runner's
 * lexicographic sort (same convention as migrations). The name is validated (no
 * path separators / traversal) and written with `wx` so an existing seeder is
 * never clobbered.
 */
export function makeSeederCommand(
	options: SeederCommandOptions,
): AtlasCommandClass {
	return class MakeSeeder {
		static commandName = "make:seeder";
		static description = "Scaffold a new seeder file";
		// Filesystem only — no connection needed.
		static options = { startApp: false };
		static args = [argument("name", { description: "Seeder file name" })];
		static flags = [
			flag("connection", "string", {
				description:
					"Which connection's seeders directory to write into (config/database.ts)",
			}),
		];

		declare name: string;
		declare connection?: string;

		async run(): Promise<void> {
			// A missing name never reaches here: the kernel reports the required
			// argument by name before `run()`.
			try {
				assertSafeName(this.name, "SEEDER_INVALID", "seeder");
			} catch {
				console.error(`[atlas] invalid seeder name: ${this.name}`);
				process.exitCode = 1;
				return;
			}
			const fileName = `${Date.now()}_${this.name}.ts`;
			// Same as `make:migration`: the application has not booted, so the
			// configured directory is read off disk rather than out of memory.
			const resolved = await resolveDir(
				"seeders",
				options.seedersDir,
				this.connection,
			);
			if (resolved.problem !== undefined) {
				console.error(`[atlas] ${resolved.problem}`);
				if (this.connection !== undefined) {
					process.exitCode = 1;
					return;
				}
			}
			const filePath = path.join(resolved.dir, fileName);
			await fsp.mkdir(resolved.dir, { recursive: true });
			await fsp.writeFile(filePath, SEEDER_STUB, { flag: "wx" });
			console.log(`Created ${filePath}`);
		}
	};
}

/**
 * `db:seed` — run every seeder in `seedersDir` (Adonis Lucid `db:seed`). Flags:
 *   --files=A,B         run only the named seeders (Lucid `--files`)
 *   --connection=name   run against a registered connection (Lucid `--connection`)
 *   --compact-output    terse output (one summary line, not per-seeder)
 *   --interactive       accepted for Lucid compat; the console kernel is
 *                       non-interactive, so it runs every seeder (no prompt)
 */
export function dbSeedCommand(
	options: SeederCommandOptions,
): AtlasCommandClass {
	return class DbSeed {
		static commandName = "db:seed";
		static description =
			"Run database seeders (--files=A,B, --connection=name)";
		static options = { startApp: true };
		static flags = [
			flag("files", "string", {
				description: "Comma-separated seeder names to run",
			}),
			flag("connection", "string", {
				description: "Named connection to seed",
			}),
			flag("compactOutput", "boolean", {
				description: "One summary line instead of per-seeder output",
			}),
			flag("environment", "string", {
				description: "Environment the seeders run under",
			}),
			flag("interactive", "boolean", {
				description: "Accepted for Lucid compatibility; runs every seeder",
			}),
		];

		declare files?: string;
		declare connection?: string;
		declare compactOutput: boolean;
		declare environment?: string;
		declare interactive: boolean;

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
			if (this.interactive === true) {
				console.warn(
					"[atlas] --interactive is not supported in the non-interactive console; running all selected seeders.",
				);
			}
			const files = parseFilesFlag(this.files);
			const environment =
				this.environment ?? options.environment ?? process.env.NODE_ENV;
			const executed = await runSeederDirectory(options.seedersDir, db, {
				files,
				naturalSort: options.naturalSort,
				environment,
			});
			const compact = this.compactOutput === true;
			if (compact) {
				console.log(`Seeded ${executed.length} seeder(s)`);
			} else {
				console.log(
					executed.length
						? `Seeded: ${executed.join(", ")}`
						: "No seeders to run",
				);
			}
		}
	};
}

/** Parse `--files=A,B,C` into a name list, or undefined when absent. */
function parseFilesFlag(
	value: string | boolean | undefined,
): string[] | undefined {
	if (typeof value !== "string" || value.length === 0) return undefined;
	return value
		.split(",")
		.map((f) => f.trim())
		.filter((f) => f.length > 0);
}
