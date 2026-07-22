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
import type { AtlasCommand } from "./schemaCheckCommand.js";

export interface SeederCommandOptions {
	/** Directory holding the seeder files. */
	seedersDir: string;
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
export function makeSeederCommand(options: SeederCommandOptions): AtlasCommand {
	return {
		name: "make:seeder",
		description: "Scaffold a new seeder file",
		async run(args) {
			const name = args[0];
			if (!name) {
				console.error("[atlas] usage: make:seeder <name>");
				process.exitCode = 1;
				return;
			}
			try {
				assertSafeName(name, "SEEDER_INVALID", "seeder");
			} catch {
				console.error(`[atlas] invalid seeder name: ${name}`);
				process.exitCode = 1;
				return;
			}
			const fileName = `${Date.now()}_${name}.ts`;
			const filePath = path.join(options.seedersDir, fileName);
			await fsp.mkdir(options.seedersDir, { recursive: true });
			await fsp.writeFile(filePath, SEEDER_STUB, { flag: "wx" });
			console.log(`Created ${filePath}`);
		},
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
export function dbSeedCommand(options: SeederCommandOptions): AtlasCommand {
	return {
		name: "db:seed",
		description: "Run database seeders (--files=A,B, --connection=name)",
		async run(_args, flags) {
			const connName =
				typeof flags.connection === "string" ? flags.connection : undefined;
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
			if (flags.interactive === true || flags.interactive === "true") {
				console.warn(
					"[atlas] --interactive is not supported in the non-interactive console; running all selected seeders.",
				);
			}
			const files = parseFilesFlag(flags.files);
			const executed = await runSeederDirectory(
				options.seedersDir,
				db,
				files ? { files } : undefined,
			);
			const compact =
				flags["compact-output"] === true || flags["compact-output"] === "true";
			if (compact) {
				console.log(`Seeded ${executed.length} seeder(s)`);
			} else {
				console.log(
					executed.length
						? `Seeded: ${executed.join(", ")}`
						: "No seeders to run",
				);
			}
		},
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
