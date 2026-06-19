/**
 * `atlas:check` console command — the Ream-idiomatic CLI for schema
 * verification. Ream commands are plain `{ name, description, run }` objects
 * registered in `reamrc.commands` and dispatched by the console kernel
 * (`new Ignitor(...).console().handle(argv)`).
 *
 * Stays framework-agnostic: resolves the live connection + dialect from atlas's
 * OWN service locators (`getDb` / `getAtlasDialect`), never importing
 * `@c9up/ream`. The {@link AtlasCommand} shape structurally matches Ream's
 * `Command` interface, so the console kernel accepts it without a type
 * dependency in either direction.
 */

import { getAtlasDialect } from "../query/native.js";
import { runSchemaCheck } from "../schema/SchemaCheck.js";
import { getDb } from "../services/db.js";

type Constructor = new (...args: unknown[]) => unknown;

/** Structural match of Ream's console `Command` (no `@c9up/ream` import). */
export interface AtlasCommand {
	name: string;
	description: string;
	run(args: string[], flags: Record<string, string | boolean>): Promise<void>;
}

/**
 * Build the `atlas:check` command for the given models. Register it in
 * `reamrc.commands` (atlas has no global entity registry — list your models,
 * as in Lucid). Run it via the console kernel; `--warn` reports drift without a
 * non-zero exit (useful for an advisory CI step).
 *
 * @example
 *   // commands/atlas-check.ts
 *   import { schemaCheckCommand } from '@c9up/atlas'
 *   import { User } from '#models/user'
 *   export default schemaCheckCommand([User])
 *
 *   // reamrc.ts → commands: [() => import('./commands/atlas-check.js')]
 *   // run:  <console-entry> atlas:check
 */
export function schemaCheckCommand(
	entities: readonly Constructor[],
): AtlasCommand {
	return {
		name: "atlas:check",
		description: "Verify models match the live database schema",
		async run(_args, flags) {
			const db = getDb();
			if (!db) {
				console.error(
					"[atlas:check] no database connection — is AtlasProvider registered?",
				);
				process.exitCode = 1;
				return;
			}
			const code = await runSchemaCheck(entities, db, getAtlasDialect());
			// `--warn` downgrades drift to advisory (exit 0); default fails CI.
			if (code !== 0 && !flags.warn) process.exitCode = code;
		},
	};
}
