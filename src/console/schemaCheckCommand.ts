/**
 * `atlas:check` console command — the Ream-idiomatic CLI for schema
 * verification. Ream commands are classes carrying their name, description and
 * inputs as statics; the console kernel discovers them in `commands/` or reads
 * them from `reamrc.commands`.
 *
 * Stays framework-agnostic: resolves the live connection + dialect from atlas's
 * OWN service locators (`getDb` / `getAtlasDialect`), never importing
 * `@c9up/ream`. The {@link AtlasCommandClass} shape structurally matches Ream's
 * command contract, so the console kernel accepts it without a type dependency
 * in either direction.
 */

import { getAtlasDialect } from "../query/native.js";
import { runSchemaCheck } from "../schema/SchemaCheck.js";
import { getDb } from "../services/db.js";
import { type AtlasCommandClass, flag } from "./contract.js";

type Constructor = new (...args: unknown[]) => unknown;

export type { AtlasCommandClass } from "./contract.js";

/**
 * Build the `atlas:check` command for the given models. Register it in
 * `reamrc.commands` (atlas has no global entity registry — list your models,
 * as in Lucid). Run it as `ream atlas:check`; `--warn` reports drift without a
 * non-zero exit (useful for an advisory CI step).
 *
 * @example
 *   // commands/atlas-check.ts
 *   import { schemaCheckCommand } from '@c9up/atlas'
 *   import { User } from '#models/user'
 *   export default schemaCheckCommand([User])
 *
 *   // run:  ream atlas:check --warn
 */
export function schemaCheckCommand(
	entities: readonly Constructor[],
): AtlasCommandClass {
	return class SchemaCheck {
		static commandName = "atlas:check";
		static description = "Verify models match the live database schema";
		static options = { startApp: true };
		static flags = [
			flag("warn", "boolean", {
				description: "Report drift without failing (exit 0)",
			}),
		];

		declare warn: boolean;

		async run(): Promise<void> {
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
			if (code !== 0 && !this.warn) process.exitCode = code;
		}
	};
}
