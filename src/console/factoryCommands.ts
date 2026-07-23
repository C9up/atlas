/**
 * Factory console command — `make:factory` (Adonis Lucid). Scaffolds a model
 * factory file. Same shape as the migration/seeder commands: a plain
 * `{ name, description, run }` object registered in `reamrc.commands`.
 *
 * @example
 *   // commands/make-factory.ts
 *   import { makeFactoryCommand } from '@c9up/atlas'
 *   export default makeFactoryCommand({ factoriesDir: 'database/factories' })
 *   // run:  <console-entry> make:factory User
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { assertSafeName } from "../utils/safePath.js";
import type { AtlasCommand } from "./schemaCheckCommand.js";

export interface FactoryCommandOptions {
	/** Directory the factory files are scaffolded into. */
	factoriesDir: string;
}

/** Scaffold body for a fresh factory (`make:factory <Model>`). */
function factoryStub(model: string): string {
	return `import { factory } from '@c9up/atlas'
import ${model} from '#models/${model.toLowerCase()}'

export const ${model}Factory = factory(${model}, ({ faker }) => ({
  // email: faker.internet.email(),
}))
`;
}

/**
 * `make:factory <Model>` — scaffold `<Model>Factory.ts` in `factoriesDir`. The
 * name is validated (no path separators / traversal) and written with `wx` so an
 * existing factory is never clobbered.
 */
export function makeFactoryCommand(
	options: FactoryCommandOptions,
): AtlasCommand {
	return {
		name: "make:factory",
		description: "Scaffold a new model factory file",
		async run(args) {
			const model = args[0];
			if (!model) {
				console.error("[atlas] usage: make:factory <Model>");
				process.exitCode = 1;
				return;
			}
			const fileName = `${model}Factory.ts`;
			try {
				assertSafeName(fileName, "FACTORY_INVALID", "factory");
			} catch {
				console.error(`[atlas] invalid factory name: ${model}`);
				process.exitCode = 1;
				return;
			}
			const filePath = path.join(options.factoriesDir, fileName);
			await fsp.mkdir(options.factoriesDir, { recursive: true });
			await fsp.writeFile(filePath, factoryStub(model), { flag: "wx" });
			console.log(`Created ${filePath}`);
		},
	};
}
