/**
 * `@c9up/atlas/commands` — every command atlas ships, configured from
 * `config/database.ts`.
 *
 *   // reamrc.ts, written by `configure()`
 *   commands: [() => import('@c9up/atlas/commands')]
 *
 * This is the shape Adonis Lucid ships (`@adonisjs/lucid/commands`, a module
 * exporting `getMetaData` / `getCommand`), and it is the whole point: a package
 * adds commands by shipping them, never by a change to the `ream` binary.
 *
 * Where the paths come from is Lucid's answer too — the connection's config
 * (`migrations.paths`, `seeders.paths`, `schemaGeneration.outputPath`). Lucid
 * reads it through the container; atlas reads it from its own locator, which
 * the provider fills at boot, so nothing here imports `@c9up/ream`.
 *
 * The option objects below use getters on purpose. A command class is built
 * when this module is imported — before the application boots — while every
 * `options.x` is read inside `run()`, after it. A plain object would capture
 * the config that did not exist yet.
 */

import {
	connectionConfigFor,
	getDatabaseConfig,
	primaryConnectionName,
} from "../services/db.js";
import type { AtlasCommandClass } from "./contract.js";
import {
	type FactoryCommandOptions,
	makeFactoryCommand,
} from "./factoryCommands.js";
import {
	dbWipeCommand,
	type MigrationCommandOptions,
	makeMigrationCommand,
	migrationFreshCommand,
	migrationRefreshCommand,
	migrationResetCommand,
	migrationRollbackCommand,
	migrationRunCommand,
	migrationStatusCommand,
	migrationUnlockCommand,
} from "./migrationCommands.js";
import { schemaCheckCommand } from "./schemaCheckCommand.js";
import {
	type SchemaDumpCommandOptions,
	schemaDumpCommand,
} from "./schemaDumpCommand.js";
import {
	type SchemaGenerateOptions,
	schemaGenerateCommand,
} from "./schemaGenerateCommand.js";
import {
	dbSeedCommand,
	makeSeederCommand,
	type SeederCommandOptions,
} from "./seederCommands.js";

/** Lucid's defaults, for a config that leaves the paths out. */
const DEFAULT_MIGRATIONS_DIR = "database/migrations";
const DEFAULT_SEEDERS_DIR = "database/seeders";
const DEFAULT_FACTORIES_DIR = "database/factories";

/** The default connection's config — per-connection first, then the top level. */
function connection() {
	return connectionConfigFor();
}

function migrationsDir(): string {
	const migrations = connection().migrations;
	return migrations?.paths?.[0] ?? migrations?.path ?? DEFAULT_MIGRATIONS_DIR;
}

const migrationOptions: MigrationCommandOptions = {
	get migrationsDir() {
		return migrationsDir();
	},
	get naturalSort() {
		return connection().migrations?.naturalSort;
	},
	get disableTransactions() {
		return connection().migrations?.disableTransactions;
	},
	get tableName() {
		return connection().migrations?.tableName ?? connection().migrations?.table;
	},
	get disableRollbacksInProduction() {
		return connection().migrations?.disableRollbacksInProduction;
	},
	get schemaGeneration() {
		return connection().schemaGeneration;
	},
};

const seederOptions: SeederCommandOptions = {
	get seedersDir() {
		return connection().seeders?.paths?.[0] ?? DEFAULT_SEEDERS_DIR;
	},
	get naturalSort() {
		return connection().seeders?.naturalSort;
	},
};

const factoryOptions: FactoryCommandOptions = {
	get factoriesDir() {
		return connection().factories?.path ?? DEFAULT_FACTORIES_DIR;
	},
};

const schemaDumpOptions: SchemaDumpCommandOptions = {
	get migrationsDir() {
		return migrationsDir();
	},
	get schemaTableName() {
		return connection().migrations?.tableName ?? connection().migrations?.table;
	},
};

// `outputPath` has no sensible default — a command that guessed one would write
// a generated file into a directory the project never chose. The command
// reports the missing key itself; an empty string is what it checks for.
const schemaGenerateOptions: SchemaGenerateOptions = {
	get outputPath() {
		return connection().schemaGeneration?.outputPath ?? "";
	},
	get excludeTables() {
		return connection().schemaGeneration?.excludeTables;
	},
	get enabled() {
		return connection().schemaGeneration?.enabled;
	},
	get rulesPaths() {
		return connection().schemaGeneration?.rulesPaths;
	},
	get compact() {
		return connection().schemaGeneration?.compact;
	},
	get schemas() {
		return connection().schemaGeneration?.schemas;
	},
};

const COMMANDS: readonly AtlasCommandClass[] = [
	migrationRunCommand(migrationOptions),
	migrationRollbackCommand(migrationOptions),
	migrationStatusCommand(migrationOptions),
	migrationResetCommand(migrationOptions),
	migrationRefreshCommand(migrationOptions),
	migrationFreshCommand(migrationOptions),
	migrationUnlockCommand(migrationOptions),
	makeMigrationCommand(migrationOptions),
	dbWipeCommand(migrationOptions),
	dbSeedCommand(seederOptions),
	makeSeederCommand(seederOptions),
	makeFactoryCommand(factoryOptions),
	schemaDumpCommand(schemaDumpOptions),
	schemaGenerateCommand(schemaGenerateOptions),
	schemaCheckCommand(() => getDatabaseConfig()?.verifySchema?.entities ?? []),
];

/** What the kernel reads to list a command without importing it. */
interface CommandMetaData {
	commandName: string;
	namespace: string | null;
	description: string;
	help?: string | string[];
	aliases: string[];
	options: Record<string, unknown>;
	args: readonly unknown[];
	flags: readonly unknown[];
}

function serialize(command: AtlasCommandClass): CommandMetaData {
	const colon = command.commandName.indexOf(":");
	return {
		commandName: command.commandName,
		namespace: colon === -1 ? null : command.commandName.slice(0, colon),
		description: command.description,
		help: command.help,
		aliases: [],
		options: { ...command.options },
		args: command.args ?? [],
		flags: command.flags ?? [],
	};
}

export async function getMetaData(): Promise<CommandMetaData[]> {
	return COMMANDS.map(serialize);
}

export async function getCommand(
	metadata: CommandMetaData,
): Promise<AtlasCommandClass | null> {
	return (
		COMMANDS.find((command) => command.commandName === metadata.commandName) ??
		null
	);
}

/** The connection these commands run against — Lucid `db.primaryConnectionName`. */
export { primaryConnectionName };
