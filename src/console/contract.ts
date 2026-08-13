/**
 * The console command contract, declared locally.
 *
 * Atlas stays framework-agnostic: it must not import `@c9up/ream`, so it
 * describes the shape Ream's console kernel dispatches against rather than
 * importing it. Same reasoning as the previous `AtlasCommand` interface — only
 * the shape changed, from a plain object to a class carrying its inputs as
 * metadata.
 *
 * Ream's decorators (`@args` / `@flags`) live in the framework, so the helpers
 * below build the same metadata without them.
 */

export interface CommandOptions {
	/** Boot the application before `run()`. Off by default. */
	startApp?: boolean;
	staysAlive?: boolean;
	allowUnknownFlags?: boolean;
}

export interface ArgumentMetaData {
	type: "string" | "spread";
	propertyName: string;
	argumentName: string;
	description?: string;
	required: boolean;
	default?: string | string[];
}

export interface FlagMetaData {
	type: "string" | "boolean" | "number" | "array";
	propertyName: string;
	flagName: string;
	description?: string;
	alias: string[];
	default?: string | string[] | number | boolean;
	required: boolean;
}

/** The static side the kernel reads. */
export interface AtlasCommandClass {
	new (): { run(): Promise<void> | void };
	commandName: string;
	description: string;
	options?: CommandOptions;
	args?: readonly ArgumentMetaData[];
	flags?: readonly FlagMetaData[];
	help?: string | string[];
}

/** `startServer` → `start-server`, matching the framework's decorators. */
function dashCase(value: string): string {
	return value.replace(/([a-z0-9])([A-Z])/g, "$1-$2").toLowerCase();
}

export function flag(
	propertyName: string,
	type: FlagMetaData["type"],
	options: {
		flagName?: string;
		description?: string;
		alias?: string[];
		default?: FlagMetaData["default"];
		required?: boolean;
	} = {},
): FlagMetaData {
	return {
		type,
		propertyName,
		flagName: options.flagName ?? dashCase(propertyName),
		description: options.description,
		alias: options.alias ?? [],
		default: options.default,
		required: options.required ?? false,
	};
}

export function argument(
	propertyName: string,
	options: {
		type?: ArgumentMetaData["type"];
		argumentName?: string;
		description?: string;
		required?: boolean;
		default?: ArgumentMetaData["default"];
	} = {},
): ArgumentMetaData {
	return {
		type: options.type ?? "string",
		propertyName,
		argumentName: options.argumentName ?? dashCase(propertyName),
		description: options.description,
		required: options.required ?? options.default === undefined,
		default: options.default,
	};
}
