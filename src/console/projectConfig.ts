/**
 * Reading `config/database.ts` when the application has not booted.
 *
 * The `make:*` commands are `startApp: false` on purpose — scaffolding a
 * migration must not need a reachable database, which is the whole reason you
 * are writing one. But the directory to write into comes from
 * `config/database.ts`, and that config only reaches memory when
 * `AtlasProvider` boots. So the commands meant to run without the application
 * were the ones that could not see its configuration, and wrote to the default
 * path while the project's own was in the file next door.
 *
 * Importing the config module is what Lucid's equivalent does through the
 * container; here it is read straight off disk, because there is no container
 * to ask. It evaluates the module — which reads environment variables and
 * returns an object — and opens nothing.
 */

import * as path from "node:path";
import { pathToFileURL } from "node:url";
import type { ConnectionConfig } from "../AtlasProvider.js";

/** What a `config/database.ts` default-exports. */
interface DatabaseConfigModule {
	connection?: string;
	connections?: Record<string, ConnectionConfig>;
	migrations?: ConnectionConfig["migrations"];
	seeders?: ConnectionConfig["seeders"];
}

/** Where a path came from, so a caller can say it rather than guess. */
export interface ResolvedPath {
	dir: string;
	/** `"config"` when the project said so, `"default"` when nothing did. */
	source: "config" | "default";
	/** Why the config could not be read, when it could not. */
	problem?: string;
}

let cached: DatabaseConfigModule | null | undefined;

/** Forget the config read from disk. Test helper. */
export function forgetProjectConfig(): void {
	cached = undefined;
}

/**
 * The project's `config/database.ts`, or `null` when there is none to read.
 *
 * Read once: a command line runs one command, and re-importing per lookup
 * would evaluate the module several times for one answer.
 */
export async function projectDatabaseConfig(
	cwd = process.cwd(),
): Promise<{ config: DatabaseConfigModule | null; problem?: string }> {
	if (cached !== undefined) return { config: cached };
	const file = path.join(cwd, "config", "database.ts");
	try {
		const module: unknown = await import(pathToFileURL(file).href);
		const value = (module as { default?: unknown }).default;
		cached =
			typeof value === "object" && value !== null
				? (value as DatabaseConfigModule)
				: null;
		return { config: cached };
	} catch (error) {
		cached = null;
		// Reported, not swallowed: a project whose config throws gets the default
		// path AND the reason, rather than a file quietly written elsewhere.
		return {
			config: null,
			problem: error instanceof Error ? error.message : String(error),
		};
	}
}

/** The connection's config in a module read off disk, merged like the runtime's. */
function connectionIn(
	config: DatabaseConfigModule,
	name?: string,
): ConnectionConfig {
	const resolved = name ?? config.connection;
	const declared =
		resolved === undefined ? undefined : config.connections?.[resolved];
	return { ...(config as ConnectionConfig), ...(declared ?? {}) };
}

/**
 * The directory a `make:*` command should write into.
 *
 * @param pick which key of the connection config holds the paths
 * @param fallback the path used when nothing names one
 * @param connection the connection to read, for a project with several
 */
export async function resolveDir(
	pick: "migrations" | "seeders",
	fallback: string,
	connection?: string,
	cwd = process.cwd(),
): Promise<ResolvedPath> {
	const { config, problem } = await projectDatabaseConfig(cwd);
	if (config === null) {
		return {
			dir: fallback,
			source: "default",
			...(problem ? { problem } : {}),
		};
	}
	const entry = connectionIn(config, connection);
	const paths = entry[pick];
	const named = paths?.paths?.[0] ?? (paths as { path?: string })?.path;
	if (typeof named === "string" && named.length > 0) {
		return { dir: named, source: "config" };
	}
	// A named connection that does not exist is a typo worth saying out loud —
	// writing to the default would look like it worked.
	if (
		connection !== undefined &&
		config.connections?.[connection] === undefined
	) {
		return {
			dir: fallback,
			source: "default",
			problem: `config/database.ts declares no connection named "${connection}" (${Object.keys(config.connections ?? {}).join(", ") || "none"})`,
		};
	}
	return { dir: fallback, source: "default" };
}
