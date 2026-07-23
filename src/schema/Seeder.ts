/**
 * Seeder — populate the database with default / reference / test data.
 *
 * The canonical pattern (idempotent via `upsert`, keyed on a unique column):
 *
 *     export default class CountrySeeder extends BaseSeeder {
 *       async run() {
 *         const countries = new BaseRepository(Country, this.db)
 *         // Conflict on isoCode → update name. Safe to re-run.
 *         await countries.upsert(
 *           [
 *             { isoCode: 'FR', name: 'France' },
 *             { isoCode: 'IN', name: 'India' },
 *           ],
 *           ['isoCode'],
 *           ['name'],
 *         )
 *       }
 *     }
 *
 * @implements MISS-4, Story 32.12
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import type { DatabaseConnection } from "../BaseRepository.js";
import { AtlasError } from "../errors.js";
import {
	assertPathInsideBase,
	assertSafeName,
	pathExists,
} from "../utils/safePath.js";

/**
 * Base class for all seeders.
 *
 * Subclasses receive the database connection via the constructor so they can
 * build repositories inside `run()` without resorting to globals.
 */
export abstract class BaseSeeder {
	protected db: DatabaseConnection;

	/**
	 * Environments this seeder is allowed to run in (Adonis Lucid
	 * `static environment`). When set, `runSeederDirectory({ environment })`
	 * skips it unless the current environment is listed. Unset = every env.
	 */
	static environment?: string[];

	constructor(db: DatabaseConnection) {
		this.db = db;
	}

	/** Alias of {@link db} — Adonis Lucid seeders expose the connection as `this.client`. */
	protected get client(): DatabaseConnection {
		return this.db;
	}

	/** The seeder body. Should be idempotent — `repo.upsert(...)` is the recommended pattern. */
	abstract run(): Promise<void> | void;
}

/** Legacy alias kept for code using the previous API. */
export const Seeder = BaseSeeder;

/**
 * Run a pre-built list of seeder INSTANCES in order. Each `run()` is awaited
 * sequentially so ordering is deterministic and side effects are visible to
 * subsequent seeders.
 */
export async function runSeeders(seeders: BaseSeeder[]): Promise<void> {
	for (const seeder of seeders) {
		await seeder.run();
	}
}

/**
 * Discover seeder files under `dir`, import them in alphabetical order, build
 * instances with the given DB connection, and run them sequentially.
 *
 * Usage: `await runSeederDirectory('./database/seeders', db)`
 *
 * @param dir      Directory containing `.ts` / `.js` seeder files
 * @param db       Database connection passed to each seeder constructor
 * @param options  Optional filter (`files: ['CountrySeeder']`) to run a subset
 *
 * @implements Story 32.12
 */
export async function runSeederDirectory(
	dir: string,
	db: DatabaseConnection,
	options?: {
		/** Run only these seeders, by base name OR full/relative file path (Lucid `--files`). */
		files?: readonly string[];
		/** Sort files numerically (`2_x` before `10_x`) instead of lexicographically (Lucid `naturalSort`). */
		naturalSort?: boolean;
		/** The current environment — skips a seeder whose `static environment` excludes it (Lucid). */
		environment?: string;
	},
): Promise<string[]> {
	if (!(await pathExists(dir))) {
		throw new AtlasError(
			"E_SEEDER_DIR_NOT_FOUND",
			`Seeder directory not found: ${dir}`,
			{
				hint: "Create the directory or adjust the path passed to runSeederDirectory.",
			},
		);
	}

	const files = (await fsp.readdir(dir)).filter(
		(f) => (f.endsWith(".ts") || f.endsWith(".js")) && !f.endsWith(".d.ts"),
	);
	// naturalSort compares embedded numbers by value; the default is lexicographic.
	const allFiles = options?.naturalSort
		? files.sort((a, b) =>
				a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }),
			)
		: files.sort();

	// `--files` matches a base name (`UserSeeder`) OR a full/relative path
	// (`database/seeders/UserSeeder.ts`) — take each entry's basename sans ext.
	const wanted = options?.files?.map((f) =>
		path.basename(f).replace(/\.(ts|js)$/, ""),
	);
	const selected = wanted
		? allFiles.filter((f) => wanted.includes(f.replace(/\.(ts|js)$/, "")))
		: allFiles;

	const executed: string[] = [];
	for (const file of selected) {
		assertSafeName(file, "E_SEEDER_INVALID", "seeder");
		const resolved = await assertPathInsideBase(
			dir,
			file,
			"E_SEEDER_INVALID_PATH",
			"Seeder",
		);

		// `pathToFileURL` is required on Windows where bare `C:\…` paths
		// trip ESM's ERR_UNSUPPORTED_ESM_URL_SCHEME on dynamic import.
		const mod = await import(pathToFileURL(resolved).href);
		const SeederClass = mod.default;
		if (!SeederClass || typeof SeederClass !== "function") {
			throw new AtlasError(
				"E_SEEDER_INVALID",
				`Seeder ${file} must export a default class extending BaseSeeder`,
				{
					hint: "Example: `export default class UserSeeder extends BaseSeeder { async run() { ... } }`",
				},
			);
		}

		// Skip a seeder whose `static environment` excludes the current one (Lucid).
		const allowed: unknown = SeederClass.environment;
		if (
			options?.environment &&
			Array.isArray(allowed) &&
			!allowed.includes(options.environment)
		) {
			continue;
		}

		const instance: BaseSeeder = new SeederClass(db);
		await instance.run();
		executed.push(file.replace(/\.(ts|js)$/, ""));
	}

	return executed;
}
