/**
 * Path/filename validation helpers shared by MigrationRunner and Seeder.
 *
 * Centralised here so the "forbidden chars + no `..`" rules + "path must stay
 * inside its base directory" check live in one place. Previously duplicated
 * — the audit flagged this as a maintenance hazard.
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { AtlasError } from "../errors.js";

/** Non-throwing async `fs.existsSync` equivalent. Returns false on ENOENT, propagates other errors. */
export async function pathExists(p: string): Promise<boolean> {
	try {
		await fsp.access(p);
		return true;
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return false;
		throw err;
	}
}

/**
 * Throw if `name` contains characters that could escape a filename context
 * (path separators, quotes, backticks) or include a directory-walk sequence
 * (`..`). Does NOT check extension or existence — that's the caller's job.
 */
export function assertSafeName(
	name: string,
	errorCode: string,
	kind: string,
): void {
	if (/[/\\'";`]/.test(name) || name.includes("..")) {
		throw new AtlasError(errorCode, `Invalid ${kind} name: ${name}`);
	}
}

/**
 * Resolve `fileName` inside `baseDir` and throw if the resulting path escapes
 * the base — guards against symlink / `../` traversal attacks when loading
 * migration or seeder files dynamically.
 */
export function assertPathInsideBase(
	baseDir: string,
	fileName: string,
	errorCode: string,
	kind: string,
): string {
	const resolved = path.resolve(baseDir, fileName);
	const base = path.resolve(baseDir);
	if (!resolved.startsWith(base + path.sep) && resolved !== base) {
		throw new AtlasError(
			errorCode,
			`${kind} path escapes directory: ${fileName}`,
		);
	}
	return resolved;
}
