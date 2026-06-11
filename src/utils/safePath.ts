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
 *
 * Two layers: (1) a logical `path.resolve` check that catches `../` walks, and
 * (2) a `realpath` check that follows symlinks — a symlink FILE sitting inside
 * the base but pointing at `/etc/passwd` passes the logical check yet is caught
 * here. When the file doesn't exist yet (ENOENT), only the logical check
 * applies and the caller's own existence check produces the not-found error.
 */
export async function assertPathInsideBase(
	baseDir: string,
	fileName: string,
	errorCode: string,
	kind: string,
): Promise<string> {
	const resolved = path.resolve(baseDir, fileName);
	const base = path.resolve(baseDir);
	if (!resolved.startsWith(base + path.sep) && resolved !== base) {
		throw new AtlasError(
			errorCode,
			`${kind} path escapes directory: ${fileName}`,
		);
	}
	// Symlink-aware check: resolve real targets and re-verify containment.
	try {
		const realBase = await fsp.realpath(base);
		const realResolved = await fsp.realpath(resolved);
		if (
			!realResolved.startsWith(realBase + path.sep) &&
			realResolved !== realBase
		) {
			throw new AtlasError(
				errorCode,
				`${kind} path escapes directory via symlink: ${fileName}`,
			);
		}
	} catch (err) {
		// File not yet created — the logical check above stands; let the caller's
		// existence check report the missing file. Re-throw real traversal errors.
		if (err instanceof AtlasError) throw err;
		if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
	}
	return resolved;
}
