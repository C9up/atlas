import path from "node:path";

/**
 * The extensions a migration or a seeder may have — the upstream script-file
 * rule (js/mjs/cjs/ts, never `.d.ts`). `.json`, which that rule also accepts,
 * is left out: it cannot default-export a class. One rule for the migration
 * runner, `schema:dump --prune` and the seeders, so a file one of them runs is
 * never a file another ignores.
 */
export const SCRIPT_EXTENSIONS = [".ts", ".js", ".mjs", ".cjs"];

/** Whether `file` is a migration/seeder source file. */
export function isScriptFile(file: string): boolean {
	if (file.endsWith(".d.ts")) return false;
	return SCRIPT_EXTENSIONS.includes(path.extname(file));
}

/** A script file's name without its extension (`1_users.mjs` → `1_users`). */
export function scriptName(file: string): string {
	return isScriptFile(file) ? file.slice(0, -path.extname(file).length) : file;
}
