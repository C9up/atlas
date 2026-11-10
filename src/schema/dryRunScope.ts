/**
 * The dry-run scope: while a migration dry run evaluates `up()`, nothing may
 * write to a database.
 *
 * `dryRun()` used to run each `up()` with the live connection in reach, so an
 * `up()` that wrote through the application's database (`db.table().insert()`,
 * a raw statement, `execute`, a transaction) wrote for real while the caller
 * believed it was previewing SQL. Lucid collects the schema calls without
 * running them but still hands the migration its real query client; atlas
 * refuses every write for the duration instead — reads of the catalog
 * (`schema.hasTable()`) keep working.
 *
 * The scope follows the async call chain (AsyncLocalStorage), so it covers any
 * connection the migration reaches, and nothing outside the dry run.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { AtlasError } from "../errors.js";

const scope = new AsyncLocalStorage<true>();

/** Run `fn` as part of a dry run: every write inside it is refused. */
export function runAsDryRun<T>(fn: () => Promise<T>): Promise<T> {
	return scope.run(true, fn);
}

/** Whether the current async context is inside a dry run. */
export function inDryRun(): boolean {
	return scope.getStore() === true;
}

const DML_WORD = /\b(insert|update|delete|merge|truncate|upsert|replace)\b/i;

/** The statement without its comments, string literals and quoted names. */
function statementSkeleton(sql: string): string {
	return sql
		.replace(/\/\*[\s\S]*?\*\//g, " ")
		.replace(/--[^\n]*/g, " ")
		.replace(/'(?:[^']|'')*'/g, "''")
		.replace(/"(?:[^"]|"")*"/g, '""')
		.replace(/`(?:[^`]|``)*`/g, "``")
		.trim();
}

/**
 * Whether a statement only reads: a single SELECT / WITH … SELECT / VALUES /
 * PRAGMA read / SHOW / DESCRIBE / EXPLAIN (not ANALYZE, which runs the
 * statement). A data-modifying CTE, `SELECT … INTO` and a PRAGMA assignment
 * are writes. Conservative: anything not recognised counts as a write.
 */
export function isReadOnlyStatement(sql: string): boolean {
	const skeleton = statementSkeleton(sql).replace(/^\(+\s*/, "");
	// One statement only — a second one after `;` is not inspected.
	if (skeleton.replace(/;\s*$/, "").includes(";")) return false;
	const word = /^[A-Za-z]+/.exec(skeleton)?.[0]?.toUpperCase();
	switch (word) {
		case "SELECT":
			return !/\binto\b/i.test(skeleton) && !DML_WORD.test(skeleton);
		case "WITH":
		case "VALUES":
			return !DML_WORD.test(skeleton) && !/\binto\b/i.test(skeleton);
		case "PRAGMA":
			return !skeleton.includes("=");
		case "SHOW":
		case "DESCRIBE":
		case "DESC":
			return true;
		case "EXPLAIN":
			return !/^EXPLAIN\s+(\(\s*)?ANALYZE\b/i.test(skeleton);
		default:
			return false;
	}
}

/** The error a write meets during a dry run. */
export function dryRunWriteError(operation: string, sql?: string): AtlasError {
	return new AtlasError(
		"E_ATLAS_DRY_RUN_WRITE",
		`A migration dry run cannot write to the database (${operation}${sql ? `: ${sql}` : ""}).`,
		{
			hint: "Check `this.dryRun` in up()/down() to skip side effects while the SQL is only being previewed.",
		},
	);
}

/** Refuse `operation` when it runs inside a dry run. */
export function assertNotDryRun(operation: string, sql?: string): void {
	if (inDryRun()) throw dryRunWriteError(operation, sql);
}

/** Refuse a statement that is not read-only when it runs inside a dry run. */
export function assertReadOnlyInDryRun(sql: string): void {
	if (inDryRun() && !isReadOnlyStatement(sql)) assertNotDryRun("query", sql);
}
