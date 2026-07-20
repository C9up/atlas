/**
 * Dialect-aware catalog queries — "which base tables exist right now".
 *
 * The single source of truth for listing user tables, shared by the migration
 * runner's drop/wipe and the test-time `truncateAll`. Each query hits the
 * dialect's own catalog (`sqlite_master`, `pg_tables`, `information_schema`);
 * they are framework introspection, not user queries, so they stay raw SQL.
 */

import type { AtlasDialect } from "../query/native.js";

/**
 * The minimal connection surface these helpers need — just `query`/`execute`.
 * Narrower than `AsyncDatabaseConnection` on purpose, so the migration runner's
 * `DatabaseAdapter` (which has no `dialect`/`ping`) satisfies it too.
 */
export interface CatalogConnection {
	query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
	): Promise<T[]>;
	// Return value is ignored — the two adapter shapes disagree on it
	// (`void` vs `{ rowsAffected }`), and these helpers only run it for effect.
	execute(sql: string, params?: unknown[]): Promise<unknown>;
}

/**
 * Prefix for framework-private tables (migrations bookkeeping), following the
 * same `ream_`-prefix convention as AdonisJS's `adonis_`.
 */
const FRAMEWORK_PREFIX = "ream_";

export interface ListTablesOptions {
	/**
	 * Include `ream_*` framework tables (the migrations table). Off by default
	 * so cleanup helpers leave the bookkeeping intact; `fresh()` turns it on
	 * because it re-creates the tracking table afterwards.
	 */
	includeFrameworkTables?: boolean;
}

/** List every base table in the current schema/database, for `dialect`. */
export async function listUserTables(
	db: CatalogConnection,
	dialect: AtlasDialect,
	options: ListTablesOptions = {},
): Promise<string[]> {
	let sql: string;
	switch (dialect) {
		case "sqlite":
			sql =
				"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
			break;
		case "postgres":
			sql =
				"SELECT tablename AS name FROM pg_tables WHERE schemaname = current_schema()";
			break;
		case "mysql":
			sql =
				"SELECT table_name AS name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'";
			break;
	}
	const rows = await db.query<{ name: unknown }>(sql);
	const names: string[] = [];
	for (const row of rows) {
		if (typeof row.name !== "string") continue;
		if (
			!options.includeFrameworkTables &&
			row.name.startsWith(FRAMEWORK_PREFIX)
		) {
			continue;
		}
		names.push(row.name);
	}
	return names;
}

/**
 * Does base table `name` exist right now, for `dialect`? The name is passed as a
 * bound parameter (never interpolated) so this is injection-safe on every
 * dialect. Backs `Schema.hasTable` (Adonis Lucid parity).
 */
export async function tableExists(
	db: CatalogConnection,
	dialect: AtlasDialect,
	name: string,
): Promise<boolean> {
	const ph = dialect === "postgres" ? "$1" : "?";
	let sql: string;
	switch (dialect) {
		case "sqlite":
			sql = `SELECT 1 FROM sqlite_master WHERE type='table' AND name = ${ph}`;
			break;
		case "postgres":
			sql = `SELECT 1 FROM pg_tables WHERE schemaname = current_schema() AND tablename = ${ph}`;
			break;
		case "mysql":
			sql = `SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ${ph} AND table_type = 'BASE TABLE'`;
			break;
	}
	const rows = await db.query(sql, [name]);
	return rows.length > 0;
}

/**
 * Does column `column` exist on table `table` right now, for `dialect`? Both
 * names are bound parameters. Backs `Schema.hasColumn` (Adonis Lucid parity).
 */
export async function columnExists(
	db: CatalogConnection,
	dialect: AtlasDialect,
	table: string,
	column: string,
): Promise<boolean> {
	if (dialect === "sqlite") {
		// PRAGMA introspection via the table-valued function form, which accepts a
		// bound table-name argument (unlike `PRAGMA table_info(x)`).
		const rows = await db.query(
			"SELECT 1 FROM pragma_table_info(?) WHERE name = ?",
			[table, column],
		);
		return rows.length > 0;
	}
	const [p1, p2] = dialect === "postgres" ? ["$1", "$2"] : ["?", "?"];
	const schemaPredicate =
		dialect === "postgres"
			? "table_schema = current_schema()"
			: "table_schema = DATABASE()";
	const rows = await db.query(
		`SELECT 1 FROM information_schema.columns WHERE ${schemaPredicate} AND table_name = ${p1} AND column_name = ${p2}`,
		[table, column],
	);
	return rows.length > 0;
}

/**
 * Run `fn` with foreign-key enforcement suspended, then restore it — the toggle
 * runs even if `fn` throws. Lets a bulk drop/truncate ignore inter-table FK
 * order. Postgres has no session-level switch (it uses `TRUNCATE … CASCADE` /
 * `DROP … CASCADE` instead), so this is a no-op there.
 */
export async function withoutForeignKeys<T>(
	db: CatalogConnection,
	dialect: AtlasDialect,
	fn: () => Promise<T>,
): Promise<T> {
	const off =
		dialect === "sqlite"
			? "PRAGMA foreign_keys = OFF"
			: dialect === "mysql"
				? "SET FOREIGN_KEY_CHECKS = 0"
				: null;
	const on =
		dialect === "sqlite"
			? "PRAGMA foreign_keys = ON"
			: dialect === "mysql"
				? "SET FOREIGN_KEY_CHECKS = 1"
				: null;

	if (off) await db.execute(off);
	try {
		return await fn();
	} finally {
		if (on) await db.execute(on);
	}
}
