/**
 * Live schema introspection — reads the ACTUAL table/column shape from the
 * connected database, per dialect. The reconciler in `SchemaCheck.ts` diffs
 * this against each model's `@Column` metadata.
 *
 * SQLite uses `pragma_table_info`; Postgres and MySQL use `information_schema`.
 * Table names come from `@Entity(...)` (developer-defined, trusted) — they are
 * validated against a strict identifier pattern before interpolation, since
 * `pragma_table_info(...)` cannot bind its argument.
 */

import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import type { AtlasDialect } from "../query/native.js";

/** One column as it actually exists in the database. */
export interface IntrospectedColumn {
	name: string;
	/** Raw dialect type string (e.g. `INTEGER`, `character varying`, `varchar`). */
	type: string;
	nullable: boolean;
	hasDefault: boolean;
	primaryKey: boolean;
}

/** A safe SQL identifier (table name from `@Entity`). */
const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

function assertIdent(name: string): void {
	if (!IDENT.test(name)) {
		throw new Error(
			`[atlas:check] refusing to introspect unsafe table identifier: ${JSON.stringify(name)}`,
		);
	}
}

/**
 * Introspect one table. Returns its columns, or `null` when the table does not
 * exist in the database (a distinct, reportable drift — not an error).
 */
export async function introspectTable(
	db: AsyncDatabaseConnection,
	dialect: AtlasDialect,
	table: string,
): Promise<IntrospectedColumn[] | null> {
	assertIdent(table);
	switch (dialect) {
		case "sqlite":
			return introspectSqlite(db, table);
		case "postgres":
			return introspectPostgres(db, table);
		case "mysql":
			return introspectMysql(db, table);
	}
}

async function introspectSqlite(
	db: AsyncDatabaseConnection,
	table: string,
): Promise<IntrospectedColumn[] | null> {
	const rows = await db.query<Record<string, unknown>>(
		`SELECT * FROM pragma_table_info('${table}')`,
	);
	if (rows.length === 0) return null; // unknown table → no columns
	return rows.map((r) => ({
		name: String(r.name),
		type: String(r.type ?? ""),
		nullable: Number(r.notnull) === 0,
		hasDefault: r.dflt_value !== null && r.dflt_value !== undefined,
		primaryKey: Number(r.pk) > 0,
	}));
}

async function introspectPostgres(
	db: AsyncDatabaseConnection,
	table: string,
): Promise<IntrospectedColumn[] | null> {
	const cols = await db.query<Record<string, unknown>>(
		`SELECT column_name, data_type, is_nullable, column_default
		 FROM information_schema.columns
		 WHERE table_schema = current_schema() AND table_name = $1
		 ORDER BY ordinal_position`,
		[table],
	);
	if (cols.length === 0) return null;
	const pkRows = await db.query<Record<string, unknown>>(
		`SELECT kcu.column_name
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON kcu.constraint_name = tc.constraint_name
		  AND kcu.table_schema = tc.table_schema
		 WHERE tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'`,
		[table],
	);
	const pks = new Set(pkRows.map((r) => String(r.column_name)));
	return cols.map((r) => ({
		name: String(r.column_name),
		type: String(r.data_type ?? ""),
		nullable: String(r.is_nullable).toUpperCase() === "YES",
		hasDefault: r.column_default !== null && r.column_default !== undefined,
		primaryKey: pks.has(String(r.column_name)),
	}));
}

async function introspectMysql(
	db: AsyncDatabaseConnection,
	table: string,
): Promise<IntrospectedColumn[] | null> {
	const rows = await db.query<Record<string, unknown>>(
		`SELECT column_name, data_type, is_nullable, column_default, column_key
		 FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name = ?
		 ORDER BY ordinal_position`,
		[table],
	);
	if (rows.length === 0) return null;
	return rows.map((r) => ({
		name: String(r.column_name ?? r.COLUMN_NAME),
		type: String(r.data_type ?? r.DATA_TYPE ?? ""),
		nullable: String(r.is_nullable ?? r.IS_NULLABLE).toUpperCase() === "YES",
		hasDefault:
			(r.column_default ?? r.COLUMN_DEFAULT) !== null &&
			(r.column_default ?? r.COLUMN_DEFAULT) !== undefined,
		primaryKey: String(r.column_key ?? r.COLUMN_KEY ?? "").toUpperCase() === "PRI",
	}));
}
