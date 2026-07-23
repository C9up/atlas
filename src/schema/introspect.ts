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

import type { AtlasDialect } from "../query/native.js";

/**
 * Minimal connection surface the check needs: a row-returning `query`. Both the
 * real `AsyncDatabaseConnection` and the test `Database` satisfy it, so the
 * checker doesn't depend on the full driver interface.
 */
export interface SchemaIntrospectable {
	query(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
}

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
	db: SchemaIntrospectable,
	dialect: AtlasDialect,
	table: string,
	schema?: string,
): Promise<IntrospectedColumn[] | null> {
	assertIdent(table);
	if (schema !== undefined) assertIdent(schema);
	switch (dialect) {
		case "sqlite":
			return introspectSqlite(db, table);
		case "postgres":
			return introspectPostgres(db, table, schema);
		case "mysql":
			return introspectMysql(db, table, schema);
	}
}

async function introspectSqlite(
	db: SchemaIntrospectable,
	table: string,
): Promise<IntrospectedColumn[] | null> {
	const rows = await db.query(`SELECT * FROM pragma_table_info('${table}')`);
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
	db: SchemaIntrospectable,
	table: string,
	schema?: string,
): Promise<IntrospectedColumn[] | null> {
	// Target a specific schema when given (Lucid `schemas`), else the current one.
	const schemaPred = schema ? "$2" : "current_schema()";
	const args = schema ? [table, schema] : [table];
	const cols = await db.query(
		`SELECT column_name, data_type, is_nullable, column_default
		 FROM information_schema.columns
		 WHERE table_schema = ${schemaPred} AND table_name = $1
		 ORDER BY ordinal_position`,
		args,
	);
	if (cols.length === 0) return null;
	const pkRows = await db.query(
		`SELECT kcu.column_name
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON kcu.constraint_name = tc.constraint_name
		  AND kcu.table_schema = tc.table_schema
		 WHERE tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
		   AND tc.table_schema = ${schemaPred}`,
		args,
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
	db: SchemaIntrospectable,
	table: string,
	schema?: string,
): Promise<IntrospectedColumn[] | null> {
	// In MySQL a "schema" IS a database; target it when given, else DATABASE().
	const schemaPred = schema ? "?" : "DATABASE()";
	const args = schema ? [schema, table] : [table];
	const rows = await db.query(
		`SELECT column_name, data_type, is_nullable, column_default, column_key
		 FROM information_schema.columns
		 WHERE table_schema = ${schemaPred} AND table_name = ?
		 ORDER BY ordinal_position`,
		args,
	);
	if (rows.length === 0) return null;
	return rows.map((r) => ({
		name: String(r.column_name ?? r.COLUMN_NAME),
		type: String(r.data_type ?? r.DATA_TYPE ?? ""),
		nullable: String(r.is_nullable ?? r.IS_NULLABLE).toUpperCase() === "YES",
		hasDefault:
			(r.column_default ?? r.COLUMN_DEFAULT) !== null &&
			(r.column_default ?? r.COLUMN_DEFAULT) !== undefined,
		primaryKey:
			String(r.column_key ?? r.COLUMN_KEY ?? "").toUpperCase() === "PRI",
	}));
}
