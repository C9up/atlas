/**
 * Shared schema types — column type names, definitions, and the
 * mapping from logical TS types to the Rust ColumnTypeKind.
 */

export type ColumnType =
	| "string"
	| "text"
	| "mediumtext"
	| "longtext"
	| "integer"
	| "tinyint"
	| "smallint"
	| "mediumint"
	| "bigInteger"
	| "decimal"
	| "float"
	| "double"
	| "boolean"
	| "date"
	| "time"
	| "timestamp"
	| "timestamptz"
	| "uuid"
	| "json"
	| "jsonb"
	| "binary"
	| "enum"
	| "specificType";

/** Maps our logical type names to the ColumnTypeKind expected by the Rust compiler. */
export const TYPE_KIND_MAP: Record<ColumnType, string> = {
	string: "string",
	text: "text",
	mediumtext: "mediumText",
	longtext: "longText",
	integer: "integer",
	tinyint: "tinyInt",
	smallint: "smallInt",
	mediumint: "mediumInt",
	bigInteger: "bigInteger",
	decimal: "decimal",
	float: "float",
	double: "double",
	boolean: "boolean",
	date: "date",
	time: "time",
	timestamp: "timestamp",
	timestamptz: "timestamptz",
	uuid: "uuid",
	json: "json",
	jsonb: "jsonb",
	binary: "binary",
	enum: "enum",
	specificType: "specificType",
};

/** MySQL text width (Lucid/Knex `text(name, textType)`). `TEXT` on pg/sqlite. */
export type TextVariant = "text" | "mediumtext" | "longtext";

/**
 * SQL referential action for a foreign key's `ON DELETE` / `ON UPDATE`
 * (Lucid parity). Validated case-insensitively by the Rust compiler.
 */
export type ReferentialAction =
	| "cascade"
	| "restrict"
	| "set null"
	| "set default"
	| "no action";

export interface ColumnDefinition {
	name: string;
	type: ColumnType;
	length?: number;
	precision?: number;
	scale?: number;
	nullable: boolean;
	primary: boolean;
	/** Dialect-appropriate auto-increment / identity column (Lucid `increments`). Implies primary key. */
	autoIncrement?: boolean;
	unique: boolean;
	/** MySQL `UNSIGNED` numeric modifier (Lucid `unsigned()`). No-op on pg/sqlite. */
	unsigned?: boolean;
	/** Allowed values for an `enum` column. */
	values?: string[];
	/**
	 * Verbatim SQL type for a `specificType` column. Validated by the Rust
	 * compiler against a narrow grammar before it reaches DDL — it is
	 * interpolated, not bound.
	 */
	rawType?: string;
	/** Column comment. Inline on MySQL, `COMMENT ON COLUMN` on pg, dropped on SQLite. */
	comment?: string;
	/** Column collation, e.g. `utf8mb4_unicode_ci`. Validated as an identifier. */
	collate?: string;
	/** MySQL-only placement of an added column (`FIRST` / `AFTER col`). */
	position?: ColumnPosition;
	defaultValue?: string;
	references?: {
		table: string;
		column: string;
		onDelete?: ReferentialAction;
		onUpdate?: ReferentialAction;
	};
}

export interface IndexDefinition {
	name: string;
	columns: string[];
	unique: boolean;
}

/** MySQL-only placement of an added column. Mirrors the Rust `ColumnPosition`. */
export type ColumnPosition = { at: "first" } | { at: "after"; column: string };

/** A scalar a CHECK constraint can compare against. DDL cannot bind params — these are quoted, not bound. */
export type CheckValue = string | number | boolean | null;

/**
 * A CHECK predicate. Structured rather than free-form SQL so each variant is
 * safe by construction; `raw` is the escape hatch and is exactly as trusted as
 * `schema.raw()`. Mirrors the Rust `CheckExpr` tagged union.
 */
export type CheckExpression =
	| { check: "positive"; column: string }
	| { check: "negative"; column: string }
	| { check: "in"; column: string; values: CheckValue[] }
	| { check: "notIn"; column: string; values: CheckValue[] }
	| { check: "between"; column: string; ranges: CheckValue[][] }
	| { check: "length"; column: string; operator: string; length: number }
	| { check: "regex"; column: string; pattern: string }
	| { check: "raw"; predicate: string };

/** Comparison operator accepted by `checkLength` (Lucid/Knex). */
export type CheckOperator = "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=";

/** A multi-column foreign key target. Mirrors the Rust `ForeignKeyRefMulti`. */
export interface ForeignKeyReference {
	table: string;
	columns: string[];
	onDelete?: ReferentialAction;
	onUpdate?: ReferentialAction;
}

/** A table-level constraint. Mirrors the Rust `TableConstraint` tagged union. */
export type TableConstraintSpec =
	| { constraint: "primary"; name?: string; columns: string[] }
	| { constraint: "unique"; name?: string; columns: string[] }
	| {
			constraint: "foreign";
			name?: string;
			columns: string[];
			references: ForeignKeyReference;
	  }
	| { constraint: "check"; name?: string; expr: CheckExpression };

/** Storage options for `CREATE TABLE`. Mirrors the Rust `TableOptions`. */
export interface TableOptionsSpec {
	engine?: string;
	charset?: string;
	collate?: string;
	comment?: string;
}

/**
 * One ALTER TABLE operation, in call order (Lucid/Knex semantics). Mirrors the
 * Rust `AlterOp` tagged union — the `op` tags and field names are the wire
 * contract with `compile_alter_table`.
 */
export type AlterOperation =
	| { op: "addColumn"; column: ColumnDefinition }
	| { op: "dropColumn"; name: string }
	| { op: "renameColumn"; from: string; to: string }
	| {
			op: "alterColumn";
			column: ColumnDefinition;
			/**
			 * Tri-state. `undefined` leaves nullability untouched so a bare type
			 * change never silently adds a NOT NULL — only an explicit
			 * `.nullable()` / `.notNullable()` before `.alter()` sets it.
			 */
			setNullable?: boolean;
	  }
	| { op: "setNullable"; name: string; nullable: boolean }
	| { op: "addConstraint"; constraint: TableConstraintSpec }
	| { op: "dropConstraint"; name: string }
	| { op: "dropPrimary"; name?: string }
	| { op: "dropUnique"; name: string }
	| { op: "dropForeign"; name: string };
