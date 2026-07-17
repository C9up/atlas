/**
 * Shared schema types — column type names, definitions, and the
 * mapping from logical TS types to the Rust ColumnTypeKind.
 */

export type ColumnType =
	| "string"
	| "text"
	| "integer"
	| "tinyint"
	| "smallint"
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
	| "binary"
	| "enum";

/** Maps our logical type names to the ColumnTypeKind expected by the Rust compiler. */
export const TYPE_KIND_MAP: Record<ColumnType, string> = {
	string: "string",
	text: "text",
	integer: "integer",
	tinyint: "tinyInt",
	smallint: "smallInt",
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
	binary: "binary",
	enum: "enum",
};

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
