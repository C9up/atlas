/**
 * Shared schema types — column type names, definitions, and the
 * mapping from logical TS types to the Rust ColumnTypeKind.
 */

export type ColumnType =
	| "string"
	| "text"
	| "integer"
	| "bigInteger"
	| "decimal"
	| "boolean"
	| "date"
	| "timestamp"
	| "uuid"
	| "json"
	| "binary";

/** Maps our logical type names to the ColumnTypeKind expected by the Rust compiler. */
export const TYPE_KIND_MAP: Record<ColumnType, string> = {
	string: "string",
	text: "text",
	integer: "integer",
	bigInteger: "bigInteger",
	decimal: "decimal",
	boolean: "boolean",
	date: "date",
	timestamp: "timestamp",
	uuid: "uuid",
	json: "json",
	binary: "binary",
};

export interface ColumnDefinition {
	name: string;
	type: ColumnType;
	length?: number;
	precision?: number;
	scale?: number;
	nullable: boolean;
	primary: boolean;
	unique: boolean;
	defaultValue?: string;
	references?: { table: string; column: string };
}

export interface IndexDefinition {
	name: string;
	columns: string[];
	unique: boolean;
}
