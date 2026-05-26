/**
 * Schema — top-level fluent API for DDL operations.
 *
 * Each method appends compiled SQL statements (via the Rust compiler) to an
 * internal buffer that the migration runner can flush to the database.
 *
 * @implements FR34
 */

import {
	type AtlasDialect,
	compileStatementNative,
	getAtlasDialect,
} from "../query/native.js";
import { TableBuilder } from "./TableBuilder.js";

export class Schema {
	#dialect: AtlasDialect;
	#statements: string[] = [];

	constructor(dialect: AtlasDialect = getAtlasDialect()) {
		this.#dialect = dialect;
	}

	createTable(name: string, callback: (table: TableBuilder) => void): this {
		const builder = new TableBuilder(name);
		callback(builder);
		this.#statements.push(...builder.toStatements(this.#dialect));
		return this;
	}

	dropTable(name: string): this {
		const { statements } = compileStatementNative(
			{ kind: "dropTable", table: name, ifExists: true },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	createIndex(
		table: string,
		columns: string | string[],
		name?: string,
		unique = false,
	): this {
		const cols = Array.isArray(columns) ? columns : [columns];
		const indexName = name ?? `idx_${table}_${cols.join("_")}`;
		const { statements } = compileStatementNative(
			{ kind: "createIndex", table, name: indexName, columns: cols, unique },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	dropIndex(name: string): this {
		const { statements } = compileStatementNative(
			{ kind: "dropIndex", name, ifExists: true },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	raw(sql: string): this {
		this.#statements.push(sql);
		return this;
	}

	reset(): void {
		this.#statements = [];
	}

	toSQL(): string[] {
		return [...this.#statements];
	}
}
