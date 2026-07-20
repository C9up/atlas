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

	/** `CREATE TABLE IF NOT EXISTS` (Lucid/Knex `createTableIfNotExists`). */
	createTableIfNotExists(
		name: string,
		callback: (table: TableBuilder) => void,
	): this {
		const builder = new TableBuilder(name);
		callback(builder);
		this.#statements.push(
			...builder.toStatements(this.#dialect, { ifNotExists: true }),
		);
		return this;
	}

	/**
	 * `ALTER TABLE` (Lucid/Knex `alterTable`, also spelled `table()`). Inside
	 * the callback a column-type method adds a column, `.alter()` changes one,
	 * and `dropColumn` / `renameColumn` / `setNullable` / `dropNullable` do
	 * what they say. Operations compile in call order.
	 */
	alterTable(name: string, callback: (table: TableBuilder) => void): this {
		const builder = new TableBuilder(name, "alter");
		callback(builder);
		// An empty callback is a caller bug, not an empty statement list: the
		// Rust compiler rejects a no-op ALTER, so short-circuit with a clearer error.
		if (
			builder.getOperations().length === 0 &&
			builder.getIndexes().length === 0
		) {
			throw new Error(
				`E_ALTER_EMPTY: schema.alterTable('${name}') declared no operations`,
			);
		}
		this.#statements.push(...builder.toStatements(this.#dialect));
		return this;
	}

	/** Alias of {@link alterTable} (Lucid/Knex `table()`). */
	table(name: string, callback: (table: TableBuilder) => void): this {
		return this.alterTable(name, callback);
	}

	/** `ALTER TABLE old RENAME TO new` (Lucid/Knex `renameTable`). */
	renameTable(from: string, to: string): this {
		const { statements } = compileStatementNative(
			{ kind: "renameTable", table: from, to },
			this.#dialect,
		);
		this.#statements.push(...statements);
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

	/**
	 * `CREATE VIEW name [(cols)] AS <select>` (Lucid/Knex `createView`). The
	 * `select` is raw, developer-authored SQL — same trust level as {@link raw} —
	 * embedded verbatim; the view name and column list are validated + quoted by
	 * the Rust compiler.
	 */
	createView(
		name: string,
		select: string,
		options: { columns?: string[] } = {},
	): this {
		return this.#pushView(name, select, options);
	}

	/** `CREATE OR REPLACE VIEW` (Lucid/Knex `createViewOrReplace`). Rejected on SQLite. */
	createViewOrReplace(
		name: string,
		select: string,
		options: { columns?: string[] } = {},
	): this {
		return this.#pushView(name, select, { ...options, orReplace: true });
	}

	/** `CREATE MATERIALIZED VIEW` (Lucid/Knex `createMaterializedView`). Postgres-only. */
	createMaterializedView(
		name: string,
		select: string,
		options: { columns?: string[] } = {},
	): this {
		return this.#pushView(name, select, { ...options, materialized: true });
	}

	/** `DROP VIEW IF EXISTS name` (Lucid/Knex `dropView`/`dropViewIfExists`). */
	dropView(name: string): this {
		const { statements } = compileStatementNative(
			{ kind: "dropView", name, ifExists: true },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	/** `DROP MATERIALIZED VIEW IF EXISTS name` (Lucid/Knex). Postgres-only. */
	dropMaterializedView(name: string): this {
		const { statements } = compileStatementNative(
			{ kind: "dropView", name, ifExists: true, materialized: true },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	#pushView(
		name: string,
		select: string,
		options: {
			columns?: string[];
			orReplace?: boolean;
			materialized?: boolean;
		},
	): this {
		const { statements } = compileStatementNative(
			{
				kind: "createView",
				name,
				select,
				orReplace: options.orReplace ?? false,
				materialized: options.materialized ?? false,
				columns: options.columns ?? null,
			},
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
