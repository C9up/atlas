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
import {
	type CatalogConnection,
	columnExists,
	tableExists,
} from "./catalog.js";
import { TableBuilder } from "./TableBuilder.js";

export class Schema {
	#dialect: AtlasDialect;
	#statements: string[] = [];
	#connection?: CatalogConnection;
	#schemaName?: string;

	constructor(dialect: AtlasDialect = getAtlasDialect()) {
		this.#dialect = dialect;
	}

	/**
	 * Qualify subsequent table names with `name` (Adonis Lucid/Knex
	 * `withSchema`), e.g. `withSchema('reporting').createTable('t', …)` targets
	 * `"reporting"."t"`. Applies to every table-taking method until changed; the
	 * Rust compiler quotes each dotted segment. Chainable.
	 */
	withSchema(name: string): this {
		this.#schemaName = name;
		return this;
	}

	/** Prefix a table name with the active schema, if one was set. */
	#qualify(table: string): string {
		return this.#schemaName ? `${this.#schemaName}.${table}` : table;
	}

	/**
	 * Attach a live connection so the async introspection helpers
	 * ({@link hasTable}/{@link hasColumn}) can query the catalog. The migration
	 * runner calls this before running a migration's `up`/`down`.
	 */
	bindConnection(connection: CatalogConnection): void {
		this.#connection = connection;
	}

	/**
	 * Does this table exist right now? (Adonis Lucid/Knex `hasTable`.) Runs a
	 * live catalog query, so it reflects migrations already applied — but NOT
	 * statements this same migration has only buffered (they run after `up`).
	 * Requires a bound connection (present inside a migration).
	 */
	async hasTable(name: string): Promise<boolean> {
		return tableExists(
			this.#requireConnection("hasTable"),
			this.#dialect,
			name,
		);
	}

	/** Does this column exist on this table right now? (Adonis Lucid/Knex `hasColumn`.) */
	async hasColumn(table: string, column: string): Promise<boolean> {
		return columnExists(
			this.#requireConnection("hasColumn"),
			this.#dialect,
			table,
			column,
		);
	}

	#requireConnection(method: string): CatalogConnection {
		if (!this.#connection) {
			throw new Error(
				`E_NO_CONNECTION: schema.${method}() needs a live connection — it is available inside a migration, not on a standalone Schema.`,
			);
		}
		return this.#connection;
	}

	createTable(name: string, callback: (table: TableBuilder) => void): this {
		const builder = new TableBuilder(this.#qualify(name));
		callback(builder);
		this.#statements.push(...builder.toStatements(this.#dialect));
		return this;
	}

	/** `CREATE TABLE IF NOT EXISTS` (Lucid/Knex `createTableIfNotExists`). */
	createTableIfNotExists(
		name: string,
		callback: (table: TableBuilder) => void,
	): this {
		const builder = new TableBuilder(this.#qualify(name));
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
		const builder = new TableBuilder(this.#qualify(name), "alter");
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
			{
				kind: "renameTable",
				table: this.#qualify(from),
				to: this.#qualify(to),
			},
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	/** `DROP TABLE` — errors if the table is missing (Adonis Lucid/Knex `dropTable`). */
	dropTable(name: string): this {
		const { statements } = compileStatementNative(
			{ kind: "dropTable", table: this.#qualify(name), ifExists: false },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	/** `DROP TABLE IF EXISTS` — a no-op when missing (Lucid/Knex `dropTableIfExists`). */
	dropTableIfExists(name: string): this {
		const { statements } = compileStatementNative(
			{ kind: "dropTable", table: this.#qualify(name), ifExists: true },
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
			{
				kind: "createIndex",
				table: this.#qualify(table),
				name: indexName,
				columns: cols,
				unique,
			},
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
			{ kind: "dropView", name: this.#qualify(name), ifExists: true },
			this.#dialect,
		);
		this.#statements.push(...statements);
		return this;
	}

	/** `DROP MATERIALIZED VIEW IF EXISTS name` (Lucid/Knex). Postgres-only. */
	dropMaterializedView(name: string): this {
		const { statements } = compileStatementNative(
			{
				kind: "dropView",
				name: this.#qualify(name),
				ifExists: true,
				materialized: true,
			},
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
				name: this.#qualify(name),
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
