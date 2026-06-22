/**
 * TableBuilder — fluent column/index builder used inside `Schema.createTable()`.
 *
 * Builds an in-memory column/index spec, then delegates SQL generation to the
 * Rust compiler via `compileStatementNative`. No SQL strings are produced in TS.
 *
 * @implements FR34
 */

import {
	type AtlasDialect,
	compileStatementNative,
	getAtlasDialect,
} from "../query/native.js";
import { RawSql } from "../query/QueryBuilder.js";
import { type DefaultValue, renderDefaultValue } from "./raw.js";
import {
	type ColumnDefinition,
	type ColumnType,
	type IndexDefinition,
	TYPE_KIND_MAP,
} from "./types.js";

/** Table builder — used inside `schema.createTable(name, callback)`. */
export class TableBuilder {
	readonly tableName: string;
	#columns: ColumnDefinition[] = [];
	#indexes: IndexDefinition[] = [];
	#currentColumn?: ColumnDefinition;

	constructor(tableName: string) {
		this.tableName = tableName;
	}

	// ─── Column types ─────────────────────────────────────────

	uuid(name: string): this {
		return this.#addColumn(name, "uuid");
	}

	/**
	 * Auto-incrementing integer primary key (Lucid `increments()`). The Rust
	 * compiler emits the dialect-appropriate identity clause (SQLite
	 * `AUTOINCREMENT`, Postgres `GENERATED ... AS IDENTITY`, MySQL
	 * `AUTO_INCREMENT`). For a 64-bit key use `bigIncrements()`.
	 */
	increments(name = "id"): this {
		return this.#addIncrements(name, "integer");
	}

	/** Auto-incrementing 64-bit primary key (Lucid `bigIncrements()`). */
	bigIncrements(name = "id"): this {
		return this.#addIncrements(name, "bigInteger");
	}

	string(name: string, length = 255): this {
		this.#addColumn(name, "string");
		if (this.#currentColumn) this.#currentColumn.length = length;
		return this;
	}

	text(name: string): this {
		return this.#addColumn(name, "text");
	}
	integer(name: string): this {
		return this.#addColumn(name, "integer");
	}
	bigInteger(name: string): this {
		return this.#addColumn(name, "bigInteger");
	}

	decimal(name: string, precision = 10, scale = 2): this {
		this.#addColumn(name, "decimal");
		if (this.#currentColumn) {
			this.#currentColumn.precision = precision;
			this.#currentColumn.scale = scale;
		}
		return this;
	}

	boolean(name: string): this {
		return this.#addColumn(name, "boolean");
	}
	date(name: string): this {
		return this.#addColumn(name, "date");
	}
	timestamp(name: string): this {
		return this.#addColumn(name, "timestamp");
	}
	/**
	 * `timestamp WITH time zone` — Postgres normalises every writer (atlas,
	 * `DEFAULT now()`, raw SQL) to UTC, so reads are unambiguous regardless of
	 * the server's TZ. Prefer this over {@link timestamp} for any value you
	 * compare exactly, or any column with a DB-side default / external writer.
	 * Pairs with `@column.dateTime()` on the entity (the decorator is decoupled
	 * from the SQL type). On MySQL/SQLite (no real tz type) it degrades to the
	 * plain timestamp mapping.
	 */
	timestamptz(name: string): this {
		return this.#addColumn(name, "timestamptz");
	}
	json(name: string): this {
		return this.#addColumn(name, "json");
	}
	binary(name: string): this {
		return this.#addColumn(name, "binary");
	}

	// ─── Shortcuts ────────────────────────────────────────────

	/**
	 * UUID primary key with Postgres-only `gen_random_uuid()` default.
	 *
	 * **Portability warning — DO NOT use in framework-shipped migration templates.**
	 *
	 * `gen_random_uuid()` is a Postgres-13+ built-in. SQLite and MySQL do NOT
	 * provide a function by that name; calling `id()` in a migration that
	 * runs on those dialects fails at `migrations:run` with a "no such
	 * function" error. The helper is retained for user-app migrations where
	 * the target dialect is known to be Postgres.
	 *
	 * In framework-shipped templates, write the column explicitly and supply
	 * the UUID at INSERT time:
	 *
	 *     t.uuid('id').primary()                  // no DEFAULT
	 *     // and at insert: db.insert({ id: crypto.randomUUID(), ... })
	 *
	 * See `AUDIT-migration-templates.md` (shipped at the package root) for
	 * the full audit and the escape-hatch procedure if a future story makes
	 * the helper dialect-aware.
	 */
	id(): this {
		return this.uuid("id").primary().defaultTo(new RawSql("gen_random_uuid()"));
	}

	/**
	 * `created_at` + `updated_at` columns with Postgres-only `DEFAULT NOW()`.
	 *
	 * **Portability warning — DO NOT use in framework-shipped migration templates.**
	 *
	 * `NOW()` is a Postgres/MySQL function — SQLite does NOT recognise it
	 * (SQLite accepts `CURRENT_TIMESTAMP`, not `NOW()`). Calling
	 * `timestamps()` in a migration that runs on SQLite fails at
	 * `migrations:run` with a "no such function" error. The helper is
	 * retained for user-app migrations where the target dialect is known to
	 * be Postgres or MySQL.
	 *
	 * In framework-shipped templates, write the columns explicitly without a
	 * DEFAULT and supply the value at INSERT/UPSERT time:
	 *
	 *     t.timestamp('created_at').notNullable()  // no DEFAULT
	 *     t.timestamp('updated_at').notNullable()
	 *     // and at insert: db.insert({ created_at: new Date().toISOString(), ... })
	 *
	 * See `AUDIT-migration-templates.md` (shipped at the package root) for
	 * the full audit and the escape-hatch procedure if a future story makes
	 * the helper dialect-aware.
	 */
	timestamps(): this {
		this.timestamp("created_at").notNullable().defaultTo(new RawSql("NOW()"));
		this.timestamp("updated_at").notNullable().defaultTo(new RawSql("NOW()"));
		return this;
	}

	// ─── Column modifiers ─────────────────────────────────────

	primary(): this {
		if (this.#currentColumn) this.#currentColumn.primary = true;
		return this;
	}

	notNullable(): this {
		if (this.#currentColumn) this.#currentColumn.nullable = false;
		return this;
	}

	nullable(): this {
		if (this.#currentColumn) this.#currentColumn.nullable = true;
		return this;
	}

	unique(): this {
		if (this.#currentColumn) this.#currentColumn.unique = true;
		return this;
	}

	/**
	 * Set a column default. JS literals are quoted/escaped (`'x'`, `123`,
	 * `true` — Lucid/Knex semantics); wrap SQL expressions in {@link raw} (or
	 * use `Migration.now()`) to emit them verbatim.
	 */
	defaultTo(value: DefaultValue): this {
		if (this.#currentColumn) {
			this.#currentColumn.defaultValue = renderDefaultValue(value);
		}
		return this;
	}

	references(table: string, column = "id"): this {
		if (this.#currentColumn) this.#currentColumn.references = { table, column };
		return this;
	}

	// ─── Indexes ──────────────────────────────────────────────

	index(columns: string | string[], name?: string): this {
		const cols = Array.isArray(columns) ? columns : [columns];
		this.#indexes.push({
			name: name ?? `idx_${this.tableName}_${cols.join("_")}`,
			columns: cols,
			unique: false,
		});
		return this;
	}

	uniqueIndex(columns: string | string[], name?: string): this {
		const cols = Array.isArray(columns) ? columns : [columns];
		this.#indexes.push({
			name: name ?? `idx_${this.tableName}_${cols.join("_")}_unique`,
			columns: cols,
			unique: true,
		});
		return this;
	}

	// ─── Accessors ────────────────────────────────────────────

	getColumns(): ColumnDefinition[] {
		return [...this.#columns];
	}
	getIndexes(): IndexDefinition[] {
		return [...this.#indexes];
	}

	/** Compile to SQL statements via the Rust compiler. */
	toStatements(dialect: AtlasDialect = getAtlasDialect()): string[] {
		const spec = {
			kind: "createTable",
			table: this.tableName,
			columns: this.#columns.map((c) => ({
				name: c.name,
				kind: TYPE_KIND_MAP[c.type],
				length: c.length ?? null,
				precision: c.precision ?? null,
				scale: c.scale ?? null,
				nullable: c.nullable,
				primary: c.primary,
				autoIncrement: c.autoIncrement ?? false,
				unique: c.unique,
				default: c.defaultValue ?? null,
				references: c.references ?? null,
			})),
			indexes: this.#indexes.map((i) => ({
				name: i.name,
				columns: i.columns,
				unique: i.unique,
			})),
			ifNotExists: false,
		};
		return compileStatementNative(spec, dialect).statements;
	}

	#addColumn(name: string, type: ColumnType): this {
		const col: ColumnDefinition = {
			name,
			type,
			nullable: true,
			primary: false,
			unique: false,
		};
		this.#columns.push(col);
		this.#currentColumn = col;
		return this;
	}

	#addIncrements(name: string, type: "integer" | "bigInteger"): this {
		this.#addColumn(name, type);
		if (this.#currentColumn) {
			this.#currentColumn.autoIncrement = true;
			this.#currentColumn.primary = true;
			this.#currentColumn.nullable = false;
		}
		return this;
	}
}
