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
	type AlterOperation,
	type CheckExpression,
	type CheckOperator,
	type CheckValue,
	type ColumnDefinition,
	type ColumnType,
	type ForeignKeyReference,
	type IndexDefinition,
	type ReferentialAction,
	type TableConstraintSpec,
	type TableOptionsSpec,
	type TextVariant,
	TYPE_KIND_MAP,
} from "./types.js";

/**
 * The chainable returned by `table.foreign([...])`, so the target reads in
 * Knex order: `.references([...]).inTable('other').onDelete('cascade')`. It
 * mutates the already-recorded constraint in place.
 */
export class ForeignKeyBuilder {
	readonly #references: ForeignKeyReference;

	constructor(references: ForeignKeyReference) {
		this.#references = references;
	}

	/** Target column(s) on the referenced table. */
	references(columns: string | readonly string[]): this {
		this.#references.columns =
			typeof columns === "string" ? [columns] : [...columns];
		return this;
	}

	/**
	 * Referenced table. Keeps returning this builder so `.onDelete()` /
	 * `.onUpdate()` can follow, as in Knex — the constraint was already
	 * recorded on the table when `foreign()` was called, so there is nothing to
	 * hand back to.
	 */
	inTable(table: string): this {
		this.#references.table = table;
		return this;
	}

	onDelete(action: ReferentialAction): this {
		this.#references.onDelete = action;
		return this;
	}

	onUpdate(action: ReferentialAction): this {
		this.#references.onUpdate = action;
		return this;
	}
}

/**
 * Whether the builder is filling a `CREATE TABLE` or an `ALTER TABLE`. In
 * `alter` mode a column-type method (`t.string('x')`) becomes `ADD COLUMN`,
 * and `.alter()` turns the pending add into a type change (Lucid/Knex).
 */
export type TableBuilderMode = "create" | "alter";

/** Table builder — used inside `schema.createTable(name, callback)`. */
export class TableBuilder {
	readonly tableName: string;
	readonly mode: TableBuilderMode;
	#columns: ColumnDefinition[] = [];
	#indexes: IndexDefinition[] = [];
	#currentColumn?: ColumnDefinition;
	/** Ordered ALTER TABLE ops. Empty (and unused) in `create` mode. */
	#operations: AlterOperation[] = [];
	/** Table-level constraints. In `alter` mode these become `addConstraint` ops instead. */
	#constraints: TableConstraintSpec[] = [];
	#options: TableOptionsSpec = {};
	/** The op the pending column modifiers apply to, in `alter` mode. */
	#currentOp?: Extract<AlterOperation, { op: "addColumn" | "alterColumn" }>;
	/**
	 * Whether `.nullable()` / `.notNullable()` was called on the current column.
	 * `ColumnDefinition.nullable` defaults to `true`, so without this flag
	 * `.alter()` could not tell "leave nullability alone" from "make it
	 * nullable" — see `AlterOperation.setNullable`.
	 */
	#nullabilityTouched = false;

	constructor(tableName: string, mode: TableBuilderMode = "create") {
		this.tableName = tableName;
		this.mode = mode;
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

	/**
	 * Text column (Lucid/Knex `text(name, textType)`). `textType` widens the
	 * MySQL type (`MEDIUMTEXT` / `LONGTEXT`); Postgres and SQLite have a single
	 * unbounded `TEXT` and ignore it.
	 */
	text(name: string, textType: TextVariant = "text"): this {
		return this.#addColumn(name, textType);
	}
	integer(name: string): this {
		return this.#addColumn(name, "integer");
	}
	/** 24-bit integer (Lucid/Knex `mediumint`). MySQL `MEDIUMINT`; pg/SQLite widen to `INTEGER`. */
	mediumint(name: string): this {
		return this.#addColumn(name, "mediumint");
	}
	/** 8-bit integer (Lucid `tinyint`). MySQL `TINYINT`; Postgres widens to `SMALLINT`; SQLite `INTEGER`. */
	tinyint(name: string): this {
		return this.#addColumn(name, "tinyint");
	}
	/** 16-bit integer (Lucid `smallint`). `SMALLINT` on pg/mysql, `INTEGER` on SQLite. */
	smallint(name: string): this {
		return this.#addColumn(name, "smallint");
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

	/**
	 * Single-precision float (Lucid `float`). `REAL` on pg/sqlite, `FLOAT` on
	 * MySQL. `precision`/`scale` render `FLOAT(p, s)` on MySQL only — pg and
	 * SQLite have fixed-width floats and ignore them.
	 */
	float(name: string, precision?: number, scale?: number): this {
		return this.#addFloat(name, "float", precision, scale);
	}
	/** Double-precision float (Lucid `double`). `DOUBLE PRECISION` on pg, `REAL` on SQLite, `DOUBLE` on MySQL. See {@link float} for precision/scale. */
	double(name: string, precision?: number, scale?: number): this {
		return this.#addFloat(name, "double", precision, scale);
	}

	boolean(name: string): this {
		return this.#addColumn(name, "boolean");
	}
	date(name: string): this {
		return this.#addColumn(name, "date");
	}
	/**
	 * Time of day (Lucid `time`). `TIME` on pg/mysql, `TEXT` on SQLite.
	 * `precision` renders `TIME(p)` fractional seconds (ignored on SQLite,
	 * which has no time type to carry it).
	 */
	time(name: string, precision?: number): this {
		this.#addColumn(name, "time");
		if (this.#currentColumn) this.#currentColumn.precision = precision;
		return this;
	}
	/**
	 * Timestamp column (Lucid `timestamp(name, options)`).
	 *
	 * `useTz: true` selects the tz-aware type — the same thing
	 * {@link timestamptz} does, exposed here for Lucid's option spelling.
	 * `precision` renders `TIMESTAMP(p)` (ignored on SQLite, which stores
	 * timestamps as TEXT).
	 */
	timestamp(
		name: string,
		options: { useTz?: boolean; precision?: number } = {},
	): this {
		this.#addColumn(name, options.useTz ? "timestamptz" : "timestamp");
		if (this.#currentColumn) this.#currentColumn.precision = options.precision;
		return this;
	}
	/** Alias of {@link timestamp} (Lucid `dateTime`). Use `{ useTz: true }` or {@link timestamptz} for a tz-aware column. */
	dateTime(
		name: string,
		options: { useTz?: boolean; precision?: number } = {},
	): this {
		return this.timestamp(name, options);
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
	/**
	 * Binary JSON (Lucid/Knex `jsonb`). `JSONB` on pg, `JSON` on MySQL, `TEXT`
	 * on SQLite.
	 *
	 * Deviation, named: atlas's {@link json} already maps to `JSONB` on
	 * Postgres (it predates this method), where Lucid's `json()` maps to
	 * `json`. Leaving `json()` alone avoids silently rewriting the physical
	 * type of existing columns and desyncing `SchemaCheck`, so on Postgres the
	 * two spellings coincide.
	 */
	jsonb(name: string): this {
		return this.#addColumn(name, "jsonb");
	}
	/**
	 * Binary blob (Lucid/Knex `binary(name, length)`). `BYTEA` on pg, `BLOB` on
	 * SQLite; on MySQL `length` selects `VARBINARY(n)` over `BLOB`.
	 */
	binary(name: string, length?: number): this {
		this.#addColumn(name, "binary");
		if (this.#currentColumn) this.#currentColumn.length = length;
		return this;
	}

	/**
	 * A column typed with a verbatim dialect type (Lucid/Knex `specificType`) —
	 * the escape hatch for types atlas has no method for (`inet`, `tsvector`,
	 * `geometry(Point, 4326)`…).
	 *
	 * Deviation, named: Knex passes the string straight through. Atlas cannot —
	 * it lands verbatim in DDL, so the Rust compiler validates it against a
	 * narrow grammar (letters, digits, spaces, `_`, and one parenthesised
	 * argument list) and rejects anything else with `E_UNSAFE_SQL`.
	 */
	specificType(name: string, type: string): this {
		this.#addColumn(name, "specificType");
		if (this.#currentColumn) this.#currentColumn.rawType = type;
		return this;
	}

	/**
	 * Fixed value-set column (Lucid `enum`). MySQL renders a native `ENUM(...)`;
	 * Postgres and SQLite render `TEXT` plus a `CHECK (col IN (...))` that pins the
	 * value set. At least one value is required.
	 */
	enum(name: string, values: string[]): this {
		this.#addColumn(name, "enum");
		if (this.#currentColumn) this.#currentColumn.values = values;
		return this;
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
	 * (A dialect-aware escape hatch can be added later if a future story needs
	 * per-dialect PK defaults.)
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
	 * (A dialect-aware escape hatch can be added later if a future story needs
	 * per-dialect PK defaults.)
	 */
	timestamps(): this {
		this.timestamp("created_at").notNullable().defaultTo(new RawSql("NOW()"));
		this.timestamp("updated_at").notNullable().defaultTo(new RawSql("NOW()"));
		return this;
	}

	// ─── Column modifiers ─────────────────────────────────────

	notNullable(): this {
		if (this.#currentColumn) this.#currentColumn.nullable = false;
		this.#nullabilityTouched = true;
		return this;
	}

	nullable(): this {
		if (this.#currentColumn) this.#currentColumn.nullable = true;
		this.#nullabilityTouched = true;
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

	/** MySQL `UNSIGNED` numeric modifier (Lucid `unsigned()`). No-op on pg/sqlite. */
	unsigned(): this {
		if (this.#currentColumn) this.#currentColumn.unsigned = true;
		return this;
	}

	references(table: string, column = "id"): this {
		if (this.#currentColumn) this.#currentColumn.references = { table, column };
		return this;
	}

	/**
	 * Referential action for the current column's foreign key `ON DELETE`
	 * (Lucid parity). Must follow {@link references}.
	 */
	onDelete(action: ReferentialAction): this {
		if (this.#currentColumn?.references) {
			this.#currentColumn.references.onDelete = action;
		}
		return this;
	}

	/** Referential action for the current column's foreign key `ON UPDATE`. Must follow {@link references}. */
	onUpdate(action: ReferentialAction): this {
		if (this.#currentColumn?.references) {
			this.#currentColumn.references.onUpdate = action;
		}
		return this;
	}

	/**
	 * Comment the current **column** (Lucid/Knex column `comment()`). Inline on
	 * MySQL, a separate `COMMENT ON COLUMN` on Postgres, dropped on SQLite.
	 *
	 * Deviation, named: Knex's `table.comment()` is the TABLE comment, because
	 * its column methods return a separate column builder. Atlas flattens the
	 * column modifiers onto the table builder (`.notNullable()`, `.unique()`,
	 * `.defaultTo()` all work this way), so `comment()` follows that same rule
	 * and the table comment is {@link tableComment}. Resolving it by "is a
	 * column pending?" would be exactly the kind of guessing that bites later.
	 */
	comment(text: string): this {
		if (this.#currentColumn) this.#currentColumn.comment = text;
		return this;
	}

	/** Collate the current **column** (Lucid/Knex column `collate()`). See {@link comment} for why the table form is {@link tableCollate}. */
	collate(collation: string): this {
		if (this.#currentColumn) this.#currentColumn.collate = collation;
		return this;
	}

	/**
	 * Place an added column first (Lucid/Knex `first()`). MySQL-only —
	 * Postgres and SQLite always append, and the Rust compiler raises
	 * `E_UNSUPPORTED` rather than dropping the instruction silently.
	 */
	first(): this {
		if (this.#currentColumn) this.#currentColumn.position = { at: "first" };
		return this;
	}

	/** Place an added column after `column` (Lucid/Knex `after()`). MySQL-only — see {@link first}. */
	after(column: string): this {
		if (this.#currentColumn) {
			this.#currentColumn.position = { at: "after", column };
		}
		return this;
	}

	// ─── CHECK constraints ────────────────────────────────────

	/** `CHECK (col > 0)` on the current column (Lucid/Knex `checkPositive`). */
	checkPositive(constraintName?: string): this {
		return this.#addCheck(
			(column) => ({ check: "positive", column }),
			constraintName,
		);
	}

	/** `CHECK (col < 0)` on the current column (Lucid/Knex `checkNegative`). */
	checkNegative(constraintName?: string): this {
		return this.#addCheck(
			(column) => ({ check: "negative", column }),
			constraintName,
		);
	}

	/** `CHECK (col IN (…))` on the current column (Lucid/Knex `checkIn`). Values are quoted, never interpolated raw. */
	checkIn(values: readonly CheckValue[], constraintName?: string): this {
		return this.#addCheck(
			(column) => ({ check: "in", column, values: [...values] }),
			constraintName,
		);
	}

	/** `CHECK (col NOT IN (…))` on the current column (Lucid/Knex `checkNotIn`). */
	checkNotIn(values: readonly CheckValue[], constraintName?: string): this {
		return this.#addCheck(
			(column) => ({ check: "notIn", column, values: [...values] }),
			constraintName,
		);
	}

	/**
	 * `CHECK (col BETWEEN lo AND hi)` on the current column (Lucid/Knex
	 * `checkBetween`). Accepts one `[min, max]` interval or a list of them —
	 * several intervals are OR'd together, as in Knex.
	 */
	checkBetween(
		range: readonly CheckValue[] | readonly (readonly CheckValue[])[],
		constraintName?: string,
	): this {
		// A single [min, max] vs a list of intervals: the first element of a
		// list-of-intervals is itself an array.
		const ranges = Array.isArray(range[0])
			? (range as readonly (readonly CheckValue[])[]).map((r) => [...r])
			: [[...(range as readonly CheckValue[])]];
		return this.#addCheck(
			(column) => ({ check: "between", column, ranges }),
			constraintName,
		);
	}

	/** `CHECK (LENGTH(col) <op> n)` on the current column (Lucid/Knex `checkLength`). The operator is allow-listed by the Rust compiler. */
	checkLength(
		operator: CheckOperator,
		length: number,
		constraintName?: string,
	): this {
		return this.#addCheck(
			(column) => ({ check: "length", column, operator, length }),
			constraintName,
		);
	}

	/**
	 * `CHECK (col ~ 'pattern')` on the current column (Lucid/Knex `checkRegex`).
	 * Postgres spells it `~`; MySQL and SQLite use `REGEXP`.
	 *
	 * SQLite parses `REGEXP` but ships no implementation — the constraint only
	 * works if the connection registers a `regexp` function. Knex behaves the
	 * same way, so this is parity rather than a new trap, but it is worth
	 * knowing before you rely on it there.
	 */
	checkRegex(pattern: string, constraintName?: string): this {
		return this.#addCheck(
			(column) => ({ check: "regex", column, pattern }),
			constraintName,
		);
	}

	/**
	 * A free-form `CHECK (predicate)` (Lucid/Knex `check`). The predicate is
	 * emitted verbatim — exactly as trusted as {@link Schema.raw}, so never
	 * build it from user input. Prefer the typed `check*` helpers, which are
	 * safe by construction.
	 */
	check(predicate: string, constraintName?: string): this {
		this.#pushConstraint({
			constraint: "check",
			name: constraintName,
			expr: { check: "raw", predicate },
		});
		return this;
	}

	/** Drop named CHECK constraints (Lucid/Knex `dropChecks`). */
	dropChecks(...constraintNames: string[]): this {
		this.#assertAlterMode("dropChecks()");
		for (const name of constraintNames) {
			this.#pushStandaloneOp({ op: "dropConstraint", name });
		}
		return this;
	}

	// ─── Table-level constraints ──────────────────────────────

	/**
	 * With no argument, mark the current column as the primary key (the
	 * existing column modifier). With a column list, declare a composite
	 * `PRIMARY KEY (…)` table constraint (Lucid/Knex `primary([...])`).
	 */
	primary(columns?: readonly string[], constraintName?: string): this {
		if (columns === undefined) {
			if (this.#currentColumn) this.#currentColumn.primary = true;
			return this;
		}
		this.#pushConstraint({
			constraint: "primary",
			name: constraintName,
			columns: [...columns],
		});
		return this;
	}

	/**
	 * With no argument, mark the current column `UNIQUE` (the existing column
	 * modifier). With a column list, declare a composite `UNIQUE (…)` table
	 * constraint (Lucid/Knex `unique([...])`).
	 *
	 * Note this is a real constraint, unlike {@link uniqueIndex}, which creates
	 * a separate `CREATE UNIQUE INDEX`.
	 */
	unique(columns?: readonly string[], constraintName?: string): this {
		if (columns === undefined) {
			if (this.#currentColumn) this.#currentColumn.unique = true;
			return this;
		}
		this.#pushConstraint({
			constraint: "unique",
			name: constraintName ?? this.#constraintName(columns, "unique"),
			columns: [...columns],
		});
		return this;
	}

	/**
	 * Declare a composite foreign key (Lucid/Knex
	 * `foreign([...]).references([...]).inTable(…)`). Returns a small chainable
	 * so the target reads in Knex order; the constraint is recorded up front
	 * and filled in as you chain.
	 */
	foreign(
		columns: string | readonly string[],
		constraintName?: string,
	): ForeignKeyBuilder {
		const cols = typeof columns === "string" ? [columns] : [...columns];
		const references: ForeignKeyReference = { table: "", columns: [] };
		this.#pushConstraint({
			constraint: "foreign",
			name: constraintName ?? this.#constraintName(cols, "foreign"),
			columns: cols,
			references,
		});
		// The constraint is already recorded; the builder fills `references` in
		// place as the caller chains, so order of arrival doesn't matter.
		return new ForeignKeyBuilder(references);
	}

	// ─── Dropping constraints ─────────────────────────────────

	/** Drop the primary key (Lucid/Knex `dropPrimary`). MySQL drops it by keyword; Postgres by name (default `<table>_pkey`). */
	dropPrimary(constraintName?: string): this {
		this.#assertAlterMode("dropPrimary()");
		this.#pushStandaloneOp({ op: "dropPrimary", name: constraintName });
		return this;
	}

	/** Drop a unique constraint by columns (using the default name) or by explicit name (Lucid/Knex `dropUnique`). */
	dropUnique(
		columns: string | readonly string[],
		constraintName?: string,
	): this {
		this.#assertAlterMode("dropUnique()");
		this.#pushStandaloneOp({
			op: "dropUnique",
			name: constraintName ?? this.#constraintName(columns, "unique"),
		});
		return this;
	}

	/** Drop a foreign key by columns (using the default name) or by explicit name (Lucid/Knex `dropForeign`). */
	dropForeign(
		columns: string | readonly string[],
		constraintName?: string,
	): this {
		this.#assertAlterMode("dropForeign()");
		this.#pushStandaloneOp({
			op: "dropForeign",
			name: constraintName ?? this.#constraintName(columns, "foreign"),
		});
		return this;
	}

	/** Drop `created_at` + `updated_at` (Lucid/Knex `dropTimestamps`). */
	dropTimestamps(): this {
		return this.dropColumns("created_at", "updated_at");
	}

	// ─── Table options ────────────────────────────────────────

	/** MySQL storage engine (Lucid/Knex `engine`). Ignored on pg/sqlite. */
	engine(name: string): this {
		this.#options.engine = name;
		return this;
	}

	/** MySQL default charset (Lucid/Knex `charset`). Ignored on pg/sqlite. */
	charset(name: string): this {
		this.#options.charset = name;
		return this;
	}

	/** MySQL default collation for the table. Named `tableCollate` because {@link collate} is the column modifier — see {@link comment}. */
	tableCollate(name: string): this {
		this.#options.collate = name;
		return this;
	}

	/** Table comment. Named `tableComment` because {@link comment} is the column modifier — see there for why. */
	tableComment(text: string): this {
		this.#options.comment = text;
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

	// ─── ALTER TABLE operations ───────────────────────────────

	/**
	 * Apply the pending column definition as a type change instead of an
	 * `ADD COLUMN` (Lucid/Knex `alter()`). Must follow a column-type method.
	 *
	 * Nullability moves only if `.nullable()` / `.notNullable()` was called
	 * before this — a bare `t.string('x').alter()` changes the type and leaves
	 * the NOT NULL constraint exactly as it is.
	 *
	 * SQLite cannot alter a column in place; the Rust compiler rejects it with
	 * `E_UNSUPPORTED` rather than emitting a table rebuild behind your back.
	 */
	alter(): this {
		this.#assertAlterMode("alter()");
		const pending = this.#currentOp;
		if (!pending) {
			throw new Error(
				"E_ALTER_MISUSE: alter() must follow a column definition, e.g. table.string('email').alter()",
			);
		}
		if (pending.op === "addColumn") {
			const converted: AlterOperation = {
				op: "alterColumn",
				column: pending.column,
				setNullable: this.#nullabilityTouched
					? pending.column.nullable
					: undefined,
			};
			this.#operations[this.#operations.indexOf(pending)] = converted;
			this.#currentOp = converted;
		}
		return this;
	}

	/** Drop a column (Lucid/Knex `dropColumn`). */
	dropColumn(name: string): this {
		this.#assertAlterMode("dropColumn()");
		this.#pushStandaloneOp({ op: "dropColumn", name });
		return this;
	}

	/** Drop several columns in call order (Lucid/Knex `dropColumns`). */
	dropColumns(...names: string[]): this {
		for (const name of names) this.dropColumn(name);
		return this;
	}

	/** Rename a column (Lucid/Knex `renameColumn`). */
	renameColumn(from: string, to: string): this {
		this.#assertAlterMode("renameColumn()");
		this.#pushStandaloneOp({ op: "renameColumn", from, to });
		return this;
	}

	/**
	 * Make an existing column nullable — `DROP NOT NULL` (Lucid/Knex
	 * `setNullable`).
	 *
	 * **Deviation from Knex, named deliberately.** Knex supports this on every
	 * dialect by querying `columnInfo()` at runtime to recover the column's
	 * type. Atlas compiles SQL synchronously in Rust with no round-trip, so
	 * this is Postgres-only — Postgres is the one dialect whose syntax needs no
	 * type. On MySQL use `table.<type>('col').nullable().alter()`, which
	 * restates the type; SQLite cannot alter a column in place at all. Both
	 * raise `E_UNSUPPORTED` with the alternative spelled out.
	 */
	setNullable(name: string): this {
		this.#assertAlterMode("setNullable()");
		this.#pushStandaloneOp({ op: "setNullable", name, nullable: true });
		return this;
	}

	/** Make an existing column `NOT NULL` (Lucid/Knex `dropNullable`). Postgres-only — see {@link setNullable}. */
	dropNullable(name: string): this {
		this.#assertAlterMode("dropNullable()");
		this.#pushStandaloneOp({ op: "setNullable", name, nullable: false });
		return this;
	}

	// ─── Accessors ────────────────────────────────────────────

	getColumns(): ColumnDefinition[] {
		return [...this.#columns];
	}
	getIndexes(): IndexDefinition[] {
		return [...this.#indexes];
	}
	/** Ordered ALTER TABLE operations. Empty in `create` mode. */
	getOperations(): AlterOperation[] {
		return [...this.#operations];
	}

	/** Compile to SQL statements via the Rust compiler. */
	toStatements(
		dialect: AtlasDialect = getAtlasDialect(),
		options: { ifNotExists?: boolean } = {},
	): string[] {
		const spec =
			this.mode === "alter"
				? {
						kind: "alterTable",
						table: this.tableName,
						operations: this.#operations.map((op) =>
							op.op === "addColumn"
								? { op: op.op, column: this.#serializeColumn(op.column) }
								: op.op === "alterColumn"
									? {
											op: op.op,
											column: this.#serializeColumn(op.column),
											setNullable: op.setNullable ?? null,
										}
									: op,
						),
					}
				: {
						kind: "createTable",
						table: this.tableName,
						columns: this.#columns.map((c) => this.#serializeColumn(c)),
						indexes: this.#indexes.map((i) => ({
							name: i.name,
							columns: i.columns,
							unique: i.unique,
						})),
						ifNotExists: options.ifNotExists ?? false,
						constraints: this.#constraints,
						options: this.#options,
					};
		const statements = compileStatementNative(spec, dialect).statements;
		// `alterTable` carries no index list — an index added alongside an
		// ALTER compiles to its own CREATE INDEX, appended in declaration order.
		if (this.mode === "alter" && this.#indexes.length > 0) {
			for (const idx of this.#indexes) {
				statements.push(
					...compileStatementNative(
						{
							kind: "createIndex",
							table: this.tableName,
							name: idx.name,
							columns: idx.columns,
							unique: idx.unique,
						},
						dialect,
					).statements,
				);
			}
		}
		return statements;
	}

	/** Flatten a column into the wire shape the Rust `ColumnDef` deserialises. */
	#serializeColumn(c: ColumnDefinition): Record<string, unknown> {
		return {
			name: c.name,
			kind: TYPE_KIND_MAP[c.type],
			length: c.length ?? null,
			precision: c.precision ?? null,
			scale: c.scale ?? null,
			// Flattened into the Rust ColumnTypeSpec — each read for one kind only.
			values: c.values ?? null,
			rawType: c.rawType ?? null,
			nullable: c.nullable,
			primary: c.primary,
			autoIncrement: c.autoIncrement ?? false,
			unique: c.unique,
			unsigned: c.unsigned ?? false,
			default: c.defaultValue ?? null,
			references: c.references ?? null,
			comment: c.comment ?? null,
			collate: c.collate ?? null,
			position: c.position ?? null,
		};
	}

	/**
	 * Record a constraint. In `create` mode it renders inside the
	 * `CREATE TABLE`; in `alter` mode it becomes an `ADD CONSTRAINT`, kept in
	 * call order with the surrounding column operations.
	 */
	#pushConstraint(spec: TableConstraintSpec): void {
		if (this.mode === "alter") {
			this.#operations.push({ op: "addConstraint", constraint: spec });
		} else {
			this.#constraints.push(spec);
		}
	}

	/** Build a CHECK against the pending column. */
	#addCheck(
		build: (column: string) => CheckExpression,
		constraintName?: string,
	): this {
		const column = this.#currentColumn?.name;
		if (!column) {
			throw new Error(
				"E_CHECK_MISUSE: a check* helper must follow a column definition, e.g. table.integer('qty').checkPositive()",
			);
		}
		this.#pushConstraint({
			constraint: "check",
			name: constraintName,
			expr: build(column),
		});
		return this;
	}

	/**
	 * Default constraint name, following Knex's `<table>_<columns>_<suffix>`
	 * convention so `unique([...])` and `dropUnique([...])` agree without the
	 * caller naming anything. Distinct from {@link uniqueIndex}, which names a
	 * separate INDEX object `idx_…`.
	 */
	#constraintName(
		columns: string | readonly string[],
		suffix: "unique" | "foreign",
	): string {
		const cols = typeof columns === "string" ? [columns] : columns;
		return `${this.tableName}_${cols.join("_")}_${suffix}`;
	}

	#assertAlterMode(method: string): void {
		if (this.mode !== "alter") {
			throw new Error(
				`E_ALTER_MISUSE: ${method} is only available inside schema.alterTable() — a new table has nothing to alter`,
			);
		}
	}

	/** Record an op that takes no column modifiers, so `.nullable()` etc. can't silently attach to it. */
	#pushStandaloneOp(op: AlterOperation): void {
		this.#operations.push(op);
		this.#currentColumn = undefined;
		this.#currentOp = undefined;
		this.#nullabilityTouched = false;
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
		this.#nullabilityTouched = false;
		if (this.mode === "alter") {
			const op: Extract<AlterOperation, { op: "addColumn" }> = {
				op: "addColumn",
				column: col,
			};
			this.#operations.push(op);
			this.#currentOp = op;
		}
		return this;
	}

	#addFloat(
		name: string,
		type: "float" | "double",
		precision?: number,
		scale?: number,
	): this {
		this.#addColumn(name, type);
		if (this.#currentColumn) {
			this.#currentColumn.precision = precision;
			this.#currentColumn.scale = scale;
		}
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
