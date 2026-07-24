/**
 * Connection-level query builder — Adonis Lucid's `db.query()` / `db.from()` /
 * `db.table()` / `db.insertQuery()`. Unlike {@link ModelQuery} it is NOT bound to
 * a model: it reads and writes plain rows against a table, executing through a
 * connection (or a transaction client passed as `{ client }`).
 *
 * Reads AND writes go through the native compiler directly (`compileStatementNative`),
 * the same path the repository uses — so quoting, casts, the full WHERE grammar
 * (between/like/raw/exists), joins, locks and parameter binding are identical.
 *
 *     const rows = await db.from('users').where('is_active', true).orderBy('id')
 *     const user = await db.query().from('users').where('id', 1).first()
 *     await db.table('audit_logs').insert({ user_id: 1, action: 'login' })
 *     await db.from('users').where('id', 1).update({ is_active: false })
 */

import type { QueryMeta } from "../adapters/NapiDbAdapter.js";
import { Paginator } from "../ModelQuery.js";
import { DmlBuilder, type DmlChainHooks } from "./DmlBuilder.js";
import { type AtlasDialect, compileStatementNative } from "./native.js";
import { RawSql, type WhereOperator } from "./QueryBuilder.js";
import { RawQueryBuilder } from "./RawQueryBuilder.js";

/** The minimal execute/query surface a connection or transaction client offers. */
export interface QueryExecutor {
	query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<T[]>;
	execute(sql: string, params?: unknown[], meta?: QueryMeta): Promise<unknown>;
}

/** The Lucid query-builder entry points a transaction client exposes. */
export interface TransactionQueryBuilders {
	/** Query builder pre-selected on `table` (Lucid `trx.from`). */
	from(table: string): DatabaseQueryBuilder;
	/** Write builder pre-selected on `table` (Lucid `trx.table`). */
	table(table: string): DatabaseQueryBuilder;
	/** An insert builder (Lucid `trx.insertQuery()`). */
	insertQuery(): DatabaseQueryBuilder;
	/**
	 * No argument → a query builder (Lucid `trx.query()`). With SQL → run it
	 * low-level, preserving the connection-level `query(sql, params)` executor.
	 */
	query(): DatabaseQueryBuilder;
	query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<T[]>;
	/** A chainable raw query bound to the transaction (Lucid `trx.rawQuery`). */
	rawQuery<T = Record<string, unknown>>(
		sql: string,
		bindings?: unknown[] | Record<string, unknown>,
	): RawQueryBuilder<T>;
	/** A raw SQL fragment (Lucid `trx.raw(sql, bindings)`). */
	raw(sql: string, params?: unknown[]): RawSql;
}

/** Build the query-builder entry points a transaction client exposes. */
export function makeTransactionQueryBuilders(
	exec: QueryExecutor,
	dialect: AtlasDialect,
): TransactionQueryBuilders {
	// Capture the ORIGINAL low-level executor now — trx assembly does
	// `Object.assign(conn, makeTransactionQueryBuilders(conn, …))`, which
	// overwrites `conn.query` with the dispatcher below. Without this bind, the
	// with-SQL branch would call itself and recurse forever.
	const rawExecQuery = exec.query.bind(exec);
	function query(): DatabaseQueryBuilder;
	function query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<T[]>;
	function query(
		sql?: string,
		params?: unknown[],
		meta?: QueryMeta,
	): DatabaseQueryBuilder | Promise<unknown[]> {
		return sql === undefined
			? new DatabaseQueryBuilder(exec, dialect)
			: rawExecQuery(sql, params, meta);
	}
	return {
		from: (table) => new DatabaseQueryBuilder(exec, dialect, table),
		table: (table) => new DatabaseQueryBuilder(exec, dialect, table),
		insertQuery: () => new DatabaseQueryBuilder(exec, dialect),
		query,
		rawQuery: (sql, bindings = []) =>
			new RawQueryBuilder(exec, dialect, sql, bindings),
		raw: (sql, params = []) => new RawSql(sql, params),
	};
}

/** One accumulated WHERE — comparison, raw fragment, EXISTS subquery, or JSON. */
type WhereEntry =
	| {
			kind: "cmp";
			column: string;
			operator: string;
			value: unknown;
			boolean: "and" | "or";
	  }
	| { kind: "raw"; sql: string; bindings: unknown[]; boolean: "and" | "or" }
	| {
			kind: "exists";
			negated: boolean;
			subquery: Record<string, unknown>;
			boolean: "and" | "or";
	  }
	| {
			kind: "inSub";
			column: string;
			negated: boolean;
			subquery: Record<string, unknown>;
			boolean: "and" | "or";
	  }
	| {
			kind: "inTuple";
			columns: string[];
			rows: unknown[][];
			negated: boolean;
			boolean: "and" | "or";
	  }
	| {
			kind: "json";
			jsonOp: "path" | "superset" | "subset";
			column: string;
			negated: boolean;
			path?: string;
			operator?: string;
			value: unknown;
			boolean: "and" | "or";
	  }
	| {
			kind: "group";
			conditions: CompiledWhere[];
			boolean: "and" | "or";
			negated?: boolean;
	  };

/** The native compiler's WHERE entry JSON (camelCase). */
type CompiledWhere = Record<string, unknown>;

/** A sub-query argument — an explicit builder OR a callback that builds one. */
type SubqueryArg =
	| DatabaseQueryBuilder
	| ((query: DatabaseQueryBuilder) => void);

/** A raw JOIN fragment (Knex/Lucid `joinRaw` / `innerJoin` / `leftJoin`). */
interface JoinEntry {
	sql: string;
	params: unknown[];
}

/**
 * The `ON` builder passed to the callback form of `join`/`innerJoin`/`leftJoin`/…
 * (Lucid/Knex). `on*` join two columns; `onVal*` bind a column to a value. The
 * `and`/`or` prefix chains conditions. Mirrors {@link ModelQuery}'s JoinBuilder
 * but with no model value-preparation (the db builder is model-agnostic).
 */
export interface DbJoinBuilder {
	on(left: string, right: string): DbJoinBuilder;
	on(left: string, operator: string, right: string): DbJoinBuilder;
	andOn(left: string, right: string): DbJoinBuilder;
	andOn(left: string, operator: string, right: string): DbJoinBuilder;
	orOn(left: string, right: string): DbJoinBuilder;
	orOn(left: string, operator: string, right: string): DbJoinBuilder;
	onVal(left: string, value: unknown): DbJoinBuilder;
	andOnVal(left: string, value: unknown): DbJoinBuilder;
	orOnVal(left: string, value: unknown): DbJoinBuilder;
	/** `ON col IN (?, ?)` — bound values (Lucid/Knex `onIn`). */
	onIn(left: string, values: unknown[]): DbJoinBuilder;
	/** `ON col NOT IN (?, ?)` — bound values (Lucid/Knex `onNotIn`). */
	onNotIn(left: string, values: unknown[]): DbJoinBuilder;
	/** `ON col IS NULL` (Lucid/Knex `onNull`). */
	onNull(left: string): DbJoinBuilder;
	/** `ON col IS NOT NULL` (Lucid/Knex `onNotNull`). */
	onNotNull(left: string): DbJoinBuilder;
	/** `ON col BETWEEN ? AND ?` — inclusive (Lucid/Knex `onBetween`). */
	onBetween(left: string, range: readonly [unknown, unknown]): DbJoinBuilder;
	/** `ON col NOT BETWEEN ? AND ?` (Lucid/Knex `onNotBetween`). */
	onNotBetween(left: string, range: readonly [unknown, unknown]): DbJoinBuilder;
	/** `ON EXISTS (subquery)` — a builder or a callback (Lucid/Knex `onExists`). */
	onExists(subquery: SubqueryArg): DbJoinBuilder;
	/** `ON NOT EXISTS (subquery)` (Lucid/Knex `onNotExists`). */
	onNotExists(subquery: SubqueryArg): DbJoinBuilder;
}

/**
 * One accumulated `ON` part. `right` is a column ref (with `operator`, default
 * `=`); `value` binds a scalar; `values` binds an `IN`/`NOT IN` list; `between`
 * binds a range; `nullOp` is `IS NULL` / `IS NOT NULL`; `exists` embeds a
 * compiled subquery.
 */
interface JoinPart {
	kind: "and" | "or";
	left?: string;
	operator?: string;
	right?: string;
	value?: { v: unknown };
	values?: unknown[];
	notIn?: boolean;
	between?: [unknown, unknown];
	notBetween?: boolean;
	nullOp?: "IS NULL" | "IS NOT NULL";
	exists?: { sql: string; params: unknown[]; not: boolean };
}

export class DatabaseQueryBuilder<T = Record<string, unknown>> {
	readonly #exec: QueryExecutor;
	readonly #dialect: AtlasDialect;
	#table: string;
	#selects: string[] = [];
	#wheres: WhereEntry[] = [];
	#orderBys: Array<
		{ column: string; direction: "asc" | "desc" } | { raw: string }
	> = [];
	#groupBys: string[] = [];
	#havings: Array<
		| { column: string; operator: string; value: unknown; type: "and" | "or" }
		| { kind: "raw"; sql: string; bindings: unknown[]; type: "and" | "or" }
	> = [];
	#joins: JoinEntry[] = [];
	#unions: Array<{
		sql: string;
		params: unknown[];
		all: boolean;
		op: "union" | "intersect" | "except" | null;
	}> = [];
	#ctes: Array<{
		name: string;
		sql: string;
		params: unknown[];
		recursive: boolean;
		materialized: boolean | null;
		columns?: string[];
	}> = [];
	#schema?: string;
	#lockMode?: string;
	#lockModifier?: string;
	#distinctOn: string[] = [];
	#returningCols: string[] = [];
	#onConflictCols?: string[];
	#mergeMode?: "merge" | "ignore";
	#mergeCols: string[] = [];
	/** Custom merge assignments from `merge({ col: value | db.raw(...) })`. */
	#mergeSet?: Array<{
		column: string;
		value?: unknown;
		raw?: string;
		rawParams?: unknown[];
	}>;
	#distinctFlag = false;
	#limit?: number;
	#offset?: number;
	#debug = false;
	#comments: string[] = [];
	#reporterData?: Record<string, unknown>;
	#fromSubquery?: { sql: string; params: unknown[]; alias: string };
	/**
	 * Raw / subquery SELECT fragments that carry their own bound params — Lucid
	 * `select(db.raw(sql, bindings))` and `select(subquery.as('x'))`. Rendered into
	 * the SELECT list by the native compiler with their placeholders remapped.
	 */
	#selectRaw: Array<{ sql: string; params: unknown[] }> = [];
	/**
	 * This builder's own alias, set by `.as(alias)`. Consumed when the builder is
	 * used as a derived `FROM (…) AS <alias>` or as a `SELECT (…) AS <alias>`
	 * subquery — the Lucid/Knex `.as()` convention.
	 */
	#alias?: string;
	/** Caller-facing statement timeout in ms (Lucid `timeout(ms)`), applied via a race in the read paths. */
	#timeoutMs?: number;

	constructor(exec: QueryExecutor, dialect: AtlasDialect, table = "") {
		this.#exec = exec;
		this.#dialect = dialect;
		this.#table = table;
	}

	/** Select the table (Lucid `db.from`). */
	from(table: string): this;
	/**
	 * Select a derived-table source — `FROM (<subquery>) AS <alias>` (Lucid
	 * `from(subquery)`). The subquery is a builder OR a callback that builds one.
	 */
	from(subquery: SubqueryArg, alias?: string): this;
	from(source: string | SubqueryArg, alias?: string): this {
		if (typeof source === "string") {
			this.#table = source;
			this.#fromSubquery = undefined;
			return this;
		}
		// A callback builds the subquery on a fresh sub-builder. The alias may come
		// from the explicit 2nd arg (atlas DX) OR from `.as()` inside the callback
		// (`from((sub) => sub.from('x').as('totals'))` — the Lucid convention).
		const sub = typeof source === "function" ? this.#buildSub(source) : source;
		const { sql, params } = sub.toSQL();
		this.#fromSubquery = {
			sql,
			params,
			alias: alias ?? sub.#alias ?? "derived",
		};
		return this;
	}

	/**
	 * Name this builder as a derived table / SELECT subquery — Lucid/Knex `.as()`.
	 * `db.from((s) => s.from('exams').sum('marks as total').as('totals'))` or
	 * `parent.select(db.from('logins').select('ip').limit(1).as('last_ip'))`.
	 */
	as(alias: string): this {
		this.#alias = alias;
		return this;
	}

	/** Run `cb` against a fresh sub-builder (same executor/dialect) and return it. */
	#buildSub(cb: (query: DatabaseQueryBuilder) => void): DatabaseQueryBuilder {
		const sub = new DatabaseQueryBuilder(this.#exec, this.#dialect);
		cb(sub);
		return sub;
	}

	/** Select the table for a write (Lucid `db.table`). Alias of {@link from}. */
	table(table: string): this {
		this.#table = table;
		return this;
	}

	/**
	 * Add columns to the SELECT list (Lucid/Knex `select`). Accepts bare names,
	 * arrays, and `{ alias: 'column' }` objects for aliasing —
	 * `select('id', ['name', 'email'], { total: 'COUNT(*)' })`.
	 */
	select(
		...columns: Array<
			string | string[] | Record<string, string> | RawSql | DatabaseQueryBuilder
		>
	): this {
		for (const col of columns) {
			if (typeof col === "string") {
				this.#selects.push(col);
			} else if (Array.isArray(col)) {
				this.#selects.push(...col);
			} else if (col instanceof RawSql) {
				// Lucid `select(db.raw(sql, bindings))` — verbatim fragment + params.
				this.#selectRaw.push({ sql: col.sql, params: [...col.params] });
			} else if (col instanceof DatabaseQueryBuilder) {
				// Lucid `select(subquery.as('alias'))` — a correlated subquery column.
				const alias = col.#alias;
				if (!alias) {
					throw new Error(
						"select(subquery) requires the subquery to be named with .as('alias')",
					);
				}
				const { sql, params } = col.toSQL();
				this.#selectRaw.push({
					sql: `(${sql}) AS ${this.#quoteAlias(alias)}`,
					params,
				});
			} else {
				for (const [alias, expr] of Object.entries(col)) {
					this.#selects.push(`${expr} AS ${alias}`);
				}
			}
		}
		return this;
	}

	/** Validate + dialect-quote a bare alias identifier. */
	#quoteAlias(alias: string): string {
		if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
			throw new Error(`Invalid alias '${alias}' — expected a bare identifier.`);
		}
		const q = this.#dialect === "mysql" ? "`" : '"';
		return `${q}${alias}${q}`;
	}

	/** A parenthesised group of conditions, built on a sub-builder (Lucid `where(cb)`). */
	where(callback: (query: DatabaseQueryBuilder) => void): this;
	/** Every key as an AND equality (Lucid/Knex `where({ a: 1, b: 2 })`). */
	where(conditions: Record<string, unknown>): this;
	where(column: string, value: unknown): this;
	where(column: string, operator: WhereOperator, value: unknown): this;
	where(
		columnOrCbOrObj:
			| string
			| ((query: DatabaseQueryBuilder) => void)
			| Record<string, unknown>,
		operatorOrValue?: WhereOperator | unknown,
		value?: unknown,
	): this {
		return this.#where("and", columnOrCbOrObj, operatorOrValue, value);
	}

	/** OR WHERE — joins the previous condition with OR (Lucid/Knex `orWhere`). */
	orWhere(callback: (query: DatabaseQueryBuilder) => void): this;
	orWhere(conditions: Record<string, unknown>): this;
	orWhere(column: string, value: unknown): this;
	orWhere(column: string, operator: WhereOperator, value: unknown): this;
	orWhere(
		columnOrCbOrObj:
			| string
			| ((query: DatabaseQueryBuilder) => void)
			| Record<string, unknown>,
		operatorOrValue?: WhereOperator | unknown,
		value?: unknown,
	): this {
		return this.#where("or", columnOrCbOrObj, operatorOrValue, value);
	}

	#where(
		boolean: "and" | "or",
		columnOrCbOrObj:
			| string
			| ((query: DatabaseQueryBuilder) => void)
			| Record<string, unknown>,
		operatorOrValue?: WhereOperator | unknown,
		value?: unknown,
	): this {
		if (typeof columnOrCbOrObj === "function") {
			// Parenthesised group: collect the callback's wheres on a sub-builder.
			const sub = new DatabaseQueryBuilder(this.#exec, this.#dialect);
			columnOrCbOrObj(sub);
			this.#wheres.push({
				kind: "group",
				conditions: sub.#compiledWheres(),
				boolean,
			});
			return this;
		}
		if (typeof columnOrCbOrObj === "object") {
			for (const [col, val] of Object.entries(columnOrCbOrObj)) {
				this.#cmp(boolean, col, "=", val);
			}
			return this;
		}
		return this.#pushBasic(boolean, columnOrCbOrObj, operatorOrValue, value);
	}

	#pushBasic(
		boolean: "and" | "or",
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		if (value === undefined) {
			this.#cmp(boolean, column, "=", operatorOrValue);
		} else {
			this.#cmp(boolean, column, operatorOrValue as WhereOperator, value);
		}
		return this;
	}

	/** Push a `col <op> value` comparison. */
	#cmp(
		boolean: "and" | "or",
		column: string,
		operator: string,
		value: unknown,
	): void {
		this.#wheres.push({ kind: "cmp", column, operator, value, boolean });
	}

	/** WHERE col NOT IN (…) — a value list, a subquery, OR a tuple (Lucid `whereNotIn`). */
	whereNotIn(column: string, values: unknown[]): this;
	whereNotIn(column: string, subquery: SubqueryArg): this;
	whereNotIn(columns: string[], rows: unknown[][]): this;
	whereNotIn(
		column: string | string[],
		arg: unknown[] | unknown[][] | SubqueryArg,
	): this {
		if (Array.isArray(column)) {
			const rows: unknown[][] = (Array.isArray(arg) ? arg : []).map((r) =>
				Array.isArray(r) ? r : [r],
			);
			return this.#pushInTuple("and", true, column, rows);
		}
		if (arg instanceof DatabaseQueryBuilder || typeof arg === "function") {
			return this.#pushInSub("and", true, column, arg);
		}
		this.#cmp("and", column, "NOT IN", arg);
		return this;
	}

	/** WHERE col BETWEEN ? AND ? — inclusive (Lucid/Knex `whereBetween`). */
	whereBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#cmp("and", column, "BETWEEN", [...range]);
		return this;
	}

	/** WHERE col NOT BETWEEN ? AND ? (Lucid/Knex `whereNotBetween`). */
	whereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#cmp("and", column, "NOT BETWEEN", [...range]);
		return this;
	}

	/** WHERE col LIKE ? — case-sensitive (Lucid/Knex `whereLike`). */
	whereLike(column: string, pattern: string): this {
		this.#cmp("and", column, "LIKE", pattern);
		return this;
	}

	/** WHERE col ILIKE ? — case-insensitive; compiled to LOWER(..) LIKE on sqlite/mysql. */
	whereILike(column: string, pattern: string): this {
		this.#cmp("and", column, "ILIKE", pattern);
		return this;
	}

	/** A raw WHERE fragment with `?` bindings (Lucid/Knex `whereRaw`). */
	whereRaw(sql: string, bindings: unknown[] = []): this {
		return this.#pushRaw("and", false, sql, bindings);
	}

	/** Alias of {@link whereRaw} — AND is the default (Lucid `andWhereRaw`). */
	andWhereRaw(sql: string, bindings: unknown[] = []): this {
		return this.#pushRaw("and", false, sql, bindings);
	}

	/** OR-combined raw WHERE fragment (Lucid `orWhereRaw`). */
	orWhereRaw(sql: string, bindings: unknown[] = []): this {
		return this.#pushRaw("or", false, sql, bindings);
	}

	/** WHERE NOT (raw fragment) (Lucid `whereNotRaw`). */
	whereNotRaw(sql: string, bindings: unknown[] = []): this {
		return this.#pushRaw("and", true, sql, bindings);
	}

	/** Alias of {@link whereNotRaw} (Lucid `andWhereNotRaw`). */
	andWhereNotRaw(sql: string, bindings: unknown[] = []): this {
		return this.#pushRaw("and", true, sql, bindings);
	}

	/** OR NOT (raw fragment) (Lucid `orWhereNotRaw`). */
	orWhereNotRaw(sql: string, bindings: unknown[] = []): this {
		return this.#pushRaw("or", true, sql, bindings);
	}

	#pushRaw(
		boolean: "and" | "or",
		negated: boolean,
		sql: string,
		bindings: unknown[],
	): this {
		this.#wheres.push({
			kind: "raw",
			sql: negated ? `NOT (${sql})` : sql,
			bindings,
			boolean,
		});
		return this;
	}

	/** WHERE left <op> right — both COLUMNS (Lucid/Knex `whereColumn`). */
	whereColumn(left: string, operator: string, right: string): this {
		return this.#pushColumn("and", false, left, operator, right);
	}

	/** Alias of {@link whereColumn} — AND is the default (Lucid `andWhereColumn`). */
	andWhereColumn(left: string, operator: string, right: string): this {
		return this.#pushColumn("and", false, left, operator, right);
	}

	/** OR left <op> right — both COLUMNS (Lucid `orWhereColumn`). */
	orWhereColumn(left: string, operator: string, right: string): this {
		return this.#pushColumn("or", false, left, operator, right);
	}

	/** WHERE NOT (left <op> right) — both COLUMNS (Lucid `whereNotColumn`). */
	whereNotColumn(left: string, operator: string, right: string): this {
		return this.#pushColumn("and", true, left, operator, right);
	}

	/** Alias of {@link whereNotColumn} (Lucid `andWhereNotColumn`). */
	andWhereNotColumn(left: string, operator: string, right: string): this {
		return this.#pushColumn("and", true, left, operator, right);
	}

	/** OR NOT (left <op> right) — both COLUMNS (Lucid `orWhereNotColumn`). */
	orWhereNotColumn(left: string, operator: string, right: string): this {
		return this.#pushColumn("or", true, left, operator, right);
	}

	#pushColumn(
		boolean: "and" | "or",
		negated: boolean,
		left: string,
		operator: string,
		right: string,
	): this {
		const ops = new Set(["=", "!=", "<>", "<", ">", "<=", ">="]);
		if (!ops.has(operator)) {
			throw new Error(`whereColumn: unsupported operator '${operator}'`);
		}
		const base = `${this.#quoteIdent(left)} ${operator} ${this.#quoteIdent(right)}`;
		const sql = negated ? `NOT (${base})` : base;
		this.#wheres.push({ kind: "raw", sql, bindings: [], boolean });
		return this;
	}

	/** WHERE EXISTS (subquery) — a builder or a callback (Lucid/Knex `whereExists`). */
	whereExists(sub: SubqueryArg): this {
		return this.#pushExists("and", false, sub);
	}

	/** Alias of {@link whereExists} — AND is the default (Lucid `andWhereExists`). */
	andWhereExists(sub: SubqueryArg): this {
		return this.#pushExists("and", false, sub);
	}

	/** WHERE NOT EXISTS (subquery). */
	whereNotExists(sub: SubqueryArg): this {
		return this.#pushExists("and", true, sub);
	}

	/** Alias of {@link whereNotExists} (Lucid `andWhereNotExists`). */
	andWhereNotExists(sub: SubqueryArg): this {
		return this.#pushExists("and", true, sub);
	}

	/** OR EXISTS (subquery). */
	orWhereExists(sub: SubqueryArg): this {
		return this.#pushExists("or", false, sub);
	}

	/** OR NOT EXISTS (subquery) (Lucid `orWhereNotExists`). */
	orWhereNotExists(sub: SubqueryArg): this {
		return this.#pushExists("or", true, sub);
	}

	#pushExists(boolean: "and" | "or", negated: boolean, sub: SubqueryArg): this {
		this.#wheres.push({
			kind: "exists",
			negated,
			subquery: this.#resolveSub(sub).#selectSpec(),
			boolean,
		});
		return this;
	}

	/** WHERE json `$.path` <op> value (Lucid/Knex `whereJsonPath`). */
	whereJsonPath(
		column: string,
		path: string,
		operator: string,
		value: unknown,
	): this {
		this.#wheres.push({
			kind: "json",
			jsonOp: "path",
			column,
			negated: false,
			path,
			operator,
			value,
			boolean: "and",
		});
		return this;
	}

	/** WHERE json column `@>` value — contains (Postgres/MySQL; `whereJsonSupersetOf`). */
	whereJsonSupersetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", false, "superset", column, value);
	}
	/** Lucid alias of {@link whereJsonSupersetOf} (`whereJsonSuperset`). */
	whereJsonSuperset(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", false, "superset", column, value);
	}
	/** OR json `@>` (Lucid `orWhereJsonSuperset`). */
	orWhereJsonSupersetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", false, "superset", column, value);
	}
	orWhereJsonSuperset(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", false, "superset", column, value);
	}
	/** WHERE NOT json `@>` (Lucid `whereNotJsonSuperset`). */
	whereNotJsonSupersetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", true, "superset", column, value);
	}
	whereNotJsonSuperset(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", true, "superset", column, value);
	}
	/** OR NOT json `@>` (Lucid `orWhereNotJsonSuperset`). */
	orWhereNotJsonSupersetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", true, "superset", column, value);
	}
	orWhereNotJsonSuperset(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", true, "superset", column, value);
	}

	/** WHERE json column `<@` value — contained by (`whereJsonSubsetOf`). */
	whereJsonSubsetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", false, "subset", column, value);
	}
	/** Lucid alias of {@link whereJsonSubsetOf} (`whereJsonSubset`). */
	whereJsonSubset(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", false, "subset", column, value);
	}
	/** OR json `<@` (Lucid `orWhereJsonSubset`). */
	orWhereJsonSubsetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", false, "subset", column, value);
	}
	orWhereJsonSubset(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", false, "subset", column, value);
	}
	/** WHERE NOT json `<@` (Lucid `whereNotJsonSubset`). */
	whereNotJsonSubsetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", true, "subset", column, value);
	}
	whereNotJsonSubset(column: string, value: unknown): this {
		return this.#pushJsonContainment("and", true, "subset", column, value);
	}
	/** OR NOT json `<@` (Lucid `orWhereNotJsonSubset`). */
	orWhereNotJsonSubsetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", true, "subset", column, value);
	}
	orWhereNotJsonSubset(column: string, value: unknown): this {
		return this.#pushJsonContainment("or", true, "subset", column, value);
	}

	#pushJsonContainment(
		boolean: "and" | "or",
		negated: boolean,
		jsonOp: "superset" | "subset",
		column: string,
		value: unknown,
	): this {
		this.#wheres.push({
			kind: "json",
			jsonOp,
			column,
			negated,
			value: typeof value === "string" ? value : JSON.stringify(value),
			boolean,
		});
		return this;
	}

	/** ORDER BY <raw> — keeps its position among orderBy terms (Lucid `orderByRaw`). */
	orderByRaw(sql: string): this {
		this.#orderBys.push({ raw: sql });
		return this;
	}

	/** GROUP BY <raw expression> (Lucid/Knex `groupByRaw`). */
	groupByRaw(sql: string): this {
		this.#groupBys.push(sql);
		return this;
	}

	/** HAVING <raw> with `?` bindings (Lucid/Knex `havingRaw`). */
	havingRaw(sql: string, bindings: unknown[] = []): this {
		this.#havings.push({ kind: "raw", sql, bindings, type: "and" });
		return this;
	}

	/** `INNER JOIN` — alias of {@link innerJoin} (Lucid/Knex `join`). */
	join(table: string, left: string, right: string): this;
	join(table: string, left: string, operator: string, right: string): this;
	join(table: string, build: (j: DbJoinBuilder) => void): this;
	join(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin("INNER", table, leftOrBuild, operatorOrRight, right);
	}

	/** `INNER JOIN table ON <left> [op] <right>` or a callback `ON` builder (Lucid/Knex). */
	innerJoin(table: string, left: string, right: string): this;
	innerJoin(table: string, left: string, operator: string, right: string): this;
	innerJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	innerJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin("INNER", table, leftOrBuild, operatorOrRight, right);
	}

	/** `LEFT JOIN table ON <left> [op] <right>` or a callback `ON` builder (Lucid/Knex). */
	leftJoin(table: string, left: string, right: string): this;
	leftJoin(table: string, left: string, operator: string, right: string): this;
	leftJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	leftJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin("LEFT", table, leftOrBuild, operatorOrRight, right);
	}

	/** `LEFT OUTER JOIN` — alias of {@link leftJoin} (Lucid/Knex `leftOuterJoin`). */
	leftOuterJoin(table: string, left: string, right: string): this;
	leftOuterJoin(
		table: string,
		left: string,
		operator: string,
		right: string,
	): this;
	leftOuterJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	leftOuterJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin("LEFT", table, leftOrBuild, operatorOrRight, right);
	}

	/** `RIGHT JOIN table ON <left> [op] <right>` or a callback `ON` builder (Lucid/Knex). */
	rightJoin(table: string, left: string, right: string): this;
	rightJoin(table: string, left: string, operator: string, right: string): this;
	rightJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	rightJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin("RIGHT", table, leftOrBuild, operatorOrRight, right);
	}

	/** `RIGHT OUTER JOIN` — alias of {@link rightJoin} (Lucid/Knex `rightOuterJoin`). */
	rightOuterJoin(table: string, left: string, right: string): this;
	rightOuterJoin(
		table: string,
		left: string,
		operator: string,
		right: string,
	): this;
	rightOuterJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	rightOuterJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin("RIGHT", table, leftOrBuild, operatorOrRight, right);
	}

	/** `FULL OUTER JOIN table ON …` (Lucid/Knex `fullOuterJoin`; Postgres — MySQL/SQLite lack it). */
	fullOuterJoin(table: string, left: string, right: string): this;
	fullOuterJoin(
		table: string,
		left: string,
		operator: string,
		right: string,
	): this;
	fullOuterJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	fullOuterJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		return this.#pushJoin(
			"FULL OUTER",
			table,
			leftOrBuild,
			operatorOrRight,
			right,
		);
	}

	/** `INNER JOIN table ON <left> = <right>` — quoted equi-join sugar (Lucid `joinOn`). */
	joinOn(table: string, left: string, right: string): this {
		return this.#pushJoin("INNER", table, left, right);
	}

	/** `CROSS JOIN table` (Lucid/Knex `crossJoin`). */
	crossJoin(table: string): this {
		this.#joins.push({
			sql: `CROSS JOIN ${this.#quoteJoinRef(table)}`,
			params: [],
		});
		return this;
	}

	/** Validate + quote a `[[schema.]table.]column` join reference (up to 3 segments). */
	#quoteJoinRef(ref: string): string {
		if (!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/.test(ref)) {
			throw new Error(
				`Invalid join/column identifier '${ref}' — expected [[schema.]table.]column. Use joinRaw() for anything else.`,
			);
		}
		const q = this.#dialect === "mysql" ? "`" : '"';
		return ref
			.split(".")
			.map((seg) => `${q}${seg}${q}`)
			.join(".");
	}

	/**
	 * Build a JOIN from the string forms — 3-arg `(left, right)` or 4-arg
	 * `(left, operator, right)` — or the callback `ON` builder (Lucid/Knex).
	 */
	#pushJoin(
		kind: "INNER" | "LEFT" | "RIGHT" | "FULL OUTER",
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		operatorOrRight?: string,
		right?: string,
	): this {
		const tq = this.#quoteJoinRef(table);
		if (typeof leftOrBuild === "function") {
			const parts: JoinPart[] = [];
			const jb: DbJoinBuilder = {
				on(l: string, opOrR: string, r?: string) {
					parts.push(
						r === undefined
							? { kind: "and", left: l, right: opOrR }
							: { kind: "and", left: l, operator: opOrR, right: r },
					);
					return jb;
				},
				andOn(l: string, opOrR: string, r?: string) {
					parts.push(
						r === undefined
							? { kind: "and", left: l, right: opOrR }
							: { kind: "and", left: l, operator: opOrR, right: r },
					);
					return jb;
				},
				orOn(l: string, opOrR: string, r?: string) {
					parts.push(
						r === undefined
							? { kind: "or", left: l, right: opOrR }
							: { kind: "or", left: l, operator: opOrR, right: r },
					);
					return jb;
				},
				onVal(l, v) {
					parts.push({ kind: "and", left: l, value: { v } });
					return jb;
				},
				andOnVal(l, v) {
					parts.push({ kind: "and", left: l, value: { v } });
					return jb;
				},
				orOnVal(l, v) {
					parts.push({ kind: "or", left: l, value: { v } });
					return jb;
				},
				onIn(l, values) {
					parts.push({ kind: "and", left: l, values: [...values] });
					return jb;
				},
				onNotIn(l, values) {
					parts.push({
						kind: "and",
						left: l,
						values: [...values],
						notIn: true,
					});
					return jb;
				},
				onNull(l) {
					parts.push({ kind: "and", left: l, nullOp: "IS NULL" });
					return jb;
				},
				onNotNull(l) {
					parts.push({ kind: "and", left: l, nullOp: "IS NOT NULL" });
					return jb;
				},
				onBetween(l, range) {
					parts.push({ kind: "and", left: l, between: [range[0], range[1]] });
					return jb;
				},
				onNotBetween(l, range) {
					parts.push({
						kind: "and",
						left: l,
						between: [range[0], range[1]],
						notBetween: true,
					});
					return jb;
				},
				onExists: (sub) => {
					const { sql, params } = this.#resolveSub(sub).toSQL();
					parts.push({ kind: "and", exists: { sql, params, not: false } });
					return jb;
				},
				onNotExists: (sub) => {
					const { sql, params } = this.#resolveSub(sub).toSQL();
					parts.push({ kind: "and", exists: { sql, params, not: true } });
					return jb;
				},
			};
			leftOrBuild(jb);
			const { sql: on, params } = this.#compileJoinParts(parts);
			this.#joins.push({ sql: `${kind} JOIN ${tq} ${on}`, params });
			return this;
		}
		// String form: 3-arg `(left, right)` or 4-arg `(left, operator, right)`.
		const left = leftOrBuild;
		const operator = right === undefined ? "=" : (operatorOrRight ?? "=");
		const rightCol = right === undefined ? operatorOrRight : right;
		if (rightCol === undefined) {
			throw new Error(
				"join() string form requires both left and right operands",
			);
		}
		this.#joins.push({
			sql: `${kind} JOIN ${tq} ON ${this.#quoteJoinRef(left)} ${this.#validateJoinOp(operator)} ${this.#quoteJoinRef(rightCol)}`,
			params: [],
		});
		return this;
	}

	/** Render accumulated `ON` parts to SQL + ordered bound params. */
	#compileJoinParts(parts: JoinPart[]): { sql: string; params: unknown[] } {
		const params: unknown[] = [];
		const sql = parts
			.map((p, i) => {
				const prefix = i === 0 ? "ON" : p.kind === "or" ? "OR" : "AND";
				if (p.exists) {
					params.push(...p.exists.params);
					return `${prefix} ${p.exists.not ? "NOT EXISTS" : "EXISTS"} (${p.exists.sql})`;
				}
				const col = this.#quoteJoinRef(p.left ?? "");
				if (p.nullOp) {
					return `${prefix} ${col} ${p.nullOp}`;
				}
				if (p.between) {
					params.push(p.between[0], p.between[1]);
					return `${prefix} ${col} ${p.notBetween ? "NOT BETWEEN" : "BETWEEN"} ? AND ?`;
				}
				if (p.values) {
					const placeholders = p.values.map(() => "?").join(", ");
					params.push(...p.values);
					return `${prefix} ${col} ${p.notIn ? "NOT IN" : "IN"} (${placeholders})`;
				}
				if (p.value) {
					params.push(p.value.v);
					return `${prefix} ${col} ${this.#validateJoinOp(p.operator ?? "=")} ?`;
				}
				return `${prefix} ${col} ${this.#validateJoinOp(p.operator ?? "=")} ${this.#quoteJoinRef(p.right ?? "")}`;
			})
			.join(" ");
		return { sql, params };
	}

	/** Allowlist the comparison operator embedded verbatim into a JOIN's ON SQL. */
	#validateJoinOp(op: string): string {
		const t = op.trim();
		const up = t.toUpperCase();
		const allowed = new Set([
			"=",
			"<>",
			"!=",
			"<",
			"<=",
			">",
			">=",
			"LIKE",
			"NOT LIKE",
			"ILIKE",
		]);
		if (allowed.has(t)) return t;
		if (allowed.has(up)) return up;
		throw new Error(`Unsupported join operator '${op}'.`);
	}

	/** A sub-query passed as a builder, or built in a callback (Lucid accepts both). */
	#resolveSub(sub: SubqueryArg): DatabaseQueryBuilder {
		return typeof sub === "function" ? this.#buildSub(sub) : sub;
	}

	/** `UNION` with another query — a builder or a callback (Lucid/Knex `union`). */
	union(sub: SubqueryArg): this {
		return this.#pushUnion(this.#resolveSub(sub), false);
	}

	/** `UNION ALL` (Lucid/Knex `unionAll`). */
	unionAll(sub: SubqueryArg): this {
		return this.#pushUnion(this.#resolveSub(sub), true, "union");
	}

	/** `INTERSECT` — rows present in both queries (Lucid/Knex `intersect`). */
	intersect(sub: SubqueryArg): this {
		return this.#pushUnion(this.#resolveSub(sub), false, "intersect");
	}

	/** `INTERSECT ALL` — duplicate-preserving {@link intersect} (Postgres/MySQL). */
	intersectAll(sub: SubqueryArg): this {
		return this.#pushUnion(this.#resolveSub(sub), true, "intersect");
	}

	/** `EXCEPT` — rows in this query but not the other (Lucid/Knex `except`). */
	except(sub: SubqueryArg): this {
		return this.#pushUnion(this.#resolveSub(sub), false, "except");
	}

	/** `EXCEPT ALL` — duplicate-preserving {@link except} (Postgres/MySQL). */
	exceptAll(sub: SubqueryArg): this {
		return this.#pushUnion(this.#resolveSub(sub), true, "except");
	}

	#pushUnion(
		sub: DatabaseQueryBuilder,
		all: boolean,
		op: "union" | "intersect" | "except" = "union",
	): this {
		const { sql, params } = sub.toSQL();
		this.#unions.push({ sql, params, all, op });
		return this;
	}

	/**
	 * `WITH name AS (subquery)` common table expression (Lucid/Knex `with`). The
	 * body is a pre-built builder OR a callback that builds one.
	 */
	with(
		name: string,
		sub: SubqueryArg,
		options: {
			recursive?: boolean;
			materialized?: boolean;
			columns?: string[];
		} = {},
	): this {
		const { sql, params } = this.#resolveSub(sub).toSQL();
		this.#ctes.push({
			name,
			sql,
			params,
			recursive: options.recursive ?? false,
			materialized: options.materialized ?? null,
			columns: options.columns,
		});
		return this;
	}

	/**
	 * `WITH RECURSIVE name[(cols)] AS (subquery)` (Lucid/Knex `withRecursive`).
	 * The optional `columns` list restricts/names the CTE's output columns.
	 */
	withRecursive(name: string, sub: SubqueryArg, columns?: string[]): this {
		return this.with(name, sub, { recursive: true, columns });
	}

	/** `WITH name AS MATERIALIZED (subquery)` — Postgres (Lucid/Knex `withMaterialized`). */
	withMaterialized(name: string, sub: SubqueryArg): this {
		return this.with(name, sub, { materialized: true });
	}

	/** `WITH name AS NOT MATERIALIZED (subquery)` — Postgres (Lucid/Knex `withNotMaterialized`). */
	withNotMaterialized(name: string, sub: SubqueryArg): this {
		return this.with(name, sub, { materialized: false });
	}

	/**
	 * A deep copy of this builder — every accumulated clause is duplicated so
	 * mutating the clone never touches the original (Lucid/Knex `clone`). Shares
	 * only the executor + dialect.
	 */
	clone(): DatabaseQueryBuilder<T> {
		const c = new DatabaseQueryBuilder<T>(
			this.#exec,
			this.#dialect,
			this.#table,
		);
		c.#selects = [...this.#selects];
		c.#selectRaw = this.#selectRaw.map((s) => ({
			...s,
			params: [...s.params],
		}));
		c.#alias = this.#alias;
		c.#wheres = this.#wheres.map((w) => ({ ...w }));
		c.#orderBys = this.#orderBys.map((o) => ({ ...o }));
		c.#groupBys = [...this.#groupBys];
		c.#havings = this.#havings.map((h) => ({ ...h }));
		c.#joins = this.#joins.map((j) => ({ sql: j.sql, params: [...j.params] }));
		c.#unions = this.#unions.map((u) => ({ ...u, params: [...u.params] }));
		c.#ctes = this.#ctes.map((cte) => ({ ...cte, params: [...cte.params] }));
		c.#schema = this.#schema;
		c.#lockMode = this.#lockMode;
		c.#lockModifier = this.#lockModifier;
		c.#distinctOn = [...this.#distinctOn];
		c.#returningCols = [...this.#returningCols];
		c.#onConflictCols = this.#onConflictCols
			? [...this.#onConflictCols]
			: undefined;
		c.#mergeMode = this.#mergeMode;
		c.#mergeCols = [...this.#mergeCols];
		c.#mergeSet = this.#mergeSet
			? this.#mergeSet.map((s) => ({ ...s }))
			: undefined;
		c.#distinctFlag = this.#distinctFlag;
		c.#limit = this.#limit;
		c.#offset = this.#offset;
		c.#debug = this.#debug;
		c.#comments = [...this.#comments];
		c.#reporterData = this.#reporterData
			? { ...this.#reporterData }
			: undefined;
		c.#fromSubquery = this.#fromSubquery
			? { ...this.#fromSubquery, params: [...this.#fromSubquery.params] }
			: undefined;
		return c;
	}

	/** Qualify the table with a schema (Lucid/Knex `withSchema`). */
	withSchema(schema: string): this {
		this.#schema = schema;
		return this;
	}

	/**
	 * Wrap every WHERE clause added so far into its own parenthesised group, so
	 * subsequent clauses combine with the group rather than its inner conditions
	 * (Lucid `wrapExisting`): `q.where(a).orWhere(b).wrapExisting().where(c)` →
	 * `WHERE (a OR b) AND c`.
	 */
	wrapExisting(): this {
		if (this.#wheres.length > 0) {
			this.#wheres = [
				{
					kind: "group",
					conditions: this.#compiledWheres(),
					boolean: "and",
				},
			];
		}
		return this;
	}

	/** Log the compiled SQL + bindings to the console on the next run (Lucid/Knex `debug`). */
	debug(enabled = true): this {
		this.#debug = enabled;
		return this;
	}

	/**
	 * Set a caller-facing statement timeout in ms (Lucid `timeout(ms)`). Matches
	 * Lucid's DEFAULT (non-cancelling) timeout: the awaiting promise rejects after
	 * `ms` on the read paths (exec / first / pluck / aggregate). Server-side
	 * cancellation is not wired — the driver still runs the query to completion.
	 * Called with no argument it clears the timeout.
	 */
	timeout(ms?: number): this {
		this.#timeoutMs = ms;
		return this;
	}

	/**
	 * Race `work` against the configured `.timeout(ms)`. Rejects the awaiter after
	 * `ms`; the losing DB promise is swallowed so a post-timeout driver error never
	 * surfaces as an unhandled rejection. No timeout set → returns `work` as-is.
	 * Matches Lucid's DEFAULT (non-cancelling) timeout — the driver still completes
	 * the query server-side.
	 */
	#raceTimeout<R>(work: Promise<R>): Promise<R> {
		const ms = this.#timeoutMs;
		if (!ms || ms <= 0) return work;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const guard = new Promise<never>((_, reject) => {
			timer = setTimeout(
				() => reject(new Error(`Query timed out after ${ms}ms`)),
				ms,
			);
		});
		work.catch(() => {});
		return Promise.race([work, guard]).finally(() => clearTimeout(timer));
	}

	/** Prepend a `/* … *​/` SQL comment to the compiled query (Lucid/Knex `comment`). */
	comment(text: string): this {
		// Reject the comment terminator so a comment can never break out of `/* */`.
		if (text.includes("*/")) {
			throw new Error("comment() text may not contain '*/'");
		}
		this.#comments.push(text);
		return this;
	}

	/**
	 * Attach arbitrary metadata to the `db:query` event this query emits (Adonis
	 * Lucid `reporterData`) — a listener reads it off `event.reporterData`.
	 * Repeated calls merge; setting it forces emission so the data reaches a
	 * listener even when the connection has `debug: false`.
	 */
	reporterData(data: Record<string, unknown>): this {
		this.#reporterData = { ...this.#reporterData, ...data };
		this.#debug = true;
		return this;
	}

	/** QueryMeta carrying the debug/reporterData channel to the connection. */
	#queryMeta(method: string): QueryMeta {
		return {
			method,
			debug: this.#debug,
			reporterData: this.#reporterData,
		};
	}

	/** Validate + quote an identifier (optionally `table.column`) for the dialect. */
	#quoteIdent(name: string): string {
		if (!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(name)) {
			throw new Error(`unsafe identifier ${JSON.stringify(name)}`);
		}
		const q = this.#dialect === "mysql" ? "`" : '"';
		return name
			.split(".")
			.map((p) => `${q}${p}${q}`)
			.join(".");
	}

	/**
	 * SELECT DISTINCT (Lucid/Knex `distinct`). With columns, those are added to
	 * the SELECT list too — `distinct('a', 'b')` ≈ `SELECT DISTINCT a, b`.
	 */
	distinct(...columns: string[]): this {
		this.#distinctFlag = true;
		if (columns.length > 0) this.#selects.push(...columns);
		return this;
	}

	/** Postgres `SELECT DISTINCT ON (cols)` (Lucid/Knex `distinctOn`). */
	distinctOn(...columns: string[]): this {
		this.#distinctOn.push(...columns);
		return this;
	}

	/** GROUP BY columns (Lucid/Knex `groupBy`). */
	groupBy(...columns: string[]): this {
		this.#groupBys.push(...columns);
		return this;
	}

	/** HAVING condition after GROUP BY (Lucid/Knex `having`). */
	having(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		this.#havings.push(
			value === undefined
				? { column, operator: "=", value: operatorOrValue, type: "and" }
				: {
						column,
						operator: operatorOrValue as WhereOperator,
						value,
						type: "and",
					},
		);
		return this;
	}

	/** OR-combined HAVING condition (Lucid/Knex `orHaving`). */
	orHaving(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		this.#havings.push(
			value === undefined
				? { column, operator: "=", value: operatorOrValue, type: "or" }
				: {
						column,
						operator: operatorOrValue as WhereOperator,
						value,
						type: "or",
					},
		);
		return this;
	}

	/** HAVING col IS NULL (Lucid/Knex `havingNull`). */
	havingNull(column: string): this {
		this.#havings.push({
			column,
			operator: "IS NULL",
			value: null,
			type: "and",
		});
		return this;
	}

	/** HAVING col IS NOT NULL (Lucid/Knex `havingNotNull`). */
	havingNotNull(column: string): this {
		this.#havings.push({
			column,
			operator: "IS NOT NULL",
			value: null,
			type: "and",
		});
		return this;
	}

	/** HAVING col IN (...) (Lucid/Knex `havingIn`). */
	havingIn(column: string, values: unknown[]): this {
		this.#havings.push({ column, operator: "IN", value: values, type: "and" });
		return this;
	}

	/** HAVING col NOT IN (...) (Lucid/Knex `havingNotIn`). */
	havingNotIn(column: string, values: unknown[]): this {
		this.#havings.push({
			column,
			operator: "NOT IN",
			value: values,
			type: "and",
		});
		return this;
	}

	/** HAVING col BETWEEN ? AND ? (Lucid/Knex `havingBetween`). */
	havingBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#havings.push({
			column,
			operator: "BETWEEN",
			value: [...range],
			type: "and",
		});
		return this;
	}

	/** HAVING col NOT BETWEEN ? AND ? (Lucid/Knex `havingNotBetween`). */
	havingNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#havings.push({
			column,
			operator: "NOT BETWEEN",
			value: [...range],
			type: "and",
		});
		return this;
	}

	/**
	 * WHERE col IN (…) — a value list, a subquery, OR a multi-column tuple
	 * (Lucid/Knex `whereIn`): `whereIn('id', [1,2])`, `whereIn('id', subquery)`,
	 * `whereIn(['a','b'], [[1,2],[3,4]])`.
	 */
	whereIn(column: string, values: unknown[]): this;
	whereIn(column: string, subquery: SubqueryArg): this;
	whereIn(columns: string[], rows: unknown[][]): this;
	whereIn(
		column: string | string[],
		arg: unknown[] | unknown[][] | SubqueryArg,
	): this {
		if (Array.isArray(column)) {
			const rows: unknown[][] = (Array.isArray(arg) ? arg : []).map((r) =>
				Array.isArray(r) ? r : [r],
			);
			return this.#pushInTuple("and", false, column, rows);
		}
		if (arg instanceof DatabaseQueryBuilder || typeof arg === "function") {
			return this.#pushInSub("and", false, column, arg);
		}
		this.#cmp("and", column, "IN", arg);
		return this;
	}

	#pushInSub(
		boolean: "and" | "or",
		negated: boolean,
		column: string,
		sub: SubqueryArg,
	): this {
		this.#wheres.push({
			kind: "inSub",
			column,
			negated,
			subquery: this.#resolveSub(sub).#selectSpec(),
			boolean,
		});
		return this;
	}

	#pushInTuple(
		boolean: "and" | "or",
		negated: boolean,
		columns: string[],
		rows: unknown[][],
	): this {
		this.#wheres.push({ kind: "inTuple", columns, rows, negated, boolean });
		return this;
	}

	whereNull(column: string): this {
		this.#cmp("and", column, "IS NULL", null);
		return this;
	}

	whereNotNull(column: string): this {
		this.#cmp("and", column, "IS NOT NULL", null);
		return this;
	}

	/**
	 * Negated WHERE (Lucid/Knex `whereNot`) — the same forms as {@link where}: a
	 * `(column, [operator,] value)` comparison, an object (`whereNot({ a: 1 })` →
	 * `a <> 1`), or a callback group (`whereNot((q) => …)` → `NOT (…)`).
	 */
	whereNot(callback: (query: DatabaseQueryBuilder) => void): this;
	whereNot(conditions: Record<string, unknown>): this;
	whereNot(column: string, value: unknown): this;
	whereNot(column: string, operator: WhereOperator, value: unknown): this;
	whereNot(
		columnOrCbOrObj:
			| string
			| ((query: DatabaseQueryBuilder) => void)
			| Record<string, unknown>,
		operatorOrValue?: WhereOperator | unknown,
		value?: unknown,
	): this {
		if (typeof columnOrCbOrObj === "function") {
			const sub = new DatabaseQueryBuilder(this.#exec, this.#dialect);
			columnOrCbOrObj(sub);
			this.#wheres.push({
				kind: "group",
				conditions: sub.#compiledWheres(),
				boolean: "and",
				negated: true,
			});
			return this;
		}
		if (typeof columnOrCbOrObj === "object") {
			for (const [col, val] of Object.entries(columnOrCbOrObj)) {
				this.#cmp("and", col, "<>", val);
			}
			return this;
		}
		if (value === undefined) {
			this.#cmp("and", columnOrCbOrObj, negateOperator("="), operatorOrValue);
		} else {
			this.#cmp(
				"and",
				columnOrCbOrObj,
				negateOperator(String(operatorOrValue)),
				value,
			);
		}
		return this;
	}

	/** OR col IN (...) (Lucid/Knex `orWhereIn`). */
	orWhereIn(column: string, values: unknown[]): this;
	orWhereIn(column: string, subquery: SubqueryArg): this;
	orWhereIn(column: string, valuesOrSub: unknown[] | SubqueryArg): this {
		if (
			valuesOrSub instanceof DatabaseQueryBuilder ||
			typeof valuesOrSub === "function"
		) {
			return this.#pushInSub("or", false, column, valuesOrSub);
		}
		this.#cmp("or", column, "IN", valuesOrSub);
		return this;
	}

	/** OR col NOT IN (...) (Lucid/Knex `orWhereNotIn`). */
	orWhereNotIn(column: string, values: unknown[]): this {
		this.#cmp("or", column, "NOT IN", values);
		return this;
	}

	/** OR col IS NULL (Lucid/Knex `orWhereNull`). */
	orWhereNull(column: string): this {
		this.#cmp("or", column, "IS NULL", null);
		return this;
	}

	/** OR col IS NOT NULL (Lucid/Knex `orWhereNotNull`). */
	orWhereNotNull(column: string): this {
		this.#cmp("or", column, "IS NOT NULL", null);
		return this;
	}

	/** OR col BETWEEN ? AND ? (Lucid/Knex `orWhereBetween`). */
	orWhereBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#cmp("or", column, "BETWEEN", [...range]);
		return this;
	}

	/** OR col NOT BETWEEN ? AND ? (Lucid/Knex `orWhereNotBetween`). */
	orWhereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#cmp("or", column, "NOT BETWEEN", [...range]);
		return this;
	}

	/** OR col LIKE ? (Lucid/Knex `orWhereLike`). */
	orWhereLike(column: string, pattern: string): this {
		this.#cmp("or", column, "LIKE", pattern);
		return this;
	}

	/** OR col ILIKE ? (Lucid/Knex `orWhereILike`). */
	orWhereILike(column: string, pattern: string): this {
		this.#cmp("or", column, "ILIKE", pattern);
		return this;
	}

	/** A raw JOIN fragment with `?` bindings (Lucid/Knex `joinRaw`). */
	joinRaw(sql: string, bindings: unknown[] = []): this {
		this.#joins.push({ sql, params: bindings });
		return this;
	}

	/** `FOR UPDATE` row lock (Lucid/Knex `forUpdate`). Dropped on SQLite. */
	forUpdate(): this {
		if (this.#dialect === "sqlite") {
			console.warn(
				"[atlas] forUpdate ignored on sqlite (no row-level lock support)",
			);
		} else {
			this.#lockMode = "FOR UPDATE";
		}
		return this;
	}

	/** `FOR SHARE` row lock (Lucid/Knex `forShare`). Dropped on SQLite. */
	forShare(): this {
		if (this.#dialect === "sqlite") {
			console.warn(
				"[atlas] forShare ignored on sqlite (no row-level lock support)",
			);
		} else {
			this.#lockMode = "FOR SHARE";
		}
		return this;
	}

	/** Postgres `FOR NO KEY UPDATE` — weaker lock that doesn't block FK checks (Lucid/Knex). */
	forNoKeyUpdate(): this {
		if (this.#dialect === "postgres") {
			this.#lockMode = "FOR NO KEY UPDATE";
		} else {
			console.warn(
				`[atlas] forNoKeyUpdate ignored on ${this.#dialect} (Postgres-only lock)`,
			);
		}
		return this;
	}

	/** Postgres `FOR KEY SHARE` — the weakest share lock (Lucid/Knex). */
	forKeyShare(): this {
		if (this.#dialect === "postgres") {
			this.#lockMode = "FOR KEY SHARE";
		} else {
			console.warn(
				`[atlas] forKeyShare ignored on ${this.#dialect} (Postgres-only lock)`,
			);
		}
		return this;
	}

	/** Append `SKIP LOCKED` — skip locked rows instead of waiting (Lucid/Knex). */
	skipLocked(): this {
		if (this.#dialect === "sqlite") {
			console.warn("[atlas] skipLocked ignored on sqlite (no row-level lock)");
		} else {
			this.#lockModifier = "SKIP LOCKED";
		}
		return this;
	}

	/** Append `NOWAIT` — error immediately on a locked row (Lucid/Knex). */
	noWait(): this {
		if (this.#dialect === "sqlite") {
			console.warn("[atlas] noWait ignored on sqlite (no row-level lock)");
		} else {
			this.#lockModifier = "NOWAIT";
		}
		return this;
	}

	/** Apply `cb` only when `condition` is truthy, else `elseCb` (Lucid `if`). */
	if(
		condition: unknown,
		cb: (query: this) => void,
		elseCb?: (query: this) => void,
	): this {
		if (condition) cb(this);
		else elseCb?.(this);
		return this;
	}

	/** Inverse of {@link if} — apply `cb` only when `condition` is falsy (Lucid `unless`). */
	unless(
		condition: unknown,
		cb: (query: this) => void,
		elseCb?: (query: this) => void,
	): this {
		if (!condition) cb(this);
		else elseCb?.(this);
		return this;
	}

	/** Apply the first `[guard, cb]` whose guard is truthy; a trailing bare cb is
	 * the default (Adonis Lucid `match`). */
	match(
		...blocks: Array<[unknown, (query: this) => void] | ((query: this) => void)>
	): this {
		for (const block of blocks) {
			if (typeof block === "function") {
				block(this);
				return this;
			}
			const [guard, cb] = block;
			if (guard) {
				cb(this);
				return this;
			}
		}
		return this;
	}

	/** ORDER BY a column, or an array of terms (Lucid/Knex `orderBy([...])`). */
	orderBy(column: string, direction?: "asc" | "desc"): this;
	orderBy(
		terms: Array<string | { column: string; order?: "asc" | "desc" }>,
	): this;
	orderBy(
		columnOrTerms:
			| string
			| Array<string | { column: string; order?: "asc" | "desc" }>,
		direction: "asc" | "desc" = "asc",
	): this {
		if (Array.isArray(columnOrTerms)) {
			for (const t of columnOrTerms) {
				if (typeof t === "string") {
					this.#orderBys.push({ column: t, direction: "asc" });
				} else {
					this.#orderBys.push({
						column: t.column,
						direction: t.order ?? "asc",
					});
				}
			}
			return this;
		}
		this.#orderBys.push({ column: columnOrTerms, direction });
		return this;
	}

	limit(n: number): this {
		this.#limit = n;
		return this;
	}

	offset(n: number): this {
		this.#offset = n;
		return this;
	}

	/** The table, qualified with a schema when {@link withSchema} was used. */
	#qualifiedTable(): string {
		return this.#schema ? `${this.#schema}.${this.#table}` : this.#table;
	}

	/** `FOR UPDATE`(+`SKIP LOCKED`/`NOWAIT`) or null — the modifier needs a base lock. */
	#composedLockMode(): string | null {
		if (!this.#lockMode) return null;
		return this.#lockModifier
			? `${this.#lockMode} ${this.#lockModifier}`
			: this.#lockMode;
	}

	/** LIMIT/OFFSET for a 1-based page (Lucid/Knex `forPage`). */
	forPage(page: number, perPage = 20): this {
		this.#limit = perPage;
		this.#offset = (Math.max(1, page) - 1) * perPage;
		return this;
	}

	/** Build the SELECT spec JSON directly (full grammar: joins, locks, raw, etc.). */
	#selectSpec(select?: string[]): Record<string, unknown> {
		return {
			kind: "select",
			table: this.#qualifiedTable(),
			fromSubquery: this.#fromSubquery ?? null,
			select: select ?? (this.#selects.length > 0 ? this.#selects : ["*"]),
			wheres: this.#compiledWheres(),
			orderBy: this.#orderBys,
			groupBy: this.#groupBys,
			having: this.#havings,
			limit: this.#limit ?? null,
			offset: this.#offset ?? null,
			distinct: this.#distinctFlag,
			distinctOn: this.#distinctOn,
			ctes: this.#ctes,
			unions: this.#unions,
			selectSubqueries: [],
			selectRaw: this.#selectRaw,
			joins: this.#joins,
			lockMode: this.#composedLockMode(),
		};
	}

	/**
	 * The compiled SELECT WITHOUT executing (Lucid `toSQL`). Returns both
	 * `bindings` (Lucid's name) and `params` (atlas's) — same array, so either
	 * name ports.
	 */
	toSQL(): { sql: string; bindings: unknown[]; params: unknown[] } {
		const compiled = compileStatementNative(this.#selectSpec(), this.#dialect);
		const prefix = this.#commentPrefix();
		const sql = prefix + compiled.statements[0];
		if (this.#debug) {
			console.debug("[atlas:sql]", sql, compiled.params);
		}
		return { sql, bindings: compiled.params, params: compiled.params };
	}

	/** Apply `cb` only on the given dialect(s) (Lucid `ifDialect`). */
	ifDialect(
		dialect: AtlasDialect | AtlasDialect[],
		cb: (query: this) => void,
	): this {
		const set = Array.isArray(dialect) ? dialect : [dialect];
		if (set.includes(this.#dialect)) cb(this);
		return this;
	}

	/** Apply `cb` on every dialect EXCEPT the given one(s) (Lucid `unlessDialect`). */
	unlessDialect(
		dialect: AtlasDialect | AtlasDialect[],
		cb: (query: this) => void,
	): this {
		const set = Array.isArray(dialect) ? dialect : [dialect];
		if (!set.includes(this.#dialect)) cb(this);
		return this;
	}

	/** Run the SELECT and return every row. */
	async exec(): Promise<T[]> {
		const { sql, params } = this.toSQL();
		return this.#raceTimeout(
			this.#exec.query<T>(sql, params, this.#queryMeta("exec")),
		);
	}

	/** Run the SELECT and return the first row (Lucid `first`), or `null`. */
	async first(): Promise<T | null> {
		this.#limit = 1;
		const rows = await this.exec();
		return rows[0] ?? null;
	}

	/** {@link first} but throws when no row matches (Lucid `firstOrFail`). */
	async firstOrFail(): Promise<T> {
		const row = await this.first();
		if (row === null) throw new Error("firstOrFail: no matching row");
		return row;
	}

	/** Return a single column's values across the result set (Lucid/Knex `pluck`). */
	async pluck(column: string): Promise<unknown[]> {
		const { sql, params } = this.toSQL();
		const rows = await this.#raceTimeout(
			this.#exec.query<Record<string, unknown>>(
				sql,
				params,
				this.#queryMeta("pluck"),
			),
		);
		return rows.map((r) => r[column]);
	}

	/** The parameterized SQL string (Lucid `toQuery`). */
	toQuery(): string {
		return this.toSQL().sql;
	}

	/** `{ sql, bindings }` — the compiled native query (Lucid `toNative`). */
	toNative(): { sql: string; bindings: unknown[] } {
		const { sql, params } = this.toSQL();
		return { sql, bindings: params };
	}

	/** Run a scalar aggregate (COUNT/SUM/AVG/MIN/MAX) over the current WHERE. */
	async #aggregate(expr: string): Promise<number> {
		const compiled = compileStatementNative(
			this.#selectSpec([`${expr} AS aggregate`]),
			this.#dialect,
		);
		const rows = await this.#raceTimeout(
			this.#exec.query<{ aggregate: number | string | null }>(
				compiled.statements[0],
				compiled.params,
				this.#queryMeta("aggregate"),
			),
		);
		return Number(rows[0]?.aggregate ?? 0);
	}

	/**
	 * Build a `FN(expr) AS alias` projection string from an `expr [as alias]` form.
	 * `'* as total'` → `COUNT(*) AS total`; `'amount'` → `SUM(amount)`.
	 */
	#aggProjection(fn: string, expr: string): string {
		const m = expr.match(/^(.*?)\s+as\s+(.+)$/i);
		return m ? `${fn}(${m[1].trim()}) AS ${m[2].trim()}` : `${fn}(${expr})`;
	}

	/**
	 * COUNT — terminal scalar with no argument (atlas DX: `await q.count()` → n),
	 * or a chainable projection with an aliased expression (Lucid/Knex:
	 * `q.count('* as total').groupBy(...)`).
	 */
	count(): Promise<number>;
	count(aliasExpr: `${string} as ${string}`): this;
	count(aliasExpr?: string): Promise<number> | this {
		if (aliasExpr === undefined) return this.#aggregate("COUNT(*)");
		this.#selects.push(this.#aggProjection("COUNT", aliasExpr));
		return this;
	}

	/** SUM — terminal scalar (`sum('amount')`) or chainable projection (`sum('amount as total')`). */
	sum(aliasExpr: `${string} as ${string}`): this;
	sum(column: string): Promise<number>;
	sum(expr: string): Promise<number> | this {
		return this.#aggMethod("SUM", expr);
	}

	/** AVG — terminal scalar or chainable projection (Lucid/Knex `avg`). */
	avg(aliasExpr: `${string} as ${string}`): this;
	avg(column: string): Promise<number>;
	avg(expr: string): Promise<number> | this {
		return this.#aggMethod("AVG", expr);
	}

	/** MIN — terminal scalar or chainable projection (Lucid/Knex `min`). */
	min(aliasExpr: `${string} as ${string}`): this;
	min(column: string): Promise<number>;
	min(expr: string): Promise<number> | this {
		return this.#aggMethod("MIN", expr);
	}

	/** MAX — terminal scalar or chainable projection (Lucid/Knex `max`). */
	max(aliasExpr: `${string} as ${string}`): this;
	max(column: string): Promise<number>;
	max(expr: string): Promise<number> | this {
		return this.#aggMethod("MAX", expr);
	}

	/** An aliased `expr` (`col as alias`) is a chainable projection; a bare column is a terminal scalar. */
	#aggMethod(fn: string, expr: string): Promise<number> | this {
		if (/\s+as\s+/i.test(expr)) {
			this.#selects.push(this.#aggProjection(fn, expr));
			return this;
		}
		return this.#aggregate(`${fn}(${expr})`);
	}

	/** COUNT(DISTINCT column) (Lucid/Knex `countDistinct`). */
	countDistinct(column: string): Promise<number> {
		return this.#aggregate(`COUNT(DISTINCT ${column})`);
	}

	/** SUM(DISTINCT column) (Lucid/Knex `sumDistinct`). */
	sumDistinct(column: string): Promise<number> {
		return this.#aggregate(`SUM(DISTINCT ${column})`);
	}

	/** AVG(DISTINCT column) (Lucid/Knex `avgDistinct`). */
	avgDistinct(column: string): Promise<number> {
		return this.#aggregate(`AVG(DISTINCT ${column})`);
	}

	/** WHERE clauses translated to the native compiler's JSON shapes. */
	#compiledWheres(): CompiledWhere[] {
		return this.#wheres.map((w): CompiledWhere => {
			switch (w.kind) {
				case "raw":
					return {
						kind: "raw",
						sql: w.sql,
						bindings: w.bindings,
						type: w.boolean,
					};
				case "exists":
					return {
						kind: "exists",
						negated: w.negated,
						subquery: w.subquery,
						type: w.boolean,
					};
				case "inSub":
					return {
						kind: "inSub",
						column: w.column,
						negated: w.negated,
						subquery: w.subquery,
						type: w.boolean,
					};
				case "inTuple":
					return {
						kind: "inTuple",
						columns: w.columns,
						rows: w.rows,
						negated: w.negated,
						type: w.boolean,
					};
				case "json":
					return {
						kind: "json",
						jsonOp: w.jsonOp,
						column: w.column,
						negated: w.negated,
						path: w.path,
						operator: w.operator,
						value: w.value,
						type: w.boolean,
					};
				case "group":
					return {
						kind: "group",
						conditions: w.conditions,
						type: w.boolean,
						negated: w.negated ?? false,
					};
				default:
					return {
						column: w.column,
						operator: w.operator,
						value: w.value,
						type: w.boolean,
					};
			}
		});
	}

	/**
	 * Columns to return from a subsequent insert/update/delete (Lucid `returning`).
	 * Accepts spread names, an array, or `'*'` — `returning('id')`,
	 * `returning(['id', 'created_at'])`, `returning('*')`.
	 */
	returning(...columns: Array<string | string[]>): this {
		for (const c of columns) {
			if (Array.isArray(c)) this.#returningCols.push(...c);
			else this.#returningCols.push(c);
		}
		return this;
	}

	/** The `/* … *​/` prefix for the compiled SQL (Lucid `comment`), or empty. */
	#commentPrefix(): string {
		return this.#comments.length > 0
			? `${this.#comments.map((c) => `/* ${c} */`).join(" ")} `
			: "";
	}

	/**
	 * Compile a DML spec (adding RETURNING when set) with the comment prefix —
	 * shared by the lazy builders' `.toSQL()` and their execution.
	 */
	#compileDmlSpec(spec: Record<string, unknown>): {
		sql: string;
		bindings: unknown[];
		params: unknown[];
	} {
		const withReturning =
			this.#returningCols.length > 0
				? { ...spec, returning: this.#returningCols }
				: spec;
		const compiled = compileStatementNative(withReturning, this.#dialect);
		const sql = this.#commentPrefix() + compiled.statements[0];
		return { sql, bindings: compiled.params, params: compiled.params };
	}

	/** Run a DML spec: RETURNING rows when set, else execute; `interpret` shapes the result. */
	async #runDml<R>(
		spec: Record<string, unknown>,
		interpret: (result: unknown, rows: Record<string, unknown>[] | null) => R,
	): Promise<R> {
		const { sql, params } = this.#compileDmlSpec(spec);
		const method = String(spec.kind ?? "dml");
		if (this.#returningCols.length > 0) {
			const rows = await this.#raceTimeout(
				this.#exec.query<Record<string, unknown>>(
					sql,
					params,
					this.#queryMeta(method),
				),
			);
			return interpret(null, rows);
		}
		const result = await this.#raceTimeout(
			this.#exec.execute(sql, params, this.#queryMeta(method)),
		);
		return interpret(result, null);
	}

	/** An insert/upsert result: RETURNING rows, else `[insertId]` (MySQL/SQLite) or `[]`. */
	readonly #interpretInsert = (
		result: unknown,
		rows: Record<string, unknown>[] | null,
	): Array<Record<string, unknown> | number> => {
		if (rows) return rows;
		if (this.#dialect === "mysql" || this.#dialect === "sqlite") {
			const id = lastInsertIdOf(result);
			if (id !== undefined) return [id];
		}
		return [];
	};

	/** An update/delete result: RETURNING rows, else the affected-row count. */
	readonly #interpretWrite = (
		result: unknown,
		rows: Record<string, unknown>[] | null,
	): number | Record<string, unknown>[] => {
		return rows ?? rowsAffected(result);
	};

	/** The chainable-clause hooks the lazy {@link DmlBuilder} delegates back to. */
	#dmlHooks(): DmlChainHooks {
		return {
			onConflict: (...c) => {
				this.onConflict(...c);
			},
			merge: (...a) => {
				this.merge(...a);
			},
			ignore: () => {
				this.ignore();
			},
			returning: (...c) => {
				this.returning(...c);
			},
			timeout: (ms) => {
				this.timeout(ms);
			},
			comment: (t) => {
				this.comment(t);
			},
		};
	}

	/**
	 * Conflict target for an upsert (Lucid/Knex `onConflict`). Accepts spread
	 * names, an array, or no argument (any unique constraint) —
	 * `onConflict('email')`, `onConflict(['email', 'tenant_id'])`, `onConflict()`.
	 */
	onConflict(...columns: Array<string | string[]>): this {
		this.#onConflictCols = columns.flat();
		return this;
	}

	/**
	 * On conflict, UPDATE columns (Lucid/Knex `merge`). No argument updates every
	 * insert column; spread names or an array update only those; an object sets
	 * custom values (scalars or `db.raw(...)` expressions) —
	 * `merge()`, `merge(['a', 'b'])`, `merge({ login_count: db.raw('users.login_count + 1') })`.
	 */
	merge(...args: Array<string | string[] | Record<string, unknown>>): this {
		this.#mergeMode = "merge";
		const cols: string[] = [];
		const set: Array<{
			column: string;
			value?: unknown;
			raw?: string;
			rawParams?: unknown[];
		}> = [];
		for (const a of args) {
			if (typeof a === "string") {
				cols.push(a);
			} else if (Array.isArray(a)) {
				cols.push(...a);
			} else {
				for (const [col, v] of Object.entries(a)) {
					if (v instanceof RawSql) {
						set.push({ column: col, raw: v.sql, rawParams: [...v.params] });
					} else {
						set.push({ column: col, value: v });
					}
				}
			}
		}
		this.#mergeCols = cols;
		this.#mergeSet = set.length > 0 ? set : undefined;
		return this;
	}

	/** On conflict, do nothing (Lucid/Knex `onConflict(...).ignore()`). */
	ignore(): this {
		this.#mergeMode = "ignore";
		return this;
	}

	/** Build the insert or upsert spec from the current onConflict/merge/returning state. */
	#buildInsertOrUpsertSpec(
		rows: Array<Array<[string, unknown]>>,
	): Record<string, unknown> {
		if (this.#onConflictCols) {
			const conflictColumns = this.#onConflictCols;
			const allCols = rows[0]?.map(([c]) => c) ?? [];
			const updateColumns =
				this.#mergeMode === "ignore"
					? []
					: this.#mergeCols.length > 0
						? this.#mergeCols
						: allCols.filter((c) => !conflictColumns.includes(c));
			return {
				kind: "upsert",
				table: this.#qualifiedTable(),
				rows,
				conflictColumns,
				updateColumns,
				updateSet: this.#mergeSet ?? [],
				ctes: this.#ctes,
			};
		}
		return {
			kind: "insert",
			table: this.#qualifiedTable(),
			rows,
			ctes: this.#ctes,
		};
	}

	#buildUpdateSpec(set: Array<[string, unknown]>): Record<string, unknown> {
		return {
			kind: "update",
			table: this.#qualifiedTable(),
			set,
			wheres: this.#compiledWheres(),
			ctes: this.#ctes,
		};
	}

	#buildDeleteSpec(): Record<string, unknown> {
		return {
			kind: "delete",
			table: this.#qualifiedTable(),
			wheres: this.#compiledWheres(),
			ctes: this.#ctes,
		};
	}

	/**
	 * Insert one row (Lucid `db.table(t).insert(data)`). Lazy + chainable: the
	 * statement runs on `await`/`.exec()`, so `insert(data).onConflict(...).merge()`,
	 * `insert(data).returning(...)` and `insert(data).toSQL()` all work. Resolves to
	 * the RETURNING rows, or `[insertId]` (MySQL/SQLite) / `[]` otherwise.
	 */
	insert(
		data: Record<string, unknown>,
	): DmlBuilder<Array<Record<string, unknown> | number>> {
		const rows = [Object.entries(data)];
		return new DmlBuilder(
			() =>
				rows[0].length === 0
					? Promise.resolve<Array<Record<string, unknown> | number>>([])
					: this.#runDml(
							this.#buildInsertOrUpsertSpec(rows),
							this.#interpretInsert,
						),
			() => this.#compileDmlSpec(this.#buildInsertOrUpsertSpec(rows)),
			this.#dmlHooks(),
		);
	}

	/** Insert many rows in one statement (Lucid/Knex `multiInsert`). Lazy + chainable. */
	multiInsert(
		rows: Array<Record<string, unknown>>,
	): DmlBuilder<Array<Record<string, unknown> | number>> {
		// Lucid fills missing keys with NULL — take the union of every row's
		// columns, then project each row onto it so all rows share one column set.
		const cols = Array.from(new Set(rows.flatMap((r) => Object.keys(r))));
		const rowEntries = rows.map((r) =>
			cols.map((c): [string, unknown] => [c, c in r ? r[c] : null]),
		);
		return new DmlBuilder(
			() =>
				rows.length === 0
					? Promise.resolve<Array<Record<string, unknown> | number>>([])
					: this.#runDml(
							this.#buildInsertOrUpsertSpec(rowEntries),
							this.#interpretInsert,
						),
			() => this.#compileDmlSpec(this.#buildInsertOrUpsertSpec(rowEntries)),
			this.#dmlHooks(),
		);
	}

	/**
	 * Update rows matching the current WHERE (Lucid/Knex `update`). Lazy + chainable
	 * (`update(data).returning(...)`, `.toSQL()`). Accepts a `{ col: value }` map OR
	 * a `(column, value)` pair; a value may be a `db.raw(...)` expression. Resolves to
	 * the affected count, or the RETURNING rows when {@link returning} is set.
	 */
	update(
		column: string,
		value: unknown,
	): DmlBuilder<number | Record<string, unknown>[]>;
	update(
		data: Record<string, unknown>,
	): DmlBuilder<number | Record<string, unknown>[]>;
	update(
		dataOrColumn: Record<string, unknown> | string,
		value?: unknown,
	): DmlBuilder<number | Record<string, unknown>[]> {
		const data =
			typeof dataOrColumn === "string"
				? { [dataOrColumn]: value }
				: dataOrColumn;
		const set: Array<[string, unknown]> = Object.entries(data).map(
			([col, v]): [string, unknown] =>
				v instanceof RawSql
					? [col, { raw: v.sql, rawParams: [...v.params] }]
					: [col, v],
		);
		return new DmlBuilder(
			() =>
				set.length === 0
					? Promise.resolve<number | Record<string, unknown>[]>(0)
					: this.#runDml(this.#buildUpdateSpec(set), this.#interpretWrite),
			() => this.#compileDmlSpec(this.#buildUpdateSpec(set)),
			this.#dmlHooks(),
		);
	}

	/** Delete rows matching the current WHERE (Lucid `delete`). Lazy + chainable. */
	delete(): DmlBuilder<number | Record<string, unknown>[]> {
		return new DmlBuilder(
			() => this.#runDml(this.#buildDeleteSpec(), this.#interpretWrite),
			() => this.#compileDmlSpec(this.#buildDeleteSpec()),
			this.#dmlHooks(),
		);
	}

	/** Alias of {@link delete} (Lucid/Knex `del`). */
	del(): DmlBuilder<number | Record<string, unknown>[]> {
		return this.delete();
	}

	/**
	 * Atomically add to a column (or a `{col: amount}` map) — `SET col = col + ?`,
	 * never read-modify-write (Lucid/Knex `increment`). Returns affected count.
	 */
	increment(column: string, amount?: number): Promise<number>;
	increment(patch: Record<string, number>): Promise<number>;
	increment(
		colOrPatch: string | Record<string, number>,
		amount = 1,
	): Promise<number> {
		return this.#runIncDec("increment", colOrPatch, amount);
	}

	/** Atomically subtract from a column (Lucid/Knex `decrement`). */
	decrement(column: string, amount?: number): Promise<number>;
	decrement(patch: Record<string, number>): Promise<number>;
	decrement(
		colOrPatch: string | Record<string, number>,
		amount = 1,
	): Promise<number> {
		return this.#runIncDec("decrement", colOrPatch, amount);
	}

	async #runIncDec(
		op: "increment" | "decrement",
		colOrPatch: string | Record<string, number>,
		amount: number,
	): Promise<number> {
		const patch =
			typeof colOrPatch === "string" ? { [colOrPatch]: amount } : colOrPatch;
		const set = Object.entries(patch).map(
			([col, value]) => [col, { op, value }] as [string, unknown],
		);
		if (set.length === 0) return 0;
		const compiled = compileStatementNative(
			{
				kind: "update",
				table: this.#qualifiedTable(),
				set,
				wheres: this.#compiledWheres(),
				ctes: this.#ctes,
			},
			this.#dialect,
		);
		const result = await this.#raceTimeout(
			this.#exec.execute(
				this.#commentPrefix() + compiled.statements[0],
				compiled.params,
				this.#queryMeta(op),
			),
		);
		return rowsAffected(result);
	}

	/**
	 * Offset paginate the current query (Lucid/Knex `paginate`). Runs a COUNT over
	 * the WHERE, then the page slice, and returns a {@link Paginator}.
	 */
	async paginate(page: number, perPage = 20): Promise<Paginator<T>> {
		const p = Math.max(1, Math.floor(page));
		const pp = Math.max(1, Math.floor(perPage));
		// COUNT over the WHERE only — ignore limit/offset/orderBy (they don't apply
		// to a total). Built directly so paginate() doesn't disturb builder state.
		const countCompiled = compileStatementNative(
			{
				kind: "select",
				table: this.#qualifiedTable(),
				select: ["COUNT(*) AS aggregate"],
				wheres: this.#compiledWheres(),
				orderBy: [],
				groupBy: this.#groupBys,
				having: this.#havings,
				limit: null,
				offset: null,
				distinct: this.#distinctFlag,
				distinctOn: this.#distinctOn,
				ctes: this.#ctes,
				unions: this.#unions,
				selectSubqueries: [],
				joins: this.#joins,
				lockMode: null,
			},
			this.#dialect,
		);
		const countRows = await this.#exec.query<{
			aggregate: number | string | null;
		}>(
			countCompiled.statements[0],
			countCompiled.params,
			this.#queryMeta("paginate"),
		);
		const total = Number(countRows[0]?.aggregate ?? 0);

		this.#limit = pp;
		this.#offset = (p - 1) * pp;
		const items = await this.exec();
		return new Paginator<T>(items, { total, perPage: pp, currentPage: p });
	}

	/** Thenable, so `await db.from('users').where(...)` resolves to the rows. */
	// biome-ignore lint/suspicious/noThenProperty: Adonis Lucid query builders are awaitable by design — `await db.from(t).where(...)` must resolve to the rows; the thenable is the intended public API.
	then<R1 = T[], R2 = never>(
		onfulfilled?: ((value: T[]) => R1 | PromiseLike<R1>) | null,
		onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
	): Promise<R1 | R2> {
		return this.exec().then(onfulfilled, onrejected);
	}
}

/** Flip a comparison operator for `whereNot` (Lucid/Knex negate the predicate). */
function negateOperator(op: string): string {
	const map: Record<string, string> = {
		"=": "!=",
		"!=": "=",
		"<>": "=",
		"<": ">=",
		">": "<=",
		"<=": ">",
		">=": "<",
		IN: "NOT IN",
		"NOT IN": "IN",
		LIKE: "NOT LIKE",
		"NOT LIKE": "LIKE",
		ILIKE: "NOT ILIKE",
		BETWEEN: "NOT BETWEEN",
		"NOT BETWEEN": "BETWEEN",
		"IS NULL": "IS NOT NULL",
		"IS NOT NULL": "IS NULL",
	};
	const negated = map[op.toUpperCase()] ?? map[op];
	if (!negated) throw new Error(`whereNot: unsupported operator '${op}'`);
	return negated;
}

/** Read an affected-row count from an execute() result, whatever its shape. */
function rowsAffected(result: unknown): number {
	if (
		result !== null &&
		typeof result === "object" &&
		"rowsAffected" in result &&
		typeof result.rowsAffected === "number"
	) {
		return result.rowsAffected;
	}
	return 0;
}

/** Read the auto-increment id off an execute outcome (MySQL/SQLite). */
function lastInsertIdOf(result: unknown): number | undefined {
	if (
		result !== null &&
		typeof result === "object" &&
		"lastInsertId" in result &&
		typeof result.lastInsertId === "number"
	) {
		return result.lastInsertId;
	}
	return undefined;
}
