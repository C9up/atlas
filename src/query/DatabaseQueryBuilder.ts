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

import { type AtlasDialect, compileStatementNative } from "./native.js";
import type { WhereOperator } from "./QueryBuilder.js";

/** The minimal execute/query surface a connection or transaction client offers. */
export interface QueryExecutor {
	query<T = Record<string, unknown>>(
		sql: string,
		params?: unknown[],
	): Promise<T[]>;
	execute(sql: string, params?: unknown[]): Promise<unknown>;
}

/** The Lucid query-builder entry points a transaction client exposes. */
export interface TransactionQueryBuilders {
	/** Query builder pre-selected on `table` (Lucid `trx.from`). */
	from(table: string): DatabaseQueryBuilder;
	/** Write builder pre-selected on `table` (Lucid `trx.table`). */
	table(table: string): DatabaseQueryBuilder;
	/** An insert builder (Lucid `trx.insertQuery()`). */
	insertQuery(): DatabaseQueryBuilder;
}

/** Build the `from`/`table`/`insertQuery` methods for a transaction client. */
export function makeTransactionQueryBuilders(
	exec: QueryExecutor,
	dialect: AtlasDialect,
): TransactionQueryBuilders {
	return {
		from: (table) => new DatabaseQueryBuilder(exec, dialect, table),
		table: (table) => new DatabaseQueryBuilder(exec, dialect, table),
		insertQuery: () => new DatabaseQueryBuilder(exec, dialect),
	};
}

/** One accumulated WHERE — a `col <op> value` comparison or a raw fragment. */
type WhereEntry =
	| {
			kind: "cmp";
			column: string;
			operator: string;
			value: unknown;
			boolean: "and" | "or";
	  }
	| { kind: "raw"; sql: string; bindings: unknown[]; boolean: "and" | "or" };

/** The native compiler's WHERE / HAVING entry shapes (camelCase JSON). */
type CompiledWhere =
	| { column: string; operator: string; value: unknown; type: "and" | "or" }
	| { kind: "raw"; sql: string; bindings: unknown[]; type: "and" | "or" };

/** A raw JOIN fragment (Knex/Lucid `joinRaw` / `innerJoin` / `leftJoin`). */
interface JoinEntry {
	sql: string;
	params: unknown[];
}

export class DatabaseQueryBuilder<T = Record<string, unknown>> {
	readonly #exec: QueryExecutor;
	readonly #dialect: AtlasDialect;
	#table: string;
	#selects: string[] = [];
	#wheres: WhereEntry[] = [];
	#orderBys: Array<{ column: string; direction: "asc" | "desc" }> = [];
	#groupBys: string[] = [];
	#havings: Array<{
		column: string;
		operator: string;
		value: unknown;
		type: "and" | "or";
	}> = [];
	#joins: JoinEntry[] = [];
	#lockMode?: string;
	#returningCols: string[] = [];
	#onConflictCols?: string[];
	#mergeMode?: "merge" | "ignore";
	#mergeCols: string[] = [];
	#distinctFlag = false;
	#limit?: number;
	#offset?: number;

	constructor(exec: QueryExecutor, dialect: AtlasDialect, table = "") {
		this.#exec = exec;
		this.#dialect = dialect;
		this.#table = table;
	}

	/** Select the table (Lucid `db.from`). */
	from(table: string): this {
		this.#table = table;
		return this;
	}

	/** Select the table for a write (Lucid `db.table`). Alias of {@link from}. */
	table(table: string): this {
		this.#table = table;
		return this;
	}

	select(...columns: string[]): this {
		this.#selects.push(...columns);
		return this;
	}

	where(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		return this.#pushBasic("and", column, operatorOrValue, value);
	}

	/** OR WHERE — joins the previous condition with OR (Lucid/Knex `orWhere`). */
	orWhere(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		return this.#pushBasic("or", column, operatorOrValue, value);
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

	/** WHERE col NOT IN (...) (Lucid/Knex `whereNotIn`). */
	whereNotIn(column: string, values: unknown[]): this {
		this.#cmp("and", column, "NOT IN", values);
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
		this.#wheres.push({ kind: "raw", sql, bindings, boolean: "and" });
		return this;
	}

	/** OR-combined raw WHERE fragment. */
	orWhereRaw(sql: string, bindings: unknown[] = []): this {
		this.#wheres.push({ kind: "raw", sql, bindings, boolean: "or" });
		return this;
	}

	/** SELECT DISTINCT (Lucid/Knex `distinct`). */
	distinct(): this {
		this.#distinctFlag = true;
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

	whereIn(column: string, values: unknown[]): this {
		this.#cmp("and", column, "IN", values);
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

	/** A raw JOIN fragment with `?` bindings (Lucid/Knex `joinRaw`). */
	joinRaw(sql: string, bindings: unknown[] = []): this {
		this.#joins.push({ sql, params: bindings });
		return this;
	}

	/** `INNER JOIN table ON <on>` (Lucid/Knex `innerJoin`; `on` is a raw predicate). */
	innerJoin(table: string, on: string): this {
		this.#joins.push({ sql: `INNER JOIN ${table} ON ${on}`, params: [] });
		return this;
	}

	/** `LEFT JOIN table ON <on>` (Lucid/Knex `leftJoin`). */
	leftJoin(table: string, on: string): this {
		this.#joins.push({ sql: `LEFT JOIN ${table} ON ${on}`, params: [] });
		return this;
	}

	/** `FOR UPDATE` row lock (Lucid/Knex `forUpdate`). Dropped on SQLite. */
	forUpdate(): this {
		this.#lockMode = "for_update";
		return this;
	}

	/** `FOR SHARE` row lock (Lucid/Knex `forShare`). Dropped on SQLite. */
	forShare(): this {
		this.#lockMode = "for_share";
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

	orderBy(column: string, direction: "asc" | "desc" = "asc"): this {
		this.#orderBys.push({ column, direction });
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

	/** Build the SELECT spec JSON directly (full grammar: joins, locks, raw, etc.). */
	#selectSpec(select?: string[]): Record<string, unknown> {
		return {
			kind: "select",
			table: this.#table,
			select: select ?? (this.#selects.length > 0 ? this.#selects : ["*"]),
			wheres: this.#compiledWheres(),
			orderBy: this.#orderBys,
			groupBy: this.#groupBys,
			having: this.#havings,
			limit: this.#limit ?? null,
			offset: this.#offset ?? null,
			distinct: this.#distinctFlag,
			distinctOn: [],
			ctes: [],
			unions: [],
			selectSubqueries: [],
			joins: this.#joins,
			lockMode: this.#lockMode ?? null,
		};
	}

	/** The compiled SELECT `{ sql, params }` WITHOUT executing (Lucid `toSQL`). */
	toSQL(): { sql: string; params: unknown[] } {
		const compiled = compileStatementNative(this.#selectSpec(), this.#dialect);
		return { sql: compiled.statements[0], params: compiled.params };
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
		return this.#exec.query<T>(sql, params);
	}

	/** Run the SELECT and return the first row (Lucid `first`), or `null`. */
	async first(): Promise<T | null> {
		this.#limit = 1;
		const rows = await this.exec();
		return rows[0] ?? null;
	}

	/** Run a scalar aggregate (COUNT/SUM/AVG/MIN/MAX) over the current WHERE. */
	async #aggregate(expr: string): Promise<number> {
		const compiled = compileStatementNative(
			this.#selectSpec([`${expr} AS aggregate`]),
			this.#dialect,
		);
		const rows = await this.#exec.query<{ aggregate: number | string | null }>(
			compiled.statements[0],
			compiled.params,
		);
		return Number(rows[0]?.aggregate ?? 0);
	}

	/** COUNT(*) over the current WHERE (Lucid aggregate helper). */
	count(): Promise<number> {
		return this.#aggregate("COUNT(*)");
	}

	/** SUM(column) (Lucid/Knex `sum`). */
	sum(column: string): Promise<number> {
		return this.#aggregate(`SUM(${column})`);
	}

	/** AVG(column) (Lucid/Knex `avg`). */
	avg(column: string): Promise<number> {
		return this.#aggregate(`AVG(${column})`);
	}

	/** MIN(column) (Lucid/Knex `min`). */
	min(column: string): Promise<number> {
		return this.#aggregate(`MIN(${column})`);
	}

	/** MAX(column) (Lucid/Knex `max`). */
	max(column: string): Promise<number> {
		return this.#aggregate(`MAX(${column})`);
	}

	/** WHERE clauses translated to the native compiler's shape (cmp or raw). */
	#compiledWheres(): CompiledWhere[] {
		return this.#wheres.map(
			(w): CompiledWhere =>
				w.kind === "raw"
					? { kind: "raw", sql: w.sql, bindings: w.bindings, type: w.boolean }
					: {
							column: w.column,
							operator: w.operator,
							value: w.value,
							type: w.boolean,
						},
		);
	}

	/** Columns to return from a subsequent insert/update/delete (Lucid `returning`). */
	returning(...columns: string[]): this {
		this.#returningCols.push(...columns);
		return this;
	}

	/** Run a DML spec: return the RETURNING rows when set, else execute for effect. */
	async #runDml(
		spec: Record<string, unknown>,
	): Promise<Record<string, unknown>[]> {
		const withReturning =
			this.#returningCols.length > 0
				? { ...spec, returning: this.#returningCols }
				: spec;
		const compiled = compileStatementNative(withReturning, this.#dialect);
		if (this.#returningCols.length > 0) {
			return this.#exec.query(compiled.statements[0], compiled.params);
		}
		await this.#exec.execute(compiled.statements[0], compiled.params);
		return [];
	}

	/** Conflict target for an upsert (Lucid/Knex `onConflict(...cols)`). */
	onConflict(...columns: string[]): this {
		this.#onConflictCols = columns;
		return this;
	}

	/** On conflict, UPDATE the given columns (or all non-conflict ones) — `merge`. */
	merge(...columns: string[]): this {
		this.#mergeMode = "merge";
		this.#mergeCols = columns;
		return this;
	}

	/** On conflict, do nothing (Lucid/Knex `onConflict(...).ignore()`). */
	ignore(): this {
		this.#mergeMode = "ignore";
		return this;
	}

	/** Compile an upsert from accumulated onConflict/merge state. */
	#runUpsert(
		rows: Array<Array<[string, unknown]>>,
	): Promise<Record<string, unknown>[]> {
		const conflictColumns = this.#onConflictCols ?? [];
		const allCols = rows[0]?.map(([c]) => c) ?? [];
		const updateColumns =
			this.#mergeMode === "ignore"
				? []
				: this.#mergeCols.length > 0
					? this.#mergeCols
					: allCols.filter((c) => !conflictColumns.includes(c));
		return this.#runDml({
			kind: "upsert",
			table: this.#table,
			rows,
			conflictColumns,
			updateColumns,
		});
	}

	/**
	 * Insert one row (Lucid `db.table(t).insert(data)`). Returns the RETURNING
	 * rows when {@link returning} was set (Postgres/SQLite), otherwise `[]`. When
	 * {@link onConflict} was set, compiles an upsert instead.
	 */
	async insert(
		data: Record<string, unknown>,
	): Promise<Record<string, unknown>[]> {
		const values = Object.entries(data);
		if (values.length === 0) return [];
		if (this.#onConflictCols) return this.#runUpsert([values]);
		return this.#runDml({ kind: "insert", table: this.#table, values });
	}

	/** Insert many rows in one statement (Lucid/Knex `multiInsert`). */
	async multiInsert(
		rows: Array<Record<string, unknown>>,
	): Promise<Record<string, unknown>[]> {
		if (rows.length === 0) return [];
		const rowEntries = rows.map((r) => Object.entries(r));
		if (this.#onConflictCols) return this.#runUpsert(rowEntries);
		return this.#runDml({
			kind: "insert",
			table: this.#table,
			rows: rowEntries,
		});
	}

	/** Update rows matching the current WHERE; returns affected count. */
	async update(data: Record<string, unknown>): Promise<number> {
		const set = Object.entries(data);
		if (set.length === 0) return 0;
		const compiled = compileStatementNative(
			{
				kind: "update",
				table: this.#table,
				set,
				wheres: this.#compiledWheres(),
			},
			this.#dialect,
		);
		const result = await this.#exec.execute(
			compiled.statements[0],
			compiled.params,
		);
		return rowsAffected(result);
	}

	/** Delete rows matching the current WHERE; returns affected count. */
	async delete(): Promise<number> {
		const compiled = compileStatementNative(
			{ kind: "delete", table: this.#table, wheres: this.#compiledWheres() },
			this.#dialect,
		);
		const result = await this.#exec.execute(
			compiled.statements[0],
			compiled.params,
		);
		return rowsAffected(result);
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
