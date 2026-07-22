/**
 * Connection-level query builder — Adonis Lucid's `db.query()` / `db.from()` /
 * `db.table()` / `db.insertQuery()`. Unlike {@link ModelQuery} it is NOT bound to
 * a model: it reads and writes plain rows against a table, executing through a
 * connection (or a transaction client passed as `{ client }`).
 *
 * Reads are compiled by the shared {@link QueryBuilder}; writes (insert / update
 * / delete) go straight through the native compiler, the same path the
 * repository uses — so quoting, casts and parameter binding are identical.
 *
 *     const rows = await db.from('users').where('is_active', true).orderBy('id')
 *     const user = await db.query().from('users').where('id', 1).first()
 *     await db.table('audit_logs').insert({ user_id: 1, action: 'login' })
 *     await db.from('users').where('id', 1).update({ is_active: false })
 */

import { type AtlasDialect, compileStatementNative } from "./native.js";
import { QueryBuilder, type WhereOperator } from "./QueryBuilder.js";

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

/** One accumulated WHERE, replayable into a read builder AND a DML spec. */
type WhereEntry =
	| {
			kind: "basic";
			column: string;
			operator: WhereOperator;
			value: unknown;
			boolean: "and" | "or";
	  }
	| { kind: "in"; column: string; values: unknown[] }
	| { kind: "null"; column: string }
	| { kind: "notNull"; column: string };

/** DML `wheres` entry shape the native compiler expects. */
interface CompiledWhere {
	column: string;
	operator: string;
	value: unknown;
	type: "and" | "or";
}

export class DatabaseQueryBuilder<T = Record<string, unknown>> {
	readonly #exec: QueryExecutor;
	readonly #dialect: AtlasDialect;
	#table: string;
	#selects: string[] = [];
	#wheres: WhereEntry[] = [];
	#orderBys: Array<{ column: string; direction: "asc" | "desc" }> = [];
	#groupBys: string[] = [];
	#havings: Array<{ column: string; operator: WhereOperator; value: unknown }> =
		[];
	#returningCols: string[] = [];
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
			this.#wheres.push({
				kind: "basic",
				column,
				operator: "=",
				value: operatorOrValue,
				boolean,
			});
		} else {
			this.#wheres.push({
				kind: "basic",
				column,
				operator: operatorOrValue as WhereOperator,
				value,
				boolean,
			});
		}
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
				? { column, operator: "=", value: operatorOrValue }
				: { column, operator: operatorOrValue as WhereOperator, value },
		);
		return this;
	}

	whereIn(column: string, values: unknown[]): this {
		this.#wheres.push({ kind: "in", column, values });
		return this;
	}

	whereNull(column: string): this {
		this.#wheres.push({ kind: "null", column });
		return this;
	}

	whereNotNull(column: string): this {
		this.#wheres.push({ kind: "notNull", column });
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

	/** Assemble a read {@link QueryBuilder} from the accumulated state. */
	#readBuilder(): QueryBuilder<T> {
		const qb = new QueryBuilder<T>(this.#table, { dialect: this.#dialect });
		if (this.#distinctFlag) qb.distinct();
		if (this.#selects.length > 0) qb.select(...this.#selects);
		for (const w of this.#wheres) {
			if (w.kind === "basic") {
				if (w.boolean === "or") qb.orWhere(w.column, w.operator, w.value);
				else qb.where(w.column, w.operator, w.value);
			} else if (w.kind === "in") qb.whereIn(w.column, w.values);
			else if (w.kind === "null") qb.whereNull(w.column);
			else qb.whereNotNull(w.column);
		}
		if (this.#groupBys.length > 0) qb.groupBy(...this.#groupBys);
		for (const h of this.#havings) qb.having(h.column, h.operator, h.value);
		for (const o of this.#orderBys) qb.orderBy(o.column, o.direction);
		if (this.#limit !== undefined) qb.limit(this.#limit);
		if (this.#offset !== undefined) qb.offset(this.#offset);
		return qb;
	}

	/** Run the SELECT and return every row. */
	async exec(): Promise<T[]> {
		const { sql, params } = this.#readBuilder().toSQL();
		return this.#exec.query<T>(sql, params);
	}

	/** Run the SELECT and return the first row (Lucid `first`), or `null`. */
	async first(): Promise<T | null> {
		this.#limit = 1;
		const rows = await this.exec();
		return rows[0] ?? null;
	}

	/** COUNT(*) over the current WHERE (Lucid aggregate helper). */
	async count(): Promise<number> {
		const rows = await this.#exec.query<{ count: number | string }>(
			this.#readBuilder().select("COUNT(*) AS count").toSQL().sql,
			this.#readBuilder().toSQL().params,
		);
		return Number(rows[0]?.count ?? 0);
	}

	/** WHERE clauses translated to the native compiler's DML shape. */
	#compiledWheres(): CompiledWhere[] {
		return this.#wheres.map((w): CompiledWhere => {
			if (w.kind === "basic")
				return {
					column: w.column,
					operator: w.operator,
					value: w.value,
					type: w.boolean,
				};
			if (w.kind === "in")
				return {
					column: w.column,
					operator: "IN",
					value: w.values,
					type: "and",
				};
			if (w.kind === "null")
				return {
					column: w.column,
					operator: "IS NULL",
					value: null,
					type: "and",
				};
			return {
				column: w.column,
				operator: "IS NOT NULL",
				value: null,
				type: "and",
			};
		});
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

	/**
	 * Insert one row (Lucid `db.table(t).insert(data)`). Returns the RETURNING
	 * rows when {@link returning} was set (Postgres/SQLite), otherwise `[]`.
	 */
	async insert(
		data: Record<string, unknown>,
	): Promise<Record<string, unknown>[]> {
		const values = Object.entries(data);
		if (values.length === 0) return [];
		return this.#runDml({ kind: "insert", table: this.#table, values });
	}

	/** Insert many rows in one statement (Lucid/Knex `multiInsert`). */
	async multiInsert(
		rows: Array<Record<string, unknown>>,
	): Promise<Record<string, unknown>[]> {
		if (rows.length === 0) return [];
		return this.#runDml({
			kind: "insert",
			table: this.#table,
			rows: rows.map((r) => Object.entries(r)),
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
