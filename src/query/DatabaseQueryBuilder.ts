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

import { Paginator } from "../ModelQuery.js";
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
			kind: "json";
			jsonOp: "path" | "superset" | "subset";
			column: string;
			negated: boolean;
			path?: string;
			operator?: string;
			value: unknown;
			boolean: "and" | "or";
	  };

/** The native compiler's WHERE entry JSON (camelCase). */
type CompiledWhere = Record<string, unknown>;

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
	andOn(left: string, right: string): DbJoinBuilder;
	orOn(left: string, right: string): DbJoinBuilder;
	onVal(left: string, value: unknown): DbJoinBuilder;
	andOnVal(left: string, value: unknown): DbJoinBuilder;
	orOnVal(left: string, value: unknown): DbJoinBuilder;
}

/** One accumulated `ON` part — column-to-column, or column-to-value (`value` set). */
interface JoinPart {
	kind: "and" | "or";
	left: string;
	right?: string;
	value?: { v: unknown };
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
	}> = [];
	#schema?: string;
	#lockMode?: string;
	#lockModifier?: string;
	#distinctOn: string[] = [];
	#returningCols: string[] = [];
	#onConflictCols?: string[];
	#mergeMode?: "merge" | "ignore";
	#mergeCols: string[] = [];
	#distinctFlag = false;
	#limit?: number;
	#offset?: number;
	#debug = false;
	#comments: string[] = [];

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

	/** WHERE left <op> right — both COLUMNS (Lucid/Knex `whereColumn`). */
	whereColumn(left: string, operator: string, right: string): this {
		const ops = new Set(["=", "!=", "<>", "<", ">", "<=", ">="]);
		if (!ops.has(operator)) {
			throw new Error(`whereColumn: unsupported operator '${operator}'`);
		}
		const sql = `${this.#quoteIdent(left)} ${operator} ${this.#quoteIdent(right)}`;
		this.#wheres.push({ kind: "raw", sql, bindings: [], boolean: "and" });
		return this;
	}

	/** WHERE EXISTS (subquery) (Lucid/Knex `whereExists`). */
	whereExists(sub: DatabaseQueryBuilder): this {
		return this.#pushExists("and", false, sub);
	}

	/** WHERE NOT EXISTS (subquery). */
	whereNotExists(sub: DatabaseQueryBuilder): this {
		return this.#pushExists("and", true, sub);
	}

	/** OR EXISTS (subquery). */
	orWhereExists(sub: DatabaseQueryBuilder): this {
		return this.#pushExists("or", false, sub);
	}

	#pushExists(
		boolean: "and" | "or",
		negated: boolean,
		sub: DatabaseQueryBuilder,
	): this {
		this.#wheres.push({
			kind: "exists",
			negated,
			subquery: sub.#selectSpec(),
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
		return this.#pushJsonContainment("superset", column, value);
	}

	/** WHERE json column `<@` value — contained by (`whereJsonSubsetOf`). */
	whereJsonSubsetOf(column: string, value: unknown): this {
		return this.#pushJsonContainment("subset", column, value);
	}

	#pushJsonContainment(
		jsonOp: "superset" | "subset",
		column: string,
		value: unknown,
	): this {
		this.#wheres.push({
			kind: "json",
			jsonOp,
			column,
			negated: false,
			value: typeof value === "string" ? value : JSON.stringify(value),
			boolean: "and",
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
	join(table: string, build: (j: DbJoinBuilder) => void): this;
	join(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("INNER", table, leftOrBuild, right);
	}

	/** `INNER JOIN table ON <left> = <right>` or a callback `ON` builder (Lucid/Knex). */
	innerJoin(table: string, left: string, right: string): this;
	innerJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	innerJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("INNER", table, leftOrBuild, right);
	}

	/** `LEFT JOIN table ON <left> = <right>` or a callback `ON` builder (Lucid/Knex). */
	leftJoin(table: string, left: string, right: string): this;
	leftJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	leftJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("LEFT", table, leftOrBuild, right);
	}

	/** `LEFT OUTER JOIN` — alias of {@link leftJoin} (Lucid/Knex `leftOuterJoin`). */
	leftOuterJoin(table: string, left: string, right: string): this;
	leftOuterJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	leftOuterJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("LEFT", table, leftOrBuild, right);
	}

	/** `RIGHT JOIN table ON <left> = <right>` or a callback `ON` builder (Lucid/Knex). */
	rightJoin(table: string, left: string, right: string): this;
	rightJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	rightJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("RIGHT", table, leftOrBuild, right);
	}

	/** `RIGHT OUTER JOIN` — alias of {@link rightJoin} (Lucid/Knex `rightOuterJoin`). */
	rightOuterJoin(table: string, left: string, right: string): this;
	rightOuterJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	rightOuterJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("RIGHT", table, leftOrBuild, right);
	}

	/** `FULL OUTER JOIN table ON …` (Lucid/Knex `fullOuterJoin`; Postgres — MySQL/SQLite lack it). */
	fullOuterJoin(table: string, left: string, right: string): this;
	fullOuterJoin(table: string, build: (j: DbJoinBuilder) => void): this;
	fullOuterJoin(
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("FULL OUTER", table, leftOrBuild, right);
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

	/** Build a JOIN from the 3-arg or callback form (mirrors ModelQuery `#pushJoin`). */
	#pushJoin(
		kind: "INNER" | "LEFT" | "RIGHT" | "FULL OUTER",
		table: string,
		leftOrBuild: string | ((j: DbJoinBuilder) => void),
		right?: string,
	): this {
		const tq = this.#quoteJoinRef(table);
		if (typeof leftOrBuild === "function") {
			const parts: JoinPart[] = [];
			const jb: DbJoinBuilder = {
				on(l, r) {
					parts.push({ kind: "and", left: l, right: r });
					return jb;
				},
				andOn(l, r) {
					parts.push({ kind: "and", left: l, right: r });
					return jb;
				},
				orOn(l, r) {
					parts.push({ kind: "or", left: l, right: r });
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
			};
			leftOrBuild(jb);
			const params: unknown[] = [];
			const on = parts
				.map((p, i) => {
					const prefix = i === 0 ? "ON" : p.kind === "or" ? "OR" : "AND";
					if (p.value) {
						params.push(p.value.v);
						return `${prefix} ${this.#quoteJoinRef(p.left)} = ?`;
					}
					return `${prefix} ${this.#quoteJoinRef(p.left)} = ${this.#quoteJoinRef(p.right ?? "")}`;
				})
				.join(" ");
			this.#joins.push({ sql: `${kind} JOIN ${tq} ${on}`, params });
			return this;
		}
		if (right === undefined) {
			throw new Error(
				"join() string form requires both left and right operands",
			);
		}
		this.#joins.push({
			sql: `${kind} JOIN ${tq} ON ${this.#quoteJoinRef(leftOrBuild)} = ${this.#quoteJoinRef(right)}`,
			params: [],
		});
		return this;
	}

	/** `UNION` with another query (Lucid/Knex `union`). */
	union(sub: DatabaseQueryBuilder): this {
		return this.#pushUnion(sub, false);
	}

	/** `UNION ALL` (Lucid/Knex `unionAll`). */
	unionAll(sub: DatabaseQueryBuilder): this {
		return this.#pushUnion(sub, true, "union");
	}

	/** `INTERSECT` — rows present in both queries (Lucid/Knex `intersect`). */
	intersect(sub: DatabaseQueryBuilder): this {
		return this.#pushUnion(sub, false, "intersect");
	}

	/** `INTERSECT ALL` — duplicate-preserving {@link intersect} (Postgres/MySQL). */
	intersectAll(sub: DatabaseQueryBuilder): this {
		return this.#pushUnion(sub, true, "intersect");
	}

	/** `EXCEPT` — rows in this query but not the other (Lucid/Knex `except`). */
	except(sub: DatabaseQueryBuilder): this {
		return this.#pushUnion(sub, false, "except");
	}

	/** `EXCEPT ALL` — duplicate-preserving {@link except} (Postgres/MySQL). */
	exceptAll(sub: DatabaseQueryBuilder): this {
		return this.#pushUnion(sub, true, "except");
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

	/** `WITH name AS (subquery)` common table expression (Lucid/Knex `with`). */
	with(
		name: string,
		sub: DatabaseQueryBuilder,
		options: { recursive?: boolean; materialized?: boolean } = {},
	): this {
		const { sql, params } = sub.toSQL();
		this.#ctes.push({
			name,
			sql,
			params,
			recursive: options.recursive ?? false,
			materialized: options.materialized ?? null,
		});
		return this;
	}

	/** `WITH RECURSIVE name AS (subquery)` (Lucid/Knex `withRecursive`). */
	withRecursive(name: string, sub: DatabaseQueryBuilder): this {
		return this.with(name, sub, { recursive: true });
	}

	/** `WITH name AS MATERIALIZED (subquery)` — Postgres (Lucid/Knex `withMaterialized`). */
	withMaterialized(name: string, sub: DatabaseQueryBuilder): this {
		return this.with(name, sub, { materialized: true });
	}

	/** `WITH name AS NOT MATERIALIZED (subquery)` — Postgres (Lucid/Knex `withNotMaterialized`). */
	withNotMaterialized(name: string, sub: DatabaseQueryBuilder): this {
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
		c.#distinctFlag = this.#distinctFlag;
		c.#limit = this.#limit;
		c.#offset = this.#offset;
		c.#debug = this.#debug;
		c.#comments = [...this.#comments];
		return c;
	}

	/** Qualify the table with a schema (Lucid/Knex `withSchema`). */
	withSchema(schema: string): this {
		this.#schema = schema;
		return this;
	}

	/** Log the compiled SQL + bindings to the console on the next run (Lucid/Knex `debug`). */
	debug(enabled = true): this {
		this.#debug = enabled;
		return this;
	}

	/**
	 * Accepted for Lucid/Knex compatibility — a statement timeout is a
	 * driver/connection concern atlas does not expose at this compile layer.
	 * Kept as a no-op so a Lucid `.timeout(ms)` call site ports unchanged.
	 */
	timeout(): this {
		return this;
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

	/** SELECT DISTINCT (Lucid/Knex `distinct`). */
	distinct(): this {
		this.#distinctFlag = true;
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

	/** WHERE NOT (col <op> value) — negated comparison (Lucid/Knex `whereNot`). */
	whereNot(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		const [op, val] =
			value === undefined
				? ["=", operatorOrValue]
				: [operatorOrValue as string, value];
		this.#cmp("and", column, negateOperator(op), val);
		return this;
	}

	/** OR col IN (...) (Lucid/Knex `orWhereIn`). */
	orWhereIn(column: string, values: unknown[]): this {
		this.#cmp("or", column, "IN", values);
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
			joins: this.#joins,
			lockMode: this.#composedLockMode(),
		};
	}

	/** The compiled SELECT `{ sql, params }` WITHOUT executing (Lucid `toSQL`). */
	toSQL(): { sql: string; params: unknown[] } {
		const compiled = compileStatementNative(this.#selectSpec(), this.#dialect);
		const prefix =
			this.#comments.length > 0
				? `${this.#comments.map((c) => `/* ${c} */`).join(" ")} `
				: "";
		const sql = prefix + compiled.statements[0];
		if (this.#debug) {
			console.debug("[atlas:sql]", sql, compiled.params);
		}
		return { sql, params: compiled.params };
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

	/** {@link first} but throws when no row matches (Lucid `firstOrFail`). */
	async firstOrFail(): Promise<T> {
		const row = await this.first();
		if (row === null) throw new Error("firstOrFail: no matching row");
		return row;
	}

	/** Return a single column's values across the result set (Lucid/Knex `pluck`). */
	async pluck(column: string): Promise<unknown[]> {
		const { sql, params } = this.toSQL();
		const rows = await this.#exec.query<Record<string, unknown>>(sql, params);
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
		}>(countCompiled.statements[0], countCompiled.params);
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
