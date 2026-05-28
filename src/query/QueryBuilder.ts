/**
 * Fluent Query Builder — with advanced features (CTE, subqueries, unions, window functions).
 *
 * @implements FR31, FR32, FR33, FR36, FR37
 */

import { AtlasError } from "../errors.js";
import { assertValidIdentifier } from "../utils/identifier.js";
import {
	type AtlasDialect,
	compileStatementNative,
	getAtlasDialect,
} from "./native.js";

export type WhereOperator =
	| "="
	| "!="
	| ">"
	| ">="
	| "<"
	| "<="
	| "LIKE"
	| "IN"
	| "NOT IN"
	| "IS NULL"
	| "IS NOT NULL";

const WHERE_OPERATORS = new Set<string>([
	"=",
	"!=",
	">",
	">=",
	"<",
	"<=",
	"LIKE",
	"IN",
	"NOT IN",
	"IS NULL",
	"IS NOT NULL",
]);

function isWhereOperator(v: unknown): v is WhereOperator {
	return typeof v === "string" && WHERE_OPERATORS.has(v);
}

export interface WhereClause {
	column: string;
	operator: WhereOperator;
	value: unknown;
	type: "and" | "or";
}

export interface ExistsClause {
	kind: "exists";
	subquery: QueryBuilder;
	type: "and" | "or";
}

export interface OrderByClause {
	column: string;
	direction: "asc" | "desc";
}

export interface CteDefinition {
	name: string;
	query: QueryBuilder | RawSql;
}

export interface QueryResult<T> {
	data: T[];
	total?: number;
	page?: number;
	perPage?: number;
}

/** Typed raw SQL with parameterized values. */
export class RawSql {
	readonly sql: string;
	readonly params: unknown[];

	constructor(sql: string, params: unknown[] = []) {
		this.sql = sql;
		this.params = params;
	}

	/** Tagged template literal for raw SQL with automatic parameterization. */
	static sql(strings: TemplateStringsArray, ...values: unknown[]): RawSql {
		let sql = "";
		const params: unknown[] = [];
		let paramIndex = 1;

		for (let i = 0; i < strings.length; i++) {
			sql += strings[i];
			if (i < values.length) {
				if (values[i] instanceof RawSql) {
					// Inline raw SQL fragments — re-index their $N placeholders
					const raw = values[i] as RawSql;
					let remapped = raw.sql;
					for (let j = raw.params.length; j >= 1; j--) {
						remapped = remapped.replace(
							new RegExp(`\\$${j}(?!\\d)`, "g"),
							`$${paramIndex + j - 1}`,
						);
					}
					sql += remapped;
					params.push(...raw.params);
					paramIndex += raw.params.length;
				} else {
					sql += `$${paramIndex++}`;
					params.push(values[i]);
				}
			}
		}

		return new RawSql(sql, params);
	}
}

// Identifier quoting and SQL compilation now handled by Rust (ream-query crate).
// The TS QueryBuilder is a thin wrapper that serializes to JSON and calls the Rust compiler.

/**
 * Fluent query builder — type-safe, composable, with advanced features.
 */
export class QueryBuilder<_T = Record<string, unknown>> {
	#table: string;
	#select: string[] = ["*"];
	#where: Array<WhereClause | ExistsClause> = [];
	#orderBy: OrderByClause[] = [];
	#groupBy: string[] = [];
	#having: WhereClause[] = [];
	#limit?: number;
	#offset?: number;
	#preload: string[] = [];
	#ctes: CteDefinition[] = [];
	#unions: Array<{ query: QueryBuilder; all: boolean }> = [];
	#distinct = false;
	/** SQL dialect for compilation — explicit override or process default. */
	#dialect: AtlasDialect;

	constructor(table: string, options?: { dialect?: AtlasDialect }) {
		this.#table = table;
		this.#dialect = options?.dialect ?? getAtlasDialect();
	}

	/** SELECT DISTINCT. */
	distinct(): this {
		this.#distinct = true;
		return this;
	}

	/** Select specific columns (supports expressions like 'COUNT(*) AS total'). */
	select(...columns: string[]): this {
		if (columns.length === 0) {
			throw new AtlasError(
				"EMPTY_SELECT",
				"select() requires at least one column",
			);
		}
		this.#select = columns;
		return this;
	}

	/** Add a WHERE condition. */
	where(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		if (value === undefined) {
			this.#where.push({
				column,
				operator: "=",
				value: operatorOrValue,
				type: "and",
			});
		} else {
			if (!isWhereOperator(operatorOrValue)) {
				throw new AtlasError(
					"INVALID_OPERATOR",
					`Invalid WHERE operator: ${String(operatorOrValue)}`,
				);
			}
			this.#where.push({
				column,
				operator: operatorOrValue,
				value,
				type: "and",
			});
		}
		return this;
	}

	/** Add an OR WHERE condition. */
	orWhere(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		if (value === undefined) {
			this.#where.push({
				column,
				operator: "=",
				value: operatorOrValue,
				type: "or",
			});
		} else {
			if (!isWhereOperator(operatorOrValue)) {
				throw new AtlasError(
					"INVALID_OPERATOR",
					`Invalid WHERE operator: ${String(operatorOrValue)}`,
				);
			}
			this.#where.push({
				column,
				operator: operatorOrValue,
				value,
				type: "or",
			});
		}
		return this;
	}

	/** WHERE column IN (values). */
	whereIn(column: string, values: unknown[]): this {
		this.#where.push({ column, operator: "IN", value: values, type: "and" });
		return this;
	}

	/** WHERE column IS NULL. */
	whereNull(column: string): this {
		this.#where.push({ column, operator: "IS NULL", value: null, type: "and" });
		return this;
	}

	/** WHERE column IS NOT NULL. */
	whereNotNull(column: string): this {
		this.#where.push({
			column,
			operator: "IS NOT NULL",
			value: null,
			type: "and",
		});
		return this;
	}

	/** WHERE EXISTS (subquery). Subquery is deferred — toSQL() called at build time. */
	whereExists(subquery: QueryBuilder): this {
		this.#where.push({ kind: "exists", subquery, type: "and" });
		return this;
	}

	/** GROUP BY columns. */
	groupBy(...columns: string[]): this {
		this.#groupBy.push(...columns);
		return this;
	}

	/** HAVING condition (used after GROUP BY). Supports aggregate expressions. */
	having(
		column: string,
		operatorOrValue: WhereOperator | unknown,
		value?: unknown,
	): this {
		if (value === undefined) {
			this.#having.push({
				column,
				operator: "=",
				value: operatorOrValue,
				type: "and",
			});
		} else {
			if (!isWhereOperator(operatorOrValue)) {
				throw new AtlasError(
					"INVALID_OPERATOR",
					`Invalid HAVING operator: ${String(operatorOrValue)}`,
				);
			}
			this.#having.push({
				column,
				operator: operatorOrValue,
				value,
				type: "and",
			});
		}
		return this;
	}

	/** ORDER BY column. */
	orderBy(column: string, direction: "asc" | "desc" = "asc"): this {
		this.#orderBy.push({ column, direction });
		return this;
	}

	/** LIMIT results. */
	limit(n: number): this {
		if (n < 0) {
			throw new AtlasError("INVALID_LIMIT", "limit must be >= 0");
		}
		this.#limit = n;
		return this;
	}

	/** OFFSET results. */
	offset(n: number): this {
		if (n < 0) {
			throw new AtlasError("INVALID_OFFSET", "offset must be >= 0");
		}
		this.#offset = n;
		return this;
	}

	/** Paginate results. */
	paginate(page: number, perPage = 20): this {
		if (page < 1) {
			throw new AtlasError("INVALID_PAGE", "page must be >= 1");
		}
		if (perPage < 1) {
			throw new AtlasError("INVALID_PER_PAGE", "perPage must be >= 1");
		}
		this.#limit = perPage;
		this.#offset = (page - 1) * perPage;
		return this;
	}

	/** Eager-load a relation. */
	preload(relation: string): this {
		this.#preload.push(relation);
		return this;
	}

	/** WITH (Common Table Expression). */
	with(name: string, query: QueryBuilder | RawSql): this {
		assertValidIdentifier(
			name,
			"CTE name",
			(msg) => new AtlasError("INVALID_CTE_NAME", msg),
		);
		this.#ctes.push({ name, query });
		return this;
	}

	/** UNION with another query (wrapped in parentheses). */
	union(query: QueryBuilder): this {
		this.#unions.push({ query, all: false });
		return this;
	}

	/** UNION ALL with another query (wrapped in parentheses). */
	unionAll(query: QueryBuilder): this {
		this.#unions.push({ query, all: true });
		return this;
	}

	/**
	 * Serialize the query description to JSON for the Rust compiler.
	 */
	#toQueryDescription(): Record<string, unknown> {
		// Serialize CTEs — pre-compile sub-queries
		const ctes = this.#ctes.map((cte) => {
			if (cte.query instanceof RawSql) {
				return { name: cte.name, sql: cte.query.sql, params: cte.query.params };
			}
			const sub = cte.query.toSQL();
			return { name: cte.name, sql: sub.sql, params: sub.params };
		});

		// Serialize WHERE clauses
		const wheres = this.#where.map((w) => {
			if ("kind" in w) {
				const sub = w.subquery.#toQueryDescription();
				return { kind: "exists", subquery: sub, type: w.type };
			}
			return {
				column: w.column,
				operator: w.operator,
				value: w.value,
				type: w.type,
			};
		});

		// Serialize UNIONs — pre-compile sub-queries
		const unions = this.#unions.map((u) => {
			const sub = u.query.toSQL();
			return { sql: sub.sql, params: sub.params, all: u.all };
		});

		return {
			table: this.#table,
			select: this.#select,
			wheres,
			orderBy: this.#orderBy,
			groupBy: this.#groupBy,
			having: this.#having,
			limit: this.#limit ?? null,
			offset: this.#offset ?? null,
			distinct: this.#distinct,
			ctes,
			unions,
		};
	}

	/** Build the SQL query string via the Rust compiler. */
	toSQL(): { sql: string; params: unknown[] } {
		const desc = this.#toQueryDescription();
		const compiled = compileStatementNative(
			{ kind: "select", ...desc },
			this.#dialect,
		);
		return { sql: compiled.statements[0], params: compiled.params };
	}

	/** Get the table name. */
	getTable(): string {
		return this.#table;
	}

	/** Get preloaded relations. */
	getPreloads(): string[] {
		return [...this.#preload];
	}
}
