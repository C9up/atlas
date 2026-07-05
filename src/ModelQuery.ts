/**
 * ModelQuery — executable query builder for repositories.
 *
 * Like AdonisJS Lucid Model.query():
 *   repo.query().where('status', 'active').orderBy('created_at', 'desc').limit(10).exec()
 *
 * Builds SQL fluently and executes against the database connection.
 */

import type { BaseEntity } from "./BaseEntity.js";
import type { DatabaseConnection } from "./BaseRepository.js";
import {
	getColumnMetadata,
	getEntityMetadata,
	getPrimaryKey,
	getRelationMetadata,
	hasSoftDeletes,
	type RelationMetadata,
} from "./decorators/entity.js";
import { fireHooks } from "./decorators/hooks.js";
import {
	type AtlasDialect,
	compileStatementNative,
	getAtlasDialect,
} from "./query/native.js";
import { camelToSnake, snakeToCamel } from "./utils/casing.js";

/**
 * Comparison operators allowed in `whereExpr`'s raw 4-arg form (where
 * `op` is interpolated into SQL rather than parameterized). Kept tight
 * to operators that take a single bound `?` value — IN / IS NULL etc.
 * have no place in this helper.
 */
const WHEREEXPR_OPERATORS = new Set<string>([
	"=",
	"!=",
	"<>",
	">",
	">=",
	"<",
	"<=",
	"LIKE",
	"NOT LIKE",
]);

/** True when every `(` in `s` has a matching `)` and none closes early. */
function hasBalancedParens(s: string): boolean {
	let depth = 0;
	for (const ch of s) {
		if (ch === "(") depth++;
		else if (ch === ")") {
			depth--;
			if (depth < 0) return false;
		}
	}
	return depth === 0;
}

type PreloadCallback = (query: ModelQuery<BaseEntity>) => void;

type ColumnResolver = (column: string) => string;

/** Per-preload-relation locals shared by the resolver helpers. Built once per relation, then passed by ref. */
interface PreloadContext {
	relation: RelationMetadata;
	relationName: string;
	relatedClass: new () => BaseEntity;
	relatedTable: string;
	relatedPk: string;
	hydrate: (row: Record<string, unknown>) => BaseEntity;
	runInQuery: (
		table: string,
		column: string,
		values: unknown[],
	) => Promise<Record<string, unknown>[]>;
	/**
	 * Relation-aware query — applies `relation.onQuery` + the user preload
	 * callback before executing. Use for the primary related-row fetch; use
	 * `runInQuery` for auxiliary fetches (e.g. the intermediate hop of a
	 * through relation or the pivot lookup of a m2m) where filters don't apply.
	 */
	runRelationQuery: (
		column: string,
		values: unknown[],
	) => Promise<Record<string, unknown>[]>;
	nestedCallback: PreloadCallback | undefined;
}

type SoftDeleteScope = "default" | "with-trashed" | "only-trashed";

/** Set an empty relation value on every parent and return no related rows. */
function assignEmptyRelation(
	entities: BaseEntity[],
	relationName: string,
	single: boolean,
): BaseEntity[] {
	for (const e of entities) e.setProp(relationName, single ? null : []);
	return [];
}

/**
 * Map each intermediate row's `secondLocal` key to its parent id (`firstKey`).
 * Throws when two intermediate rows share a key but point at different parents
 * — a non-unique `secondLocalKey` would otherwise silently drop data.
 */
function buildThroughToParent(
	throughRows: Record<string, unknown>[],
	secondLocal: string,
	firstKey: string,
	err: {
		relationName: string;
		throughTable: string;
		throughClass: string;
		throughPk: string;
	},
): Map<unknown, unknown> {
	const throughToParent = new Map<unknown, unknown>();
	for (const row of throughRows) {
		const key = row[secondLocal];
		if (
			throughToParent.has(key) &&
			throughToParent.get(key) !== row[firstKey]
		) {
			throw new Error(
				`@HasManyThrough/@HasOneThrough '${err.relationName}': duplicate secondLocalKey='${String(key)}' ` +
					`on ${err.throughTable} maps to multiple parents. Either set secondLocalKey to a unique column ` +
					`(default: ${err.throughClass}.${err.throughPk}) or fix the underlying data.`,
			);
		}
		throughToParent.set(key, row[firstKey]);
	}
	return throughToParent;
}

/** A standard column predicate (column OP value). */
interface StandardWhere {
	type: "and" | "or";
	column: string;
	operator: string;
	value: unknown;
}

/** A raw SQL fragment with `?` bindings — kind-tagged for the Rust compiler. */
interface RawWhere {
	type: "and" | "or";
	kind: "raw";
	sql: string;
	bindings: unknown[];
}

/** An EXISTS / NOT EXISTS correlated subquery — used by whereHas / doesntHave. */
interface ExistsWhere {
	type: "and" | "or";
	kind: "exists";
	negated: boolean;
	subquery: SelectSpec;
}

/** Parenthesised group of WHERE conditions — built via `where(cb)`. */
interface GroupWhere {
	type: "and" | "or";
	kind: "group";
	conditions: WhereClause[];
}

/** `col IN (SELECT ...)` / `col NOT IN (SELECT ...)` — built via `whereIn(col, subQ)`. */
interface InSubWhere {
	type: "and" | "or";
	kind: "inSub";
	column: string;
	negated: boolean;
	subquery: SelectSpec;
}

/** Shape of the spec object sent to the Rust compiler. Shared by root + sub queries. */
interface HavingClause {
	column: string;
	operator: string;
	value: unknown;
	type: "and" | "or";
}

interface SubqueryProjection {
	alias: string;
	subquery: SelectSpec;
}

interface SelectSpec {
	kind: "select";
	table: string;
	select: string[];
	selectSubqueries: SubqueryProjection[];
	wheres: WhereClause[];
	orderBy: Array<{ column: string; direction: "asc" | "desc" }>;
	groupBy: string[];
	having: HavingClause[];
	limit: number | null;
	offset: number | null;
	distinct: boolean;
	ctes: unknown[];
	unions: unknown[];
	joins: string[];
	lockMode: "FOR UPDATE" | "FOR SHARE" | null;
}

type WhereClause =
	| StandardWhere
	| RawWhere
	| ExistsWhere
	| GroupWhere
	| InSubWhere;

type WhereCallback = (q: ModelQuery<BaseEntity>) => void;

/**
 * Process-wide strict mode flag. When enabled, `whereRaw()` and `joinRaw()`
 * throw unconditionally — forcing every call site to use the typed
 * `whereExpr()` / `joinOn()` / structured builder paths. Intended for prod
 * hardening on apps that can't audit every call site manually.
 *
 * Enable via:
 *   - `setAtlasStrictMode(true)` at app bootstrap
 *   - `ATLAS_STRICT=1` environment variable (picked up lazily on first call)
 *
 * Framework-internal call sites that legitimately need raw SQL (relation
 * resolvers, preload join predicates) bypass strict mode via the private
 * `__internal: true` flag on the call — not exposed in the public types.
 */
let atlasStrictMode: boolean | undefined;

/** Enable or disable Atlas strict mode. When enabled, whereRaw/joinRaw throw in user code. */
export function setAtlasStrictMode(enabled: boolean): void {
	atlasStrictMode = enabled;
}

/** Current strict mode state — lazy env var read on first access. */
export function isAtlasStrictMode(): boolean {
	if (atlasStrictMode === undefined) {
		atlasStrictMode =
			process.env.ATLAS_STRICT === "1" || process.env.ATLAS_STRICT === "true";
	}
	return atlasStrictMode;
}

/**
 * Module-local escape hatch. Framework internal code (relation proxies,
 * preload resolvers) sets this to `true` around a section where it legitimately
 * needs to call whereRaw/joinRaw. Reset to `false` in a `finally` block.
 * Not exposed from the package barrel — only accessible to files in this module.
 */
let atlasInternalBypass = false;
export function runWithAtlasInternalBypass<T>(fn: () => T): T {
	const prev = atlasInternalBypass;
	atlasInternalBypass = true;
	try {
		return fn();
	} finally {
		atlasInternalBypass = prev;
	}
}
function isInternalBypass(): boolean {
	return atlasInternalBypass;
}

/** Multi-condition join builder passed to innerJoin/leftJoin/rightJoin callbacks. */
interface JoinBuilder {
	parts: Array<{ kind: "and" | "or"; left: string; right: string }>;
	on(left: string, right: string): JoinBuilder;
	andOn(left: string, right: string): JoinBuilder;
	andOnVal(left: string, value: unknown): JoinBuilder;
}

/** Offset-based paginator (Story 29.10). */
export class Paginator<T> {
	readonly items: T[];
	readonly meta: {
		total: number;
		perPage: number;
		currentPage: number;
		lastPage: number;
		firstPage: number;
	};
	#baseUrl?: string;
	#queryString: Record<string, unknown> = {};

	constructor(
		items: T[],
		base: { total: number; perPage: number; currentPage: number },
	) {
		this.items = items;
		const lastPage = Math.max(1, Math.ceil(base.total / base.perPage));
		this.meta = { ...base, lastPage, firstPage: 1 };
	}

	all(): T[] {
		return this.items;
	}

	/** True when there is more than one page of results (AdonisJS `hasPages`). */
	get hasPages(): boolean {
		return this.meta.lastPage > 1;
	}

	/** True when there is at least one more page after the current one (AdonisJS `hasMorePages`). */
	get hasMorePages(): boolean {
		return this.meta.currentPage < this.meta.lastPage;
	}

	serialize(opts?: { fields?: string[] }): {
		data: unknown[];
		meta: Paginator<T>["meta"];
	} {
		const data = this.items.map((item) => {
			if (!opts?.fields) return item;
			const picked: Record<string, unknown> = {};
			for (const f of opts.fields)
				picked[f] = (item as Record<string, unknown>)[f];
			return picked;
		});
		return { data, meta: this.meta };
	}

	baseUrl(url: string): this {
		this.#baseUrl = url;
		return this;
	}
	queryString(qs: Record<string, unknown>): this {
		this.#queryString = qs;
		return this;
	}

	toJSON(): {
		data: unknown[];
		meta: Paginator<T>["meta"] & Record<string, unknown>;
	} {
		const meta: Paginator<T>["meta"] & Record<string, unknown> = {
			...this.meta,
		};
		if (this.#baseUrl) {
			const build = (page: number) => {
				const params = new URLSearchParams();
				for (const [k, v] of Object.entries(this.#queryString))
					params.set(k, String(v));
				params.set("page", String(page));
				return `${this.#baseUrl}?${params.toString()}`;
			};
			meta.firstPageUrl = build(1);
			meta.lastPageUrl = build(this.meta.lastPage);
			if (this.meta.currentPage < this.meta.lastPage)
				meta.nextPageUrl = build(this.meta.currentPage + 1);
			if (this.meta.currentPage > 1)
				meta.previousPageUrl = build(this.meta.currentPage - 1);
		}
		return { data: this.items as unknown[], meta };
	}
}

/** Safe deep-clone for clause containers. `structuredClone` handles the shapes we use. */
function structuredCloneSafe<T>(value: T): T {
	return structuredClone(value);
}

export class ModelQuery<T extends BaseEntity> {
	#tableName: string;
	#db: DatabaseConnection;
	#hydrateFn: (row: Record<string, unknown>) => T;
	#entityClass: new () => T;
	#resolveColumn: ColumnResolver;
	#softDeletes: boolean;
	#softScope: SoftDeleteScope = "default";
	#wheres: WhereClause[] = [];
	#orderBys: Array<{ column: string; direction: "asc" | "desc" }> = [];
	#select: string[] = ["*"];
	#limit?: number;
	#offset?: number;
	#preloads = new Map<string, PreloadCallback | undefined>();
	/** Correlated subquery projections (withCount / withAggregate). */
	#selectSubqueries: SubqueryProjection[] = [];
	/** Alias stored by `.as()` — consumed when this query is used as a withCount/withAggregate sub-builder. */
	#subqueryAlias?: string;
	/** Raw JOIN fragments — Story 29.4. */
	#joins: string[] = [];
	/** Row lock mode — Story 30.8. */
	#lockMode: "FOR UPDATE" | "FOR SHARE" | null = null;
	/** Per-query debug flag — Story 29.11. */
	#debugFlag = false;
	/** Distinct flag — Story 29.5. */
	#distinct = false;
	/** SQL dialect for compilation — inherited from the owning BaseRepository. */
	#dialect: AtlasDialect;

	constructor(
		tableName: string,
		db: DatabaseConnection,
		hydrateFn: (row: Record<string, unknown>) => T,
		entityClass: new () => T,
		resolveColumn: ColumnResolver = (c) => c,
		softDeletes = false,
		dialect: AtlasDialect = getAtlasDialect(),
	) {
		this.#tableName = tableName;
		this.#db = db;
		this.#hydrateFn = hydrateFn;
		this.#entityClass = entityClass;
		this.#resolveColumn = resolveColumn;
		this.#softDeletes = softDeletes;
		this.#dialect = dialect;
	}

	/** Include soft-deleted rows in the result (default behavior excludes them). */
	withTrashed(): this {
		this.#softScope = "with-trashed";
		return this;
	}

	/** Return ONLY soft-deleted rows (deleted_at IS NOT NULL). */
	onlyTrashed(): this {
		this.#softScope = "only-trashed";
		return this;
	}

	/**
	 * Eager-load a relation (AdonisJS-style).
	 * Relations are never loaded automatically — you must call .preload() explicitly.
	 *
	 * Usage:
	 *   repo.query().preload('posts').exec()
	 *   repo.query().preload('posts', q => q.where('published', true)).exec()
	 */
	preload(relationName: string, callback?: PreloadCallback): this {
		this.#preloads.set(relationName, callback);
		return this;
	}

	/** Select specific columns (default: `*`). Accepts a comma-separated string or an array. */
	select(columns: string | string[]): this {
		this.#select = Array.isArray(columns)
			? columns
			: columns.split(",").map((c) => c.trim());
		return this;
	}

	where(callback: WhereCallback): this;
	where(column: string, value: unknown): this;
	where(column: string, operator: string, value: unknown): this;
	where(
		columnOrCb: string | WhereCallback,
		operatorOrValue?: unknown,
		value?: unknown,
	): this {
		if (typeof columnOrCb === "function") {
			this.#wheres.push(this.#buildGroup("and", columnOrCb));
			return this;
		}
		return this.#pushWhere("and", columnOrCb, operatorOrValue, value);
	}

	orWhere(callback: WhereCallback): this;
	orWhere(column: string, value: unknown): this;
	orWhere(column: string, operator: string, value: unknown): this;
	orWhere(
		columnOrCb: string | WhereCallback,
		operatorOrValue?: unknown,
		value?: unknown,
	): this {
		if (typeof columnOrCb === "function") {
			this.#wheres.push(this.#buildGroup("or", columnOrCb));
			return this;
		}
		return this.#pushWhere("or", columnOrCb, operatorOrValue, value);
	}

	whereNull(column: string): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "IS NULL",
			value: null,
		});
		return this;
	}

	whereNotNull(column: string): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "IS NOT NULL",
			value: null,
		});
		return this;
	}

	/** `WHERE col != ?` — negation of `where`. */
	whereNot(column: string, value: unknown): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "!=",
			value,
		});
		return this;
	}

	/** `WHERE col IN (...)` — accepts an array of values OR a `ModelQuery` subquery source. */
	whereIn(
		column: string,
		source: readonly unknown[] | ModelQuery<BaseEntity>,
	): this {
		if (source instanceof ModelQuery) {
			this.#wheres.push({
				type: "and",
				kind: "inSub",
				negated: false,
				column: this.#resolveColumn(column),
				subquery: source.#buildSpec(),
			});
			return this;
		}
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "IN",
			value: [...source],
		});
		return this;
	}

	/** `WHERE col NOT IN (...)` — accepts an array of values OR a `ModelQuery` subquery source. */
	whereNotIn(
		column: string,
		source: readonly unknown[] | ModelQuery<BaseEntity>,
	): this {
		if (source instanceof ModelQuery) {
			this.#wheres.push({
				type: "and",
				kind: "inSub",
				negated: true,
				column: this.#resolveColumn(column),
				subquery: source.#buildSpec(),
			});
			return this;
		}
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "NOT IN",
			value: [...source],
		});
		return this;
	}

	/** `WHERE col BETWEEN ? AND ?` — inclusive range. */
	whereBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "BETWEEN",
			value: [...range],
		});
		return this;
	}

	/** `WHERE col NOT BETWEEN ? AND ?` */
	whereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "NOT BETWEEN",
			value: [...range],
		});
		return this;
	}

	/** `WHERE col LIKE ?` — case-sensitive pattern match. */
	whereLike(column: string, pattern: string): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "LIKE",
			value: pattern,
		});
		return this;
	}

	/**
	 * `WHERE col ILIKE ?` — case-insensitive pattern match. Uses native ILIKE
	 * on PostgreSQL; the Rust compiler rewrites it to `LOWER(col) LIKE LOWER(?)`
	 * on SQLite and MySQL at compile time.
	 */
	whereILike(column: string, pattern: string): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "ILIKE",
			value: pattern,
		});
		return this;
	}

	// ─── OR-combined variants (AdonisJS orWhere* family) ─────────
	// Same predicates as the whereX methods above, combined with OR instead of
	// AND — the named ergonomics Lucid exposes (vs emulating with `orWhere(cb)`).

	/** `OR col IS NULL`. */
	orWhereNull(column: string): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "IS NULL", value: null });
		return this;
	}

	/** `OR col IS NOT NULL`. */
	orWhereNotNull(column: string): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "IS NOT NULL", value: null });
		return this;
	}

	/** `OR col != ?`. */
	orWhereNot(column: string, value: unknown): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "!=", value });
		return this;
	}

	/** `OR col IN (...)` — array or `ModelQuery` subquery source. */
	orWhereIn(column: string, source: readonly unknown[] | ModelQuery<BaseEntity>): this {
		if (source instanceof ModelQuery) {
			this.#wheres.push({ type: "or", kind: "inSub", negated: false, column: this.#resolveColumn(column), subquery: source.#buildSpec() });
			return this;
		}
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "IN", value: [...source] });
		return this;
	}

	/** `OR col NOT IN (...)` — array or `ModelQuery` subquery source. */
	orWhereNotIn(column: string, source: readonly unknown[] | ModelQuery<BaseEntity>): this {
		if (source instanceof ModelQuery) {
			this.#wheres.push({ type: "or", kind: "inSub", negated: true, column: this.#resolveColumn(column), subquery: source.#buildSpec() });
			return this;
		}
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "NOT IN", value: [...source] });
		return this;
	}

	/** `OR col BETWEEN ? AND ?`. */
	orWhereBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "BETWEEN", value: [...range] });
		return this;
	}

	/** `OR col NOT BETWEEN ? AND ?`. */
	orWhereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "NOT BETWEEN", value: [...range] });
		return this;
	}

	/** `OR col LIKE ?`. */
	orWhereLike(column: string, pattern: string): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "LIKE", value: pattern });
		return this;
	}

	/** `OR col ILIKE ?` (rewritten to LOWER() LIKE LOWER() on sqlite/mysql). */
	orWhereILike(column: string, pattern: string): this {
		this.#wheres.push({ type: "or", column: this.#resolveColumn(column), operator: "ILIKE", value: pattern });
		return this;
	}

	/**
	 * **⚠ UNSAFE** — append a raw SQL fragment to the WHERE clause with
	 * `?`-style bindings. The Rust compiler re-indexes the placeholders so they
	 * don't clash with other clause params, but everything else in `sql` is
	 * trusted verbatim. Caller is responsible for the fragment's safety — all
	 * **values** must still go through `bindings`.
	 *
	 * Prefer `whereExpr()` for the common case of a column-referencing predicate
	 * where Atlas can handle the identifier quoting for you. Reach for
	 * `whereRaw` only when the SQL is a dialect-specific construct with no
	 * typed equivalent (window functions, `DATE_TRUNC`, vendor extensions…).
	 *
	 *     query.whereRaw('total > ? AND created_at < ?', [100, '2026-01-01'])
	 *
	 * **Strict mode**: when `setAtlasStrictMode(true)` is active (or the
	 * `ATLAS_STRICT` env var is set), this method throws unless called via the
	 * framework-internal `__unsafeWhereRaw` path. Production apps should enable
	 * strict mode and rewrite call sites to use `whereExpr()` / structured
	 * builders.
	 *
	 * @unsafe Raw SQL fragment — never concatenate user input into `sql`.
	 */
	whereRaw(sql: string, bindings: readonly unknown[] = []): this {
		if (isAtlasStrictMode() && !isInternalBypass()) {
			throw new Error(
				"whereRaw() is disabled in Atlas strict mode. " +
					"Use whereExpr() or a structured builder method instead. " +
					"Call setAtlasStrictMode(false) at bootstrap if you truly need raw SQL.",
			);
		}
		return this.#pushWhereRaw(sql, bindings);
	}

	/**
	 * Framework-internal raw WHERE path — bypasses strict mode. Used by
	 * relation preload resolvers (join predicates, pivot correlations) and by
	 * the internal `whereExpr(col, extra, op, value)` helper, which has
	 * already validated the fragment against a safe charset.
	 *
	 * Not exported from the package barrel — only accessible inside the Atlas
	 * codebase via direct ModelQuery instance access.
	 */
	#pushWhereRaw(sql: string, bindings: readonly unknown[] = []): this {
		this.#wheres.push({
			type: "and",
			kind: "raw",
			sql,
			bindings: [...bindings],
		});
		return this;
	}

	/**
	 * **SAFE** alternative to `whereRaw` for the common case of a single
	 * SQL expression built from a validated column + operator + bound value.
	 * The column goes through the normal identifier quoter (rejects injection
	 * characters), the operator is validated against the Rust allowlist, and
	 * the value is always bound — never interpolated. Use this in app code;
	 * reserve `whereRaw` for dialect-specific constructs with no typed form.
	 *
	 *     query.whereExpr('total', '>', 100)                   // WHERE "total" > ?
	 *     query.whereExpr('total', '+ tax', '>=', 100)         // WHERE "total" + tax >= ?
	 *
	 * The optional `extraExpression` parameter is appended to the quoted column
	 * identifier before the operator — handy for `+`, `-`, or function
	 * wrapping. It must match `[A-Za-z0-9_() +\-*\/,]+` (no quotes, no
	 * semicolons, no comments) or the call throws at construction time.
	 */
	whereExpr(column: string, operator: string, value: unknown): this;
	whereExpr(
		column: string,
		extraExpression: string,
		operator: string,
		value: unknown,
	): this;
	whereExpr(
		column: string,
		operatorOrExtra: string,
		operatorOrValue: unknown,
		maybeValue?: unknown,
	): this {
		// 3-arg form: whereExpr(col, op, value)
		// 4-arg form: whereExpr(col, extraExpr, op, value)
		const hasExtra = maybeValue !== undefined;
		const extra = hasExtra ? operatorOrExtra : "";
		const op = hasExtra ? (operatorOrValue as string) : operatorOrExtra;
		const value = hasExtra ? maybeValue : operatorOrValue;
		if (hasExtra) {
			if (!/^[A-Za-z0-9_() +\-*/,]+$/.test(extra)) {
				throw new Error(
					`whereExpr: extraExpression '${extra}' contains forbidden characters. ` +
						`Only [A-Za-z0-9_() +-*/,] are allowed. Use whereRaw() if you need more.`,
				);
			}
			// The charset alone doesn't stop a structural break-out like
			// `) OR (1` — require balanced parentheses so `extra` can't
			// close the column's context and splice a new predicate.
			if (!hasBalancedParens(extra)) {
				throw new Error(
					`whereExpr: extraExpression '${extra}' has unbalanced parentheses. Use whereRaw() if you need more.`,
				);
			}
			// `op` is interpolated raw into the fragment below, so it MUST be
			// allow-listed — the 3-arg path gets this from the Rust operator
			// validation, but the raw 4-arg path bypasses Rust and would
			// otherwise let `op` inject (e.g. `'> 0 OR 1=1 --'`).
			if (!WHEREEXPR_OPERATORS.has(op)) {
				throw new Error(
					`whereExpr: operator '${op}' is not allowed. Use one of ${[...WHEREEXPR_OPERATORS].join(" ")}, or whereRaw() for anything else.`,
				);
			}
		}
		const resolved = this.#resolveColumn(column);
		// Route through the standard WHERE path so the Rust compiler quotes the
		// column and validates the operator. For the extra-expression form we
		// build a raw WHERE internally via #pushWhereRaw (strict-mode exempt) —
		// but only AFTER we've validated the extra charset + paren balance AND
		// the operator against the allow-list above.
		if (hasExtra) {
			const q = this.#quote(resolved);
			return this.#pushWhereRaw(`${q} ${extra} ${op} ?`, [value]);
		}
		this.#wheres.push({ type: "and", column: resolved, operator: op, value });
		return this;
	}

	/**
	 * Compare two COLUMNS (AdonisJS/Knex `whereColumn`) — `WHERE "a" op "b"`.
	 * Both sides go through the identifier quoter (injection-safe) and the
	 * operator is allow-listed; nothing is bound (it's a column reference, not a
	 * value), which the standard `where`/`whereExpr` value-binding path can't do.
	 */
	whereColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("and", left, operator, right);
	}

	/** `OR`-combined {@link whereColumn}. */
	orWhereColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("or", left, operator, right);
	}

	#whereColumn(
		type: "and" | "or",
		left: string,
		operator: string,
		right: string,
	): this {
		if (!WHEREEXPR_OPERATORS.has(operator)) {
			throw new Error(
				`whereColumn: operator '${operator}' is not allowed. Use one of ${[...WHEREEXPR_OPERATORS].join(" ")}.`,
			);
		}
		// Both operands are interpolated as raw identifiers (no value binding for a
		// column reference), and #quote is a plain wrapper that does NOT escape an
		// embedded quote — so validate each RESOLVED identifier against a strict
		// `[table.]column` charset. This closes the injection surface regardless of
		// what #resolveColumn returns (it can be an identity resolver on sub-queries).
		const safe = (name: string): string => {
			const resolved = this.#resolveColumn(name);
			if (!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(resolved)) {
				throw new Error(
					`whereColumn: '${name}' is not a valid column identifier ([table.]column, alphanumeric + underscore).`,
				);
			}
			// Quote each dotted segment separately → `"table"."column"`, never a
			// single mis-quoted `"table.column"`.
			return resolved
				.split(".")
				.map((part) => this.#quote(part))
				.join(".");
		};
		const sql = `${safe(left)} ${operator} ${safe(right)}`;
		this.#wheres.push({ type, kind: "raw", sql, bindings: [] });
		return this;
	}

	/**
	 * `WHERE EXISTS (SELECT * FROM related WHERE <join> AND <cb>)` — filter parent rows
	 * by the existence of related rows, optionally constrained by a callback.
	 *
	 *     userRepo.query().whereHas('comments', q => q.where('approved', true))
	 */
	whereHas(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		this.#wheres.push(
			this.#buildExistsClause("and", false, relationName, callback),
		);
		return this;
	}

	/** `OR WHERE EXISTS (...)` — composes with surrounding WHERE groups. */
	orWhereHas(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		this.#wheres.push(
			this.#buildExistsClause("or", false, relationName, callback),
		);
		return this;
	}

	/** `WHERE NOT EXISTS (...)` — negation of whereHas. */
	whereDoesntHave(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		this.#wheres.push(
			this.#buildExistsClause("and", true, relationName, callback),
		);
		return this;
	}

	orWhereDoesntHave(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		this.#wheres.push(
			this.#buildExistsClause("or", true, relationName, callback),
		);
		return this;
	}

	/**
	 * Short form of `whereHas`. With an operator + count, emits a count threshold:
	 *   has('comments')          → EXISTS (SELECT * FROM comments WHERE <join>)
	 *   has('comments', '>', 2)  → EXISTS (... HAVING COUNT(*) > ?)
	 */
	has(relationName: string, countOp?: string, countThreshold?: number): this {
		this.#wheres.push(
			this.#buildExistsClause(
				"and",
				false,
				relationName,
				undefined,
				countOp,
				countThreshold,
			),
		);
		return this;
	}

	orHas(relationName: string, countOp?: string, countThreshold?: number): this {
		this.#wheres.push(
			this.#buildExistsClause(
				"or",
				false,
				relationName,
				undefined,
				countOp,
				countThreshold,
			),
		);
		return this;
	}

	/** `WHERE NOT EXISTS (...)` — short form. */
	doesntHave(relationName: string): this {
		this.#wheres.push(this.#buildExistsClause("and", true, relationName));
		return this;
	}

	/**
	 * Set this query's projection alias — only meaningful when this ModelQuery
	 * is used as the sub-builder callback argument of `withCount` / `withAggregate`.
	 * The outer query reads `#subqueryAlias` to rename the `$extras` key.
	 *
	 *     repo.query().withCount('posts', q => q.as('published').where('published', true))
	 *     // → $extras.published (instead of posts_count)
	 */
	as(alias: string): this {
		this.#subqueryAlias = alias;
		return this;
	}

	/** Read-only accessor used by lazy loaders to recover the alias set via `.as()`. */
	get subqueryAlias(): string | undefined {
		return this.#subqueryAlias;
	}

	/** Read-only accessor used by lazy loaders to list the aliases projected by withCount/withAggregate. */
	get projectedAliases(): readonly string[] {
		return this.#selectSubqueries.map((s) => s.alias);
	}

	// --- Sub-builder aggregate setters (used inside withCount / withAggregate callbacks) ---

	/** Set this sub-builder's SELECT to an aggregate expression. Used inside `withAggregate` callbacks. */
	selectAggregate(
		kind: "count" | "sum" | "avg" | "min" | "max",
		column: string = "*",
	): this {
		const fn = kind.toUpperCase();
		if (column === "*") {
			this.#select = [`${fn}(*)`];
		} else {
			this.#select = [`${fn}(${this.#resolveColumn(column)})`];
		}
		return this;
	}

	// --- Top-level scalar executors (Story 29.5) ---

	/** `SELECT COUNT(col)` — executes and returns the scalar. `col` defaults to `*`. */
	async count(column: string = "*"): Promise<number> {
		const expr =
			column === "*" ? "COUNT(*)" : `COUNT(${this.#quoteCol(column)})`;
		return Number((await this.#runScalar(expr)) ?? 0);
	}

	async sum(column: string): Promise<number | null> {
		const v = await this.#runScalar(`SUM(${this.#quoteCol(column)})`);
		return v === null || v === undefined ? null : Number(v);
	}

	async avg(column: string): Promise<number | null> {
		const v = await this.#runScalar(`AVG(${this.#quoteCol(column)})`);
		return v === null || v === undefined ? null : Number(v);
	}

	async min(column: string): Promise<number | null> {
		const v = await this.#runScalar(`MIN(${this.#quoteCol(column)})`);
		return v === null || v === undefined ? null : Number(v);
	}

	async max(column: string): Promise<number | null> {
		const v = await this.#runScalar(`MAX(${this.#quoteCol(column)})`);
		return v === null || v === undefined ? null : Number(v);
	}

	/**
	 * Project a correlated `COUNT(*)` of a relation as an extra column. Default
	 * alias is `${relationName}_count`; override by calling `.as('alias')` inside
	 * the optional callback. The count lands on `entity.$extras[alias]`.
	 *
	 *     userRepo.query().withCount('posts')                               // → $extras.posts_count
	 *     userRepo.query().withCount('posts', q => q.where('published', true))
	 *     userRepo.query().withCount('posts', q => q.as('published_count').where('published', true))
	 */
	withCount(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		this.#selectSubqueries.push(
			this.#buildRelationSubquery(
				relationName,
				callback,
				"count",
				`${relationName}_count`,
			),
		);
		return this;
	}

	/**
	 * Project any aggregate (sum/avg/min/max/count) of a relation as an extra column.
	 * The callback MUST set the aggregate via `.sum('col')` / `.avg(...)` etc. and
	 * typically also set an alias via `.as('name')`.
	 *
	 *     userRepo.query().withAggregate('posts', q => q.sum('views').as('total_views'))
	 */
	withAggregate(
		relationName: string,
		callback: (query: ModelQuery<BaseEntity>) => void,
	): this {
		this.#selectSubqueries.push(
			this.#buildRelationSubquery(
				relationName,
				callback,
				"aggregate",
				relationName,
			),
		);
		return this;
	}

	orderBy(column: string, direction: "asc" | "desc" = "asc"): this {
		this.#orderBys.push({ column: this.#resolveColumn(column), direction });
		return this;
	}

	limit(n: number): this {
		// Guard here with a clear message — the Rust spec types limit as
		// u64, so a negative/non-integer otherwise surfaces as a cryptic
		// serde deserialization error at compile time. Matches the
		// QueryBuilder.limit guard.
		if (!Number.isInteger(n) || n < 0) {
			throw new Error(`limit must be a non-negative integer, got ${n}`);
		}
		this.#limit = n;
		return this;
	}

	offset(n: number): this {
		if (!Number.isInteger(n) || n < 0) {
			throw new Error(`offset must be a non-negative integer, got ${n}`);
		}
		this.#offset = n;
		return this;
	}

	/** Execute and return the first matching entity or null. Fires beforeFind/afterFind. */
	async first(): Promise<T | null> {
		this.#limit = 1;
		await fireHooks(this.#entityClass, "beforeFind", this);
		// Bypass exec() so the multi-row beforeFetch/afterFetch hooks don't ALSO
		// fire — first() is the single-row terminal and owns beforeFind/afterFind.
		const result = (await this.#doExec())[0] ?? null;
		await fireHooks(this.#entityClass, "afterFind", result);
		return result;
	}

	/** Execute and return the first matching entity or throw. */
	async firstOrFail(): Promise<T> {
		const result = await this.first();
		if (!result) throw new Error(`No ${this.#tableName} found matching query`);
		return result;
	}

	/**
	 * Return the single matching row, or throw if there are zero OR more than one
	 * (AdonisJS/Laravel `sole`). Use when exactly one row is a correctness
	 * invariant — a second match signals a bug the silent `first()` would hide.
	 */
	async sole(): Promise<T> {
		const rows = await this.limit(2).exec();
		if (rows.length === 0) {
			throw new Error(`No ${this.#tableName} found matching query (sole()).`);
		}
		if (rows.length > 1) {
			throw new Error(
				`Expected exactly one ${this.#tableName} but the query matched multiple rows (sole()).`,
			);
		}
		return rows[0];
	}

	/**
	 * Thenable — `await someQuery` is equivalent to `await someQuery.exec()`.
	 * A chain like `await repo.query().where('active', true).orderBy('id')`
	 * works without an explicit `.exec()` thanks to this method.
	 *
	 * Idempotent: `exec()` memoizes its promise, so awaiting the same builder
	 * twice — or any Promise-like assimilation (Promise.resolve, Promise.all,
	 * vitest's `.resolves` matcher, instrumentation libs that probe `.then`,
	 * dynamic-import unwrap) — shares one SQL round-trip. Call `.clone()` to
	 * get a fresh builder that re-executes.
	 */
	// biome-ignore lint/suspicious/noThenProperty: thenable IS the public API — `await someQuery` is the documented ergonomic for the builder. Removing `.then` breaks every call site.
	then<TResult1 = T[], TResult2 = never>(
		onfulfilled?:
			| ((value: T[]) => TResult1 | PromiseLike<TResult1>)
			| null
			| undefined,
		onrejected?:
			| ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
			| null
			| undefined,
	): Promise<TResult1 | TResult2> {
		return this.exec().then(onfulfilled, onrejected);
	}

	/** Build the spec object that gets sent to the Rust compiler. Extracted so whereHas can reuse it for sub-queries. */
	#buildSpec(): SelectSpec {
		const wheres: WhereClause[] = [...this.#wheres];
		// Auto-apply soft-delete scope when the entity opts in via @SoftDeletes
		if (this.#softDeletes) {
			if (this.#softScope === "default") {
				wheres.push({
					type: "and",
					column: "deleted_at",
					operator: "IS NULL",
					value: null,
				});
			} else if (this.#softScope === "only-trashed") {
				wheres.push({
					type: "and",
					column: "deleted_at",
					operator: "IS NOT NULL",
					value: null,
				});
			}
			// 'with-trashed' adds no filter
		}

		return {
			kind: "select",
			table: this.#tableName,
			select: this.#select,
			selectSubqueries: this.#selectSubqueries,
			wheres,
			orderBy: this.#orderBys,
			groupBy: [],
			having: [],
			limit: this.#limit ?? null,
			offset: this.#offset ?? null,
			distinct: this.#distinct,
			ctes: [],
			unions: [],
			joins: this.#joins,
			lockMode: this.#lockMode,
		};
	}

	/** Build SQL + params via the Rust query compiler. */
	toSQL(): { sql: string; params: unknown[] } {
		const compiled = compileStatementNative(this.#buildSpec(), this.#dialect);
		return { sql: compiled.statements[0], params: compiled.params };
	}

	/**
	 * Cached exec result. Memoizing the promise makes the builder a one-shot
	 * Promise-like: multiple awaits / `Promise.resolve(query)` / `then` probes
	 * by instrumentation libraries / `expect().resolves` / dynamic-import
	 * unwrap — all share the same SQL round-trip. Pre-memoization, any
	 * Promise-like assimilation silently triggered the query a second time.
	 *
	 * Callers that want a fresh query result must `.clone()` the builder.
	 */
	#cachedExec?: Promise<T[]>;

	/** Execute and return all matching entities, with preloaded relations. Fires beforeFetch/afterFetch. */
	exec(): Promise<T[]> {
		this.#cachedExec ??= this.#execWithFetchHooks();
		return this.#cachedExec;
	}

	async #execWithFetchHooks(): Promise<T[]> {
		await fireHooks(this.#entityClass, "beforeFetch", this);
		const results = await this.#doExec();
		await fireHooks(this.#entityClass, "afterFetch", results);
		return results;
	}

	async #doExec(): Promise<T[]> {
		const { sql, params } = this.toSQL();
		const rawRows = await this.#db.query<Record<string, unknown>>(sql, params);
		// Peel withCount / withAggregate alias columns off the raw row into $extras
		// BEFORE hydration, so the hydrator doesn't try to interpret them as columns.
		const extraKeys = this.#selectSubqueries.map((s) => s.alias);
		const entities = rawRows.map((row) => {
			const picked: Record<string, unknown> = {};
			for (const key of extraKeys) {
				if (key in row) {
					picked[key] = row[key];
					delete row[key];
				}
			}
			const entity = this.#hydrateFn(row);
			for (const [k, v] of Object.entries(picked)) entity.setExtra(k, v);
			return entity;
		});

		// Resolve preloads (eager loading)
		if (this.#preloads.size > 0 && this.#entityClass && entities.length > 0) {
			await this.#resolvePreloads(entities);
		}

		return entities;
	}

	/** Resolve preloaded relations via batched subqueries (no N+1). */
	async #resolvePreloads(entities: T[]): Promise<void> {
		if (!this.#entityClass) return;
		const relations = getRelationMetadata(this.#entityClass);

		for (const relationName of this.#preloads.keys()) {
			const relation = relations.find((r) => r.propertyKey === relationName);
			if (!relation) continue;

			const ctx = this.#buildPreloadContext(relation, relationName);
			if (!ctx) continue;

			const allRelated = await this.#resolveOneRelation(
				entities,
				relationName,
				relation.type,
				ctx,
			);
			await this.#applyNestedPreloads(allRelated, ctx);
		}
	}

	/** Per-preload constants (related class, table, pk, hydrator, query helper, nested callback). */
	#buildPreloadContext(
		relation: RelationMetadata,
		relationName: string,
	): PreloadContext | null {
		const relatedClass = relation.target() as new () => BaseEntity;
		const relatedMeta = getEntityMetadata(relatedClass);
		if (!relatedMeta) return null;

		// Resolve row keys against declared column metadata, NOT `in entity` —
		// entities using Adonis' `declare field: T` pattern have no own-properties
		// on a freshly constructed instance, so `key in entity` is always false and
		// every column would be silently dropped. Mirrors `BaseRepository.#hydrate`.
		const relatedPkName = getPrimaryKey(relatedClass) ?? "id";
		const validColumns = new Set<string>();
		for (const col of getColumnMetadata(relatedClass)) {
			validColumns.add(col.propertyKey);
			validColumns.add(camelToSnake(col.propertyKey));
		}
		validColumns.add(relatedPkName);
		validColumns.add(camelToSnake(relatedPkName));

		const hydrate = (row: Record<string, unknown>): BaseEntity => {
			const entity = new relatedClass();
			for (const [key, value] of Object.entries(row)) {
				const camelKey = snakeToCamel(key);
				const targetKey = validColumns.has(camelKey)
					? camelKey
					: validColumns.has(key)
						? key
						: null;
				if (targetKey !== null) entity.setProp(targetKey, value);
			}
			return entity;
		};

		return {
			relation,
			relationName,
			relatedClass,
			relatedTable: relatedMeta.tableName,
			relatedPk: getPrimaryKey(relatedClass) ?? "id",
			hydrate,
			runInQuery: (table, column, values) =>
				this.#runInQuery(table, column, values),
			runRelationQuery: (column, values) =>
				this.#runRelationQuery(
					relatedMeta.tableName,
					relatedClass,
					column,
					values,
					relation,
					this.#preloads.get(relationName),
				),
			nestedCallback: this.#preloads.get(relationName),
		};
	}

	/** Dispatch to the appropriate relation resolver based on the relation type. */
	async #resolveOneRelation(
		entities: T[],
		relationName: string,
		type: RelationMetadata["type"],
		ctx: PreloadContext,
	): Promise<BaseEntity[]> {
		switch (type) {
			case "hasMany":
				return this.#resolveHasMany(entities, relationName, ctx);
			case "hasOne":
				return this.#resolveHasOne(entities, relationName, ctx);
			case "belongsTo":
				return this.#resolveBelongsTo(entities, relationName, ctx);
			case "manyToMany":
				return this.#resolveManyToMany(entities, relationName, ctx);
			case "hasOneThrough":
			case "hasManyThrough":
				return this.#resolveThrough(
					entities,
					relationName,
					ctx,
					type === "hasOneThrough",
				);
		}
	}

	/**
	 * Two-hop relations (Story 31.2). Walks parent → intermediate → related in
	 * two SELECTs (N+1 would be worse) and groups the final rows by the parent
	 * id discovered through the intermediate join.
	 */
	async #resolveThrough(
		entities: T[],
		relationName: string,
		ctx: PreloadContext,
		single: boolean,
	): Promise<BaseEntity[]> {
		const relation = ctx.relation;
		if (!relation.through) {
			throw new Error(
				`@HasOneThrough/@HasManyThrough '${relationName}' requires a through model`,
			);
		}
		const throughClass = relation.through() as new () => BaseEntity;
		const throughMeta = getEntityMetadata(throughClass);
		if (!throughMeta)
			throw new Error(
				`Entity metadata missing on through class ${throughClass.name}`,
			);
		const throughTable = throughMeta.tableName;
		const throughPk = getPrimaryKey(throughClass) ?? "id";
		const parentLocal =
			relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
		const firstKey =
			relation.firstKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
		const secondKey =
			relation.secondKey ?? `${camelToSnake(throughClass.name)}_id`;
		const secondLocal = relation.secondLocalKey ?? throughPk;

		const parentIds = entities
			.map((e) => e[parentLocal])
			.filter((v) => v != null);
		if (parentIds.length === 0) {
			return assignEmptyRelation(entities, relationName, single);
		}

		// Step 1 — intermediate rows: (throughPk, firstKey)
		const throughRows = await ctx.runInQuery(throughTable, firstKey, parentIds);
		if (throughRows.length === 0) {
			return assignEmptyRelation(entities, relationName, single);
		}
		// Map secondLocal (= through PK by default) → parentId, throwing on a
		// non-unique key that would silently drop data.
		const throughToParent = buildThroughToParent(
			throughRows,
			secondLocal,
			firstKey,
			{
				relationName,
				throughTable,
				throughClass: throughClass.name,
				throughPk,
			},
		);

		// Step 2 — related rows where secondKey IN (throughPk)
		const throughIds = [...throughToParent.keys()];
		const relRows = await ctx.runRelationQuery(secondKey, throughIds);

		const grouped = new Map<unknown, BaseEntity[]>();
		const allRelated: BaseEntity[] = [];
		for (const row of relRows) {
			const hydrated = ctx.hydrate(row);
			const parentId = throughToParent.get(row[secondKey]);
			if (!grouped.has(parentId)) grouped.set(parentId, []);
			grouped.get(parentId)?.push(hydrated);
			allRelated.push(hydrated);
		}

		for (const entity of entities) {
			const matches = grouped.get(entity[parentLocal]) ?? [];
			entity.setProp(relationName, single ? (matches[0] ?? null) : matches);
		}
		return allRelated;
	}

	async #resolveHasOne(
		entities: T[],
		relationName: string,
		ctx: PreloadContext,
	): Promise<BaseEntity[]> {
		const fk =
			ctx.relation.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
		const pk =
			ctx.relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
		const ids = entities.map((e) => e[pk]).filter((v) => v != null);
		if (ids.length === 0) {
			for (const e of entities) e.setProp(relationName, null);
			return [];
		}
		const relRows = await ctx.runRelationQuery(fk, ids);
		// Track how many rows match each parent id. More than one = invariant
		// violation on a `@HasOne` relation — throw instead of silently dropping
		// the extras (which would hide real data integrity bugs).
		const indexed = new Map<unknown, BaseEntity>();
		const counts = new Map<unknown, number>();
		const allRelated: BaseEntity[] = [];
		for (const row of relRows) {
			const key = row[fk];
			const next = (counts.get(key) ?? 0) + 1;
			counts.set(key, next);
			if (next > 1) {
				throw new Error(
					`@HasOne invariant violated: ${this.#entityClass.name}.${relationName} ` +
						`found ${next} rows in "${ctx.relatedTable}" for parent ${pk}=${String(key)}. ` +
						`Use @HasMany if multiple rows are expected, or add a unique index on "${fk}".`,
				);
			}
			const hydrated = ctx.hydrate(row);
			indexed.set(key, hydrated);
			allRelated.push(hydrated);
		}
		for (const entity of entities) {
			entity.setProp(relationName, indexed.get(entity[pk]) ?? null);
		}
		return allRelated;
	}

	async #resolveHasMany(
		entities: T[],
		relationName: string,
		ctx: PreloadContext,
	): Promise<BaseEntity[]> {
		const fk =
			ctx.relation.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
		const pk =
			ctx.relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
		const ids = entities.map((e) => e[pk]).filter((v) => v != null);
		if (ids.length === 0) return [];

		const relRows = await ctx.runRelationQuery(fk, ids);
		const grouped = new Map<unknown, BaseEntity[]>();
		const allRelated: BaseEntity[] = [];
		for (const row of relRows) {
			const key = row[fk];
			const hydrated = ctx.hydrate(row);
			if (!grouped.has(key)) grouped.set(key, []);
			grouped.get(key)?.push(hydrated);
			allRelated.push(hydrated);
		}

		for (const entity of entities) {
			entity.setProp(relationName, grouped.get(entity[pk]) ?? []);
		}
		return allRelated;
	}

	async #resolveBelongsTo(
		entities: T[],
		relationName: string,
		ctx: PreloadContext,
	): Promise<BaseEntity[]> {
		const fk =
			ctx.relation.foreignKey ?? `${camelToSnake(ctx.relatedClass.name)}_id`;
		const fkProp = `${relationName}Id`;
		const ids = entities
			.map((e) => e[fkProp] ?? e[fk])
			.filter((v) => v != null);
		const uniqueIds = [...new Set(ids)];
		if (uniqueIds.length === 0) return [];

		const relRows = await ctx.runRelationQuery(ctx.relatedPk, uniqueIds);
		const indexed = new Map<unknown, BaseEntity>();
		const allRelated: BaseEntity[] = [];
		for (const row of relRows) {
			const hydrated = ctx.hydrate(row);
			indexed.set(row[ctx.relatedPk], hydrated);
			allRelated.push(hydrated);
		}

		for (const entity of entities) {
			const fkValue = entity[fkProp] ?? entity[fk];
			entity.setProp(relationName, indexed.get(fkValue) ?? null);
		}
		return allRelated;
	}

	async #resolveManyToMany(
		entities: T[],
		relationName: string,
		ctx: PreloadContext,
	): Promise<BaseEntity[]> {
		if (!ctx.relation.pivot) {
			throw new Error(
				`@ManyToMany on ${this.#entityClass.name}.${relationName} requires pivot options`,
			);
		}
		const pivot = ctx.relation.pivot;
		// Default pivot FK = `<model_snake>_id`, derived from the entity CLASS name
		// (singular by convention), consistent with hasMany/hasOne/belongsTo. Do NOT
		// singularize the plural TABLE name by stripping a trailing `s` — that breaks
		// on `status`/`address`/`campus` (→ `statu_id`). Explicit pivot keys win.
		const foreignKey =
			pivot.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
		const otherKey =
			pivot.otherKey ?? `${camelToSnake(ctx.relatedClass.name)}_id`;
		const pk = getPrimaryKey(this.#entityClass) ?? "id";

		const ids = entities.map((e) => e[pk]).filter((v) => v != null);
		if (ids.length === 0) return [];

		// Step 1 — pivot table: find (foreignKey → otherKey) pairs
		const pivotRows = await ctx.runInQuery(pivot.pivotTable, foreignKey, ids);
		if (pivotRows.length === 0) {
			for (const entity of entities) entity.setProp(relationName, []);
			return [];
		}
		const otherIds = [
			...new Set(pivotRows.map((r) => r[otherKey]).filter((v) => v != null)),
		];

		// Step 2 — load all related entities in one query
		const relRows = await ctx.runRelationQuery(ctx.relatedPk, otherIds);
		const byRelatedPk = new Map<unknown, BaseEntity>();
		const allRelated: BaseEntity[] = [];
		for (const row of relRows) {
			const hydrated = ctx.hydrate(row);
			byRelatedPk.set(row[ctx.relatedPk], hydrated);
			allRelated.push(hydrated);
		}

		// Step 3 — group via the pivot
		const grouped = new Map<unknown, BaseEntity[]>();
		for (const pivotRow of pivotRows) {
			const related = byRelatedPk.get(pivotRow[otherKey]);
			if (!related) continue;
			const parentId = pivotRow[foreignKey];
			if (!grouped.has(parentId)) grouped.set(parentId, []);
			grouped.get(parentId)?.push(related);
		}

		for (const entity of entities) {
			entity.setProp(relationName, grouped.get(entity[pk]) ?? []);
		}
		return allRelated;
	}

	/** Recursively resolve preloads declared by the nested callback. */
	async #applyNestedPreloads(
		relatedEntities: BaseEntity[],
		ctx: PreloadContext,
	): Promise<void> {
		if (!ctx.nestedCallback || relatedEntities.length === 0) return;
		const sub = new ModelQuery<BaseEntity>(
			ctx.relatedTable,
			this.#db,
			(r) => ctx.hydrate(r),
			ctx.relatedClass,
		);
		ctx.nestedCallback(sub);
		if (sub.#preloads.size > 0) {
			await sub.#resolveAgainst(relatedEntities, ctx.relatedClass);
		}
	}

	/** Compile + execute a `SELECT * FROM <table> WHERE <column> IN (...)` via the Rust compiler. */
	async #runInQuery(
		table: string,
		column: string,
		values: unknown[],
	): Promise<Record<string, unknown>[]> {
		const spec = {
			kind: "select",
			table,
			select: ["*"],
			selectSubqueries: [],
			wheres: [{ column, operator: "IN", value: values, type: "and" }],
			orderBy: [],
			groupBy: [],
			having: [],
			limit: null,
			offset: null,
			distinct: false,
			ctes: [],
			unions: [],
			joins: [],
			lockMode: null,
		};
		const compiled = compileStatementNative(spec, this.#dialect);
		return this.#db.query<Record<string, unknown>>(
			compiled.statements[0],
			compiled.params,
		);
	}

	/**
	 * Run a relation preload against the related table, applying the relation's
	 * declared `onQuery` constraint (Story 31.4) AND the user-supplied preload
	 * callback (e.g. `preload('posts', q => q.where('published', true))`) —
	 * which, prior to this helper, was silently dropped for the primary-level
	 * row set and only applied on nested preloads.
	 *
	 * Returns raw rows (snake_case keys) so existing resolvers can continue to
	 * index/group by FK without a hydration round-trip. Nested preloads declared
	 * inside the callback are re-collected later by `#applyNestedPreloads`.
	 */
	async #runRelationQuery(
		relatedTable: string,
		relatedClass: new () => BaseEntity,
		column: string,
		values: unknown[],
		relation: RelationMetadata,
		userCallback: PreloadCallback | undefined,
	): Promise<Record<string, unknown>[]> {
		const sub = new ModelQuery<BaseEntity>(
			relatedTable,
			this.#db,
			(row) => row as BaseEntity,
			relatedClass,
			(c) => c,
			// Propagate the RELATED entity's soft-delete flag — hardcoding
			// false here meant `preload('posts')` returned soft-deleted
			// posts even when Post is @SoftDeletes (a data leak). The
			// related query now applies its own `deleted_at IS NULL` filter,
			// matching a direct query on that entity. (with-trashed on the
			// related set, if ever needed, would be opted-in via the
			// preload callback.)
			hasSoftDeletes(relatedClass),
			this.#dialect,
		);
		sub.whereIn(column, values);
		if (relation.onQuery) relation.onQuery(sub as unknown);
		if (userCallback) userCallback(sub);
		const { sql, params } = sub.toSQL();
		return this.#db.query<Record<string, unknown>>(sql, params);
	}

	/**
	 * Build a correlated subquery over a relation. Returns `SubqueryProjection`
	 * used by withCount / withAggregate. Default select is `COUNT(*)` for `'count'`
	 * mode; `'aggregate'` mode requires the callback to set the select itself via
	 * `.sum()` / `.avg()` / `.min()` / `.max()` / `.count()`.
	 */
	#buildRelationSubquery(
		relationName: string,
		callback: ((q: ModelQuery<BaseEntity>) => void) | undefined,
		mode: "count" | "aggregate",
		defaultAlias: string,
	): SubqueryProjection {
		const sub = this.#makeRelationSub(relationName);
		if (mode === "count") sub.selectAggregate("count", "*");
		if (callback) callback(sub);
		if (
			mode === "aggregate" &&
			(sub.#select.length !== 1 || sub.#select[0] === "*")
		) {
			throw new Error(
				`withAggregate('${relationName}') callback must set an aggregate via .sum/.avg/.min/.max/.count`,
			);
		}
		const alias = sub.#subqueryAlias ?? defaultAlias;
		return { alias, subquery: sub.#buildSpec() };
	}

	/**
	 * Shared helper for whereHas + withCount + withAggregate: build a sub ModelQuery
	 * on the related table with the correlated join predicate already injected.
	 */
	#makeRelationSub(relationName: string): ModelQuery<BaseEntity> {
		const relations = getRelationMetadata(this.#entityClass);
		const relation = relations.find((r) => r.propertyKey === relationName);
		if (!relation) {
			throw new Error(
				`Relation '${relationName}' not found on ${this.#entityClass.name}`,
			);
		}
		const relatedClass = relation.target() as new () => BaseEntity;
		const relatedMeta = getEntityMetadata(relatedClass);
		if (!relatedMeta) {
			throw new Error(
				`Entity metadata missing on related class ${relatedClass.name}`,
			);
		}
		const relatedTable = relatedMeta.tableName;
		const parentPk = getPrimaryKey(this.#entityClass) ?? "id";
		const parentTable = this.#tableName;
		const q =
			this.#dialect === "mysql"
				? (name: string) => `\`${name}\``
				: (name: string) => `"${name}"`;

		const sub = new ModelQuery<BaseEntity>(
			relatedTable,
			this.#db,
			(row) => row as BaseEntity,
			relatedClass,
			(c) => c,
			false,
			this.#dialect,
		);

		switch (relation.type) {
			case "hasOne":
			case "hasMany": {
				// Honour custom foreignKey/localKey exactly like the eager loader —
				// hard-coding them here produced silently-wrong whereHas/withCount SQL.
				const fk =
					relation.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
				const localKey = relation.localKey ?? parentPk;
				sub.#pushWhereRaw(
					`${q(relatedTable)}.${q(fk)} = ${q(parentTable)}.${q(localKey)}`,
				);
				break;
			}
			case "belongsTo": {
				const fk = relation.foreignKey ?? `${camelToSnake(relatedClass.name)}_id`;
				const ownerKey =
					relation.ownerKey ?? (getPrimaryKey(relatedClass) ?? "id");
				sub.#pushWhereRaw(
					`${q(relatedTable)}.${q(ownerKey)} = ${q(parentTable)}.${q(fk)}`,
				);
				break;
			}
			case "manyToMany": {
				if (!relation.pivot) {
					throw new Error(
						`@ManyToMany on ${this.#entityClass.name}.${relationName} requires pivot options`,
					);
				}
				const pivot = relation.pivot;
				// Default pivot FK from the CLASS name (singular), not the plural table
				// name stripped of a trailing `s` — see the eager loader above.
				const foreignKey =
					pivot.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
				const otherKey =
					pivot.otherKey ?? `${camelToSnake(relatedClass.name)}_id`;
				const relatedPk = getPrimaryKey(relatedClass) ?? "id";
				const localKey = relation.localKey ?? parentPk;
				sub.#pushWhereRaw(
					`${q(relatedTable)}.${q(relatedPk)} IN ` +
						`(SELECT ${q(otherKey)} FROM ${q(pivot.pivotTable)} ` +
						`WHERE ${q(pivot.pivotTable)}.${q(foreignKey)} = ${q(parentTable)}.${q(localKey)})`,
				);
				break;
			}
			default:
				// hasOneThrough / hasManyThrough build a 2-hop correlated subquery,
				// which isn't implemented here. Fail loud — falling through would
				// leave `sub` WITHOUT a join predicate, so whereHas/withCount would
				// silently match/count EVERY related row.
				throw new Error(
					`whereHas/withCount on a '${relation.type}' relation ` +
						`(${this.#entityClass.name}.${relationName}) is not supported yet. ` +
						`Use a direct hasMany/belongsTo/manyToMany relation, or filter via a sub-query.`,
				);
		}
		return sub;
	}

	/**
	 * Resolve a relation to its table + correlated join predicate and return an
	 * `ExistsWhere` clause. Used by whereHas / has / doesntHave / whereDoesntHave.
	 *
	 * The join predicate is injected as a `whereRaw` on the sub-query so the Rust
	 * compiler handles identifier quoting uniformly. ManyToMany uses a pivot
	 * subquery (`EXISTS (SELECT FROM related WHERE id IN (SELECT other_key FROM pivot WHERE foreign_key = parent.id))`).
	 *
	 * Count threshold form (`has('comments', '>', 2)`) adds a HAVING COUNT(*)
	 * without GROUP BY — SQL treats the whole sub-result as one group, so
	 * COUNT(*) against the correlated rows returns the right number.
	 */
	// === Story 29.4 — joins ===========================================================================

	/** `INNER JOIN <table> ON <left> = <right>`. */
	innerJoin(table: string, left: string, right: string): this;
	innerJoin(table: string, build: (j: JoinBuilder) => void): this;
	innerJoin(
		table: string,
		leftOrBuild: string | ((j: JoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("INNER", table, leftOrBuild, right);
	}

	leftJoin(table: string, left: string, right: string): this;
	leftJoin(table: string, build: (j: JoinBuilder) => void): this;
	leftJoin(
		table: string,
		leftOrBuild: string | ((j: JoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("LEFT", table, leftOrBuild, right);
	}

	rightJoin(table: string, left: string, right: string): this;
	rightJoin(table: string, build: (j: JoinBuilder) => void): this;
	rightJoin(
		table: string,
		leftOrBuild: string | ((j: JoinBuilder) => void),
		right?: string,
	): this {
		return this.#pushJoin("RIGHT", table, leftOrBuild, right);
	}

	crossJoin(table: string): this {
		const tq = this.#quote(table);
		this.#joins.push(`CROSS JOIN ${tq}`);
		return this;
	}

	/**
	 * **⚠ UNSAFE** — append a raw JOIN fragment verbatim. No identifier quoting,
	 * no operator validation. Caller is fully responsible for safety.
	 *
	 * Prefer `joinOn()` for the common two-column equi-join case where Atlas
	 * can quote the identifiers for you. Reach for `joinRaw` only when you
	 * need a dialect-specific construct (`LATERAL`, `USING`, index hints…).
	 *
	 *     query.joinRaw('LEFT JOIN LATERAL (SELECT ... FROM ...) t ON true')
	 *
	 * **Strict mode**: throws when `setAtlasStrictMode(true)` is active.
	 * Use `joinOn()` or the callback form of `innerJoin`/`leftJoin`/`rightJoin`
	 * instead.
	 *
	 * @unsafe Raw SQL fragment — never concatenate user input into `fragment`.
	 */
	joinRaw(fragment: string): this {
		if (isAtlasStrictMode() && !isInternalBypass()) {
			throw new Error(
				"joinRaw() is disabled in Atlas strict mode. " +
					"Use joinOn() or the callback form of innerJoin/leftJoin/rightJoin instead.",
			);
		}
		this.#joins.push(fragment);
		return this;
	}

	/**
	 * **SAFE** helper that builds an `INNER JOIN <table> ON <left> = <right>`
	 * with dialect-correct identifier quoting on both sides. Thin sugar over
	 * `innerJoin(table, left, right)` for symmetry with `whereExpr` — both
	 * are the "don't reach for *Raw" entry points.
	 *
	 *     query.joinOn('users', 'users.id', 'orders.user_id')
	 *
	 * Use the callback form of `innerJoin` / `leftJoin` / `rightJoin` when
	 * you need multiple join conditions.
	 */
	joinOn(table: string, left: string, right: string): this {
		return this.innerJoin(table, left, right);
	}

	// === Story 29.5 — aggregates / exists / pluck =====================================================

	distinct(): this {
		this.#distinct = true;
		return this;
	}

	/** `SELECT COUNT(DISTINCT col)`. */
	async countDistinct(column: string): Promise<number> {
		return Number(
			(await this.#runScalar(`COUNT(DISTINCT ${this.#quoteCol(column)})`)) ?? 0,
		);
	}

	/** `SELECT 1 FROM ... LIMIT 1` — returns boolean. */
	async exists(): Promise<boolean> {
		const clone = this.clone();
		clone.#select = ["1"];
		clone.#limit = 1;
		const { sql, params } = clone.toSQL();
		const rows = await this.#db.query<Record<string, unknown>>(sql, params);
		return rows.length > 0;
	}

	async doesntExist(): Promise<boolean> {
		return !(await this.exists());
	}

	/** Flat column projection. Rejects object/relation columns. */
	async pluck(column: string): Promise<unknown[]> {
		const col = this.#resolveColumn(column);
		const clone = this.clone();
		clone.#select = [col];
		const { sql, params } = clone.toSQL();
		const rows = await this.#db.query<Record<string, unknown>>(sql, params);
		return rows.map((row) => {
			const v = row[col];
			if (v !== null && typeof v === "object") {
				throw new Error(
					`pluck('${column}') rejected — column is an object/relation`,
				);
			}
			return v;
		});
	}

	// === Story 29.8 — scopes ===========================================================================

	/** Apply scopes declared on the entity class via `static scopes = {...}`. */
	apply(
		callback: (
			scopes: Record<string, (...args: unknown[]) => ModelQuery<T>>,
		) => void,
	): this {
		const scopes = (
			this.#entityClass as {
				scopes?: Record<string, (q: ModelQuery<T>, ...rest: unknown[]) => void>;
			}
		).scopes;
		if (!scopes)
			throw new Error(`${this.#entityClass.name} declares no static scopes`);
		const proxy: Record<string, (...args: unknown[]) => ModelQuery<T>> = {};
		for (const [name, fn] of Object.entries(scopes)) {
			proxy[name] = (...args: unknown[]) => {
				fn(this, ...args);
				return this;
			};
		}
		const wrapper = new Proxy(proxy, {
			get: (target, prop: string) => {
				if (prop in target) return target[prop];
				throw new Error(
					`Unknown scope '${String(prop)}' on ${this.#entityClass.name}`,
				);
			},
		});
		callback(wrapper);
		return this;
	}

	/** Alias for `apply` — Lucid compatibility. */
	withScopes(
		callback: (
			scopes: Record<string, (...args: unknown[]) => ModelQuery<T>>,
		) => void,
	): this {
		return this.apply(callback);
	}

	// === Story 29.9 — if / unless ======================================================================

	if<V>(
		condition: V | undefined | null | false,
		ifFn: (q: this, value: V) => void,
		elseFn?: (q: this) => void,
	): this {
		if (condition) ifFn(this, condition as V);
		else if (elseFn) elseFn(this);
		return this;
	}

	unless<V>(
		condition: V | undefined | null | false,
		fn: (q: this) => void,
	): this {
		if (!condition) fn(this);
		return this;
	}

	// === Story 29.10 — pagination =====================================================================

	/** Offset-based paginator. */
	async paginate(page: number, perPage: number): Promise<Paginator<T>> {
		const p = Math.max(1, Math.floor(page));
		const pp = Math.max(1, Math.floor(perPage));
		// beforePaginate runs BEFORE cloning so a hook mutating the query (e.g. a
		// tenant scope) propagates into both the COUNT and the data fetch.
		await fireHooks(this.#entityClass, "beforePaginate", this);
		// Parallel COUNT(*) + data fetch
		const countQ = this.clone();
		countQ.#select = ["COUNT(*) AS count"];
		countQ.#limit = undefined;
		countQ.#offset = undefined;
		countQ.#orderBys = [];
		const { sql: cSql, params: cParams } = countQ.toSQL();
		const cRows = await this.#db.query<Record<string, unknown>>(cSql, cParams);
		const total = Number(cRows[0]?.count ?? 0);

		const dataQ = this.clone();
		dataQ.#limit = pp;
		dataQ.#offset = (p - 1) * pp;
		// `#doExec` (not `exec`) so the generic beforeFetch/afterFetch don't fire on
		// top of the paginate hooks — paginate is its own terminal.
		const items = await dataQ.#doExec();
		await fireHooks(this.#entityClass, "afterPaginate", items);
		return new Paginator<T>(items, { total, perPage: pp, currentPage: p });
	}

	/**
	 * Cursor-based pagination — base64 opaque keyset, multi-column aware.
	 *
	 * `orderBy` can be a single column (`'created_at'`) or a tuple
	 * (`['created_at', 'id']`) for stable tie-breaking. The cursor encodes
	 * the last row's values for every ordering column, and the next page
	 * query uses a lexicographic tuple predicate:
	 *
	 *     (col1, col2) > (?, ?)   ≡   col1 > ?  OR (col1 = ? AND col2 > ?)
	 *
	 * Expanded into a disjunctive form because not every supported dialect
	 * accepts row-value comparisons.
	 */
	async cursorPaginate(opts: {
		cursor?: string;
		limit: number;
		orderBy: string | string[];
	}): Promise<{ items: T[]; nextCursor: string | null; hasMore: boolean }> {
		const cols = (
			Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
		).map((c) => this.#resolveColumn(c));
		if (cols.length === 0)
			throw new Error("cursorPaginate requires at least one orderBy column");
		const lim = Math.max(1, Math.floor(opts.limit));
		const clone = this.clone();

		if (opts.cursor) {
			// Cursors arrive from the API boundary (often a query string). Wrap the
			// decode so a malformed cursor produces a controlled user-facing error
			// instead of a raw `SyntaxError` from JSON.parse.
			let decoded: { v: unknown[] };
			try {
				const raw = Buffer.from(opts.cursor, "base64").toString("utf-8");
				decoded = JSON.parse(raw) as { v: unknown[] };
			} catch {
				throw new Error(
					`cursorPaginate: malformed cursor '${opts.cursor.slice(0, 32)}…' — ` +
						`must be a base64-encoded JSON object of shape { v: unknown[] }`,
				);
			}
			if (!Array.isArray(decoded.v) || decoded.v.length !== cols.length) {
				throw new Error(
					`cursor tuple length mismatch (expected ${cols.length}, got ${decoded.v?.length ?? 0})`,
				);
			}
			// Build the disjunctive tuple comparison as a nested group of WHEREs.
			clone.where((q) => {
				for (let i = 0; i < cols.length; i++) {
					q.orWhere((inner) => {
						for (let j = 0; j < i; j++) inner.where(cols[j], decoded.v[j]);
						inner.where(cols[i], ">", decoded.v[i]);
					});
				}
			});
		}

		clone.#orderBys = cols.map((column) => ({
			column,
			direction: "asc" as const,
		}));
		clone.#limit = lim + 1;
		// `#doExec` (not `exec`) — cursorPaginate is an atlas-specific terminal, not a
		// Lucid hook point; don't fire the generic beforeFetch/afterFetch on its clone.
		const rows = await clone.#doExec();
		const hasMore = rows.length > lim;
		const items = hasMore ? rows.slice(0, lim) : rows;
		const last = items[items.length - 1] as Record<string, unknown> | undefined;
		const nextCursor =
			hasMore && last
				? Buffer.from(JSON.stringify({ v: cols.map((c) => last[c]) })).toString(
						"base64",
					)
				: null;
		return { items, nextCursor, hasMore };
	}

	/** Thin alias for `offset((page-1)*perPage).limit(perPage)`. */
	forPage(page: number, perPage: number): this {
		const p = Math.max(1, Math.floor(page));
		const pp = Math.max(1, Math.floor(perPage));
		this.#offset = (p - 1) * pp;
		this.#limit = pp;
		return this;
	}

	// === Story 29.11 — debug / toQuery / clone ========================================================

	debug(flag = true): this {
		this.#debugFlag = flag;
		return this;
	}

	/** Returns the compiled SQL with bindings interpolated as dialect-safe literals. */
	toQuery(): string {
		const { sql, params } = this.toSQL();
		let i = 0;
		return sql.replace(/\?|\$\d+/g, () => {
			const v = params[i++];
			return this.#literalEscape(v);
		});
	}

	/** Deep clone of this query — mutations on the clone never affect the original. */
	clone(): ModelQuery<T> {
		const c = new ModelQuery<T>(
			this.#tableName,
			this.#db,
			this.#hydrateFn,
			this.#entityClass,
			this.#resolveColumn,
			this.#softDeletes,
			this.#dialect,
		);
		c.#softScope = this.#softScope;
		c.#wheres = structuredCloneSafe(this.#wheres);
		c.#orderBys = [...this.#orderBys];
		c.#select = [...this.#select];
		c.#limit = this.#limit;
		c.#offset = this.#offset;
		c.#preloads = new Map(this.#preloads);
		c.#selectSubqueries = structuredClone(this.#selectSubqueries);
		c.#joins = [...this.#joins];
		c.#lockMode = this.#lockMode;
		c.#distinct = this.#distinct;
		c.#debugFlag = this.#debugFlag;
		return c;
	}

	// === Story 30.2 — update / delete fluent ===========================================================

	/** Execute a fluent UPDATE. Returns affected rows (or rows when `returning` is set). */
	async update(
		patch: Record<string, unknown>,
		returning?: string[],
	): Promise<number | Record<string, unknown>[]> {
		if (!patch || Object.keys(patch).length === 0) {
			throw new Error("update() requires a non-empty payload");
		}
		const setPairs = Object.entries(patch).map(
			([k, v]) => [this.#resolveColumn(k), v] as [string, unknown],
		);
		const spec = {
			kind: "update",
			table: this.#tableName,
			set: setPairs,
			wheres: this.#wheresForDml(),
			returning: returning ?? [],
		};
		const compiled = compileStatementNative(spec, this.#dialect);
		if (returning && returning.length > 0) {
			return this.#db.query<Record<string, unknown>>(
				compiled.statements[0],
				compiled.params,
			);
		}
		const r = await this.#db.execute(compiled.statements[0], compiled.params);
		return r.rowsAffected ?? 0;
	}

	/** Execute a fluent DELETE. Returns affected rows (or rows when `returning` is set). */
	async delete(
		returning?: string[],
	): Promise<number | Record<string, unknown>[]> {
		const spec = {
			kind: "delete",
			table: this.#tableName,
			wheres: this.#wheresForDml(),
			returning: returning ?? [],
		};
		const compiled = compileStatementNative(spec, this.#dialect);
		if (returning && returning.length > 0) {
			return this.#db.query<Record<string, unknown>>(
				compiled.statements[0],
				compiled.params,
			);
		}
		const r = await this.#db.execute(compiled.statements[0], compiled.params);
		return r.rowsAffected ?? 0;
	}

	// === Story 30.3 — increment / decrement already implemented? check ================================

	increment(column: string, amount: number): Promise<number>;
	increment(patch: Record<string, number>): Promise<number>;
	increment(
		colOrPatch: string | Record<string, number>,
		amount = 1,
	): Promise<number> {
		return this.#runIncDec("increment", colOrPatch, amount);
	}

	decrement(column: string, amount: number): Promise<number>;
	decrement(patch: Record<string, number>): Promise<number>;
	decrement(
		colOrPatch: string | Record<string, number>,
		amount = 1,
	): Promise<number> {
		return this.#runIncDec("decrement", colOrPatch, amount);
	}

	// === Story 30.8 — forUpdate / forShare =============================================================

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

	// === Private helpers ==============================================================================

	#quote(name: string): string {
		return this.#dialect === "mysql" ? `\`${name}\`` : `"${name}"`;
	}

	/** Quote a `table.column` reference on both sides of the dot. */
	#quoteCol(ref: string): string {
		if (ref.includes(".")) {
			const [t, c] = ref.split(".", 2);
			return `${this.#quote(t)}.${this.#quote(c)}`;
		}
		return this.#quote(ref);
	}

	#pushJoin(
		kind: "INNER" | "LEFT" | "RIGHT",
		table: string,
		leftOrBuild: string | ((j: JoinBuilder) => void),
		right?: string,
	): this {
		const tq = this.#quote(table);
		if (typeof leftOrBuild === "function") {
			const jb: JoinBuilder = {
				parts: [],
				on(l: string, r: string) {
					this.parts.push({ kind: "and", left: l, right: r });
					return this;
				},
				andOn(l: string, r: string) {
					this.parts.push({ kind: "and", left: l, right: r });
					return this;
				},
				andOnVal(l: string, _v: unknown) {
					this.parts.push({ kind: "and", left: l, right: "?" });
					return this;
				},
			};
			leftOrBuild(jb);
			const on = jb.parts
				.map((p, i) => {
					const prefix = i === 0 ? "ON" : p.kind === "or" ? "OR" : "AND";
					return `${prefix} ${this.#quoteCol(p.left)} = ${p.right === "?" ? "?" : this.#quoteCol(p.right)}`;
				})
				.join(" ");
			this.#joins.push(`${kind} JOIN ${tq} ${on}`);
			return this;
		}
		if (right === undefined)
			throw new Error(
				"join() with string form requires both left and right operands",
			);
		this.#joins.push(
			`${kind} JOIN ${tq} ON ${this.#quoteCol(leftOrBuild)} = ${this.#quoteCol(right)}`,
		);
		return this;
	}

	async #runScalar(expr: string): Promise<unknown> {
		const clone = this.clone();
		clone.#select = [`${expr} AS __scalar__`];
		clone.#orderBys = [];
		const { sql, params } = clone.toSQL();
		const rows = await this.#db.query<Record<string, unknown>>(sql, params);
		const row = rows[0];
		return row ? row.__scalar__ : null;
	}

	async #runIncDec(
		op: "increment" | "decrement",
		colOrPatch: string | Record<string, number>,
		amount: number,
	): Promise<number> {
		const patch =
			typeof colOrPatch === "string" ? { [colOrPatch]: amount } : colOrPatch;
		const setPairs = Object.entries(patch).map(
			([k, v]) =>
				[this.#resolveColumn(k), { op, value: v }] as [
					string,
					{ op: string; value: number },
				],
		);
		const spec = {
			kind: "update",
			table: this.#tableName,
			set: setPairs,
			wheres: this.#wheresForDml(),
			returning: [],
		};
		const compiled = compileStatementNative(spec, this.#dialect);
		const r = await this.#db.execute(compiled.statements[0], compiled.params);
		return r.rowsAffected ?? 0;
	}

	/**
	 * Flatten the SELECT wheres to DML-compatible wheres. Standard predicates
	 * and `whereRaw` fragments pass through; `group` / `exists` / `inSub` are
	 * still rejected because the DML compiler's WHERE lowering does not yet
	 * handle nested sub-queries or correlated EXISTS.
	 */
	#wheresForDml(): Array<Record<string, unknown>> {
		const out: Array<Record<string, unknown>> = [];
		for (const w of this.#wheres) {
			if ("kind" in w) {
				if (w.kind === "raw") {
					out.push({
						kind: "raw",
						sql: w.sql,
						bindings: w.bindings,
						type: w.type,
					});
					continue;
				}
				throw new Error(
					`update/delete do not support '${w.kind}' WHERE clauses. ` +
						`Supported: plain predicates and whereRaw. Use a raw UPDATE/DELETE for complex criteria.`,
				);
			}
			out.push({
				column: w.column,
				operator: w.operator,
				value: w.value,
				type: w.type,
			});
		}
		return out;
	}

	/**
	 * !!! DEBUG ONLY — DO NOT USE FOR EXECUTION !!!
	 *
	 * Produces a human-readable SQL rendering with bindings inlined. The escape
	 * strategy (double single-quotes) is NOT safe against backslash-based injection
	 * on MySQL or on PostgreSQL with `standard_conforming_strings = off`: the
	 * sequence `\'` closes the string literal and opens an injection vector.
	 *
	 * This function exists ONLY to back `.toQuery()` for copy-paste debugging and
	 * log readability. The production execution path always goes through bound
	 * parameters via the Rust compiler — this escaper is never on the hot path.
	 * If you are tempted to feed `.toQuery()` output into `db.prepare()`, STOP.
	 */
	#literalEscape(v: unknown): string {
		if (v === null || v === undefined) return "NULL";
		if (typeof v === "number") return String(v);
		if (typeof v === "boolean") return v ? "1" : "0";
		if (v instanceof Date) return `'${v.toISOString()}'`;
		// Strings — escape single quotes per SQL. NOT injection-safe against `\'`.
		return `'${String(v).replace(/'/g, "''")}'`;
	}

	/**
	 * Build a parenthesised WHERE group from a callback. A throwaway ModelQuery
	 * on the SAME table is used as the scratch builder so the callback can call
	 * any of the usual where* methods, including nested `where(cb)` for deeper
	 * groups. We then copy its accumulated `#wheres` into a `GroupWhere` clause.
	 */
	#buildGroup(type: "and" | "or", callback: WhereCallback): GroupWhere {
		const scratch = new ModelQuery<BaseEntity>(
			this.#tableName,
			this.#db,
			(row) => row as BaseEntity,
			this.#entityClass as new () => BaseEntity,
			this.#resolveColumn,
			false,
			this.#dialect,
		);
		callback(scratch);
		return { type, kind: "group", conditions: scratch.#wheres };
	}

	#buildExistsClause(
		type: "and" | "or",
		negated: boolean,
		relationName: string,
		callback?: (q: ModelQuery<BaseEntity>) => void,
		countOp?: string,
		countThreshold?: number,
	): ExistsWhere {
		const sub = this.#makeRelationSub(relationName);
		if (callback) callback(sub);
		const spec = sub.#buildSpec();
		if (countOp !== undefined && countThreshold !== undefined) {
			spec.having = [
				{
					column: "COUNT(*)",
					operator: countOp,
					value: countThreshold,
					type: "and",
				},
			];
		}
		return { type, kind: "exists", negated, subquery: spec };
	}

	#pushWhere(
		type: "and" | "or",
		column: string,
		operatorOrValue: unknown,
		value: unknown,
	): this {
		const resolved = this.#resolveColumn(column);
		if (value === undefined) {
			// 2-arg form: where(col, value). A `null` value means the caller
			// wants an IS NULL test — `= ?` bound to null never matches in
			// SQL, silently returning zero rows. Mirror whereNull().
			if (operatorOrValue === null) {
				this.#wheres.push({
					type,
					column: resolved,
					operator: "IS NULL",
					value: null,
				});
				return this;
			}
			this.#wheres.push({
				type,
				column: resolved,
				operator: "=",
				value: operatorOrValue,
			});
		} else {
			this.#wheres.push({
				type,
				column: resolved,
				operator: operatorOrValue as string,
				value,
			});
		}
		return this;
	}

	/**
	 * Resolve this ModelQuery's preloads against a pre-loaded set of entities.
	 * Used by the nested-preload machinery to recurse without re-running the root select.
	 */
	async #resolveAgainst(
		entities: BaseEntity[],
		entityClass: new () => BaseEntity,
	): Promise<void> {
		// Temporarily swap the entity class so resolvePreloads looks up the right metadata.
		// Cast is safe because resolvePreloads only reads metadata + writes via setProp.
		const prevClass = this.#entityClass;
		this.#entityClass = entityClass as new () => T;
		try {
			await this.#resolvePreloads(entities as T[]);
		} finally {
			this.#entityClass = prevClass;
		}
	}
}
