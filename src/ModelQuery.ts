/**
 * ModelQuery — executable query builder for repositories.
 *
 * Like AdonisJS Lucid Model.query():
 *   repo.query().where('status', 'active').orderBy('created_at', 'desc').limit(10).exec()
 *
 * Builds SQL fluently and executes against the database connection.
 */

import { dateTimeAtlasAdapter } from "@c9up/chronos/atlas";
import { type BaseEntity, type DomainEvent, REPO_REF } from "./BaseEntity.js";
// Value import used only inside method bodies (preload hydration) — the
// BaseRepository ↔ ModelQuery cycle resolves at runtime, after both are defined.
import {
	assertNotPromise,
	BaseRepository,
	type DatabaseConnection,
	wrapAdapterError,
} from "./BaseRepository.js";
import {
	ensureEntityMetadata,
	getColumnMetadata,
	getDateColumnConfig,
	getPrimaryKey,
	getRelationMetadata,
	hasSoftDeletes,
	type RelationMetadata,
} from "./decorators/entity.js";
import { fireHooks } from "./decorators/hooks.js";
import { getNamingStrategy } from "./naming/NamingStrategy.js";
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

/**
 * SQL keyword tokens forbidden inside `whereExpr`'s arithmetic extra-expression.
 * They are just letters (pass the charset guard) but would let the fragment alter
 * the predicate's logical structure — whereExpr stays an arithmetic-only, SAFE
 * alternative to whereRaw. A column genuinely named after a keyword must use whereRaw.
 */
const WHEREEXPR_FORBIDDEN_WORDS = new Set<string>([
	"OR",
	"AND",
	"NOT",
	"IS",
	"NULL",
	"IN",
	"LIKE",
	"ILIKE",
	"BETWEEN",
	"EXISTS",
	"ANY",
	"ALL",
	"SOME",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
	"END",
	"SELECT",
	"FROM",
	"WHERE",
	"JOIN",
	"UNION",
	"INTERSECT",
	"EXCEPT",
	"HAVING",
	"GROUP",
	"ORDER",
	"BY",
	"LIMIT",
	"OFFSET",
	"AS",
	"DISTINCT",
	"TRUE",
	"FALSE",
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
/**
 * Lower a value bound for a given property to its DB form — mirrors
 * `BaseRepository.#applyPrepare` (a `@column.dateTime` DateTime → ISO string, a
 * `@Column({ prepare })` adapter runs). Threaded into ModelQuery so the fluent
 * `update()` / WHERE paths don't bypass prepare the way direct repo writes don't.
 */
type ValuePreparer = (column: string, value: unknown) => unknown;

/**
 * Column resolver for an ARBITRARY entity class, honouring `@Column({ columnName })`
 * and the snake_case convention. Used to build correlated/preload subqueries on a
 * RELATED model so their WHERE/join columns resolve like a direct query would.
 */
function buildColumnResolver(
	entityClass: new () => BaseEntity,
): ColumnResolver {
	const map = new Map<string, string>();
	for (const col of getColumnMetadata(entityClass)) {
		const db = col.columnName ?? camelToSnake(col.propertyKey);
		map.set(col.propertyKey, db);
		map.set(db, db);
	}
	return (col) => map.get(col) ?? camelToSnake(col);
}

/**
 * Value preparer for an ARBITRARY entity class — mirrors `BaseRepository.#applyPrepare`
 * (a `@column.dateTime` DateTime → ISO, a `@Column({ prepare })` adapter runs). So a
 * preload/whereHas constraint on a RELATED model prepares its values like a direct query.
 */
function buildValuePreparer(entityClass: new () => BaseEntity): ValuePreparer {
	const prepares = new Map<string, (v: unknown) => unknown>();
	// Reverse map (db column → property) so a caller passing a DB name or an
	// explicit `columnName` (e.g. preload/whereHas constraint on `published_at`)
	// still routes through the property-keyed prepare/date maps — mirrors
	// BaseRepository.#applyPrepare.
	const byDbName = new Map<string, string>();
	for (const col of getColumnMetadata(entityClass)) {
		if (col.prepare) prepares.set(col.propertyKey, col.prepare);
		byDbName.set(
			col.columnName ?? camelToSnake(col.propertyKey),
			col.propertyKey,
		);
	}
	const dateCols = getDateColumnConfig(entityClass);
	return (key, value) => {
		const prop = byDbName.get(key) ?? key;
		const p = prepares.get(prop);
		if (p) return p(value);
		if (dateCols[prop] && value != null) {
			if (value instanceof Date) return value.toISOString();
			return dateTimeAtlasAdapter.prepare(value);
		}
		return value;
	};
}

/** Structural (cross-realm-safe) check for a value exposing `toISO()` — a Chronos/Luxon DateTime. */
function joinValueHasToISO(v: unknown): v is { toISO(): string } {
	return (
		typeof v === "object" &&
		v !== null &&
		"toISO" in v &&
		typeof v.toISO === "function"
	);
}

/**
 * Universal type-lowering for a JOIN `onVal`/`andOnVal`/`orOnVal` bound value:
 * `Date`/`DateTime` → ISO string. Unlike the model value-preparer this applies NO
 * column-specific `@Column({ prepare })` adapter, so a FOREIGN join column can't
 * borrow the root model's adapter for a same-named column on a different table
 * (Knex binds join values model-agnostically; we add only safe universal
 * serialization so a DateTime still lowers to ISO like `where()`).
 */
function lowerJoinValue(value: unknown): unknown {
	if (value instanceof Date) return value.toISOString();
	if (joinValueHasToISO(value)) return value.toISO();
	return value;
}

/**
 * Does a join column's table reference (`ref`) denote the root model's own table
 * (`modelTable`)? The match is ASYMMETRIC: a reference may OMIT the schema the
 * model declares (default schema) — `orders` matches a `public.orders` model — but
 * it may NOT ADD qualification the model doesn't claim. So a `public.orders` model
 * accepts `orders.col`, while an unqualified `orders` model rejects
 * `archive.orders.col` (a different schema the model never named) — keeping it
 * foreign so the root model's `@Column` adapters aren't misapplied to it.
 */
function sameTableRef(ref: string, modelTable: string): boolean {
	const rs = ref.split(".");
	const ms = modelTable.split(".");
	// The reference cannot be MORE qualified than the model (it can only drop the
	// schema, never assert a new one) — otherwise treat it as a foreign table.
	if (rs.length > ms.length) return false;
	for (let i = 1; i <= rs.length; i++) {
		if (rs[rs.length - i] !== ms[ms.length - i]) return false;
	}
	return true;
}

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

/** A raw SQL HAVING fragment with `?` bindings — kind-tagged for the Rust compiler. */
interface HavingRawClause {
	kind: "raw";
	sql: string;
	bindings: unknown[];
	type: "and" | "or";
}

type HavingEntry = HavingClause | HavingRawClause;

/** A compiled CTE (`WITH name AS (...)`) — the sub-select is pre-compiled to SQL + params. */
interface CteSpec {
	name: string;
	sql: string;
	params: unknown[];
}

/** A compiled UNION / UNION ALL branch — pre-compiled to SQL + params. */
interface UnionSpec {
	sql: string;
	params: unknown[];
	all: boolean;
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
	having: HavingEntry[];
	limit: number | null;
	offset: number | null;
	distinct: boolean;
	ctes: CteSpec[];
	unions: UnionSpec[];
	/** JOIN fragments; each carries its own `?`-style bound params (e.g. `onVal`). */
	joins: Array<{ sql: string; params: unknown[] }>;
	/** Composite lock clause, e.g. `FOR UPDATE`, `FOR NO KEY UPDATE SKIP LOCKED`. */
	lockMode: string | null;
}

type WhereClause =
	| StandardWhere
	| RawWhere
	| ExistsWhere
	| GroupWhere
	| InSubWhere;

type WhereCallback = (q: ModelQuery<BaseEntity>) => void;

/**
 * Process-wide strict mode flag. When enabled, `whereRaw()`, `joinRaw()`,
 * `havingRaw()` and the repository's `raw()` throw unconditionally — forcing every
 * call site to use the typed `whereExpr()` / `joinOn()` / `having()` / structured
 * builder paths. The connection-level `db.query()` / `db.execute()` stay available
 * as the explicit, parameterised break-glass. Intended for prod hardening on apps
 * that can't audit every call site manually.
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

/** Enable or disable Atlas strict mode. When enabled, whereRaw/joinRaw/havingRaw throw in user code. */
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

/**
 * Multi-condition join builder passed to innerJoin/leftJoin/rightJoin callbacks.
 * `on`/`andOn`/`orOn` join two COLUMNS; `onVal`/`andOnVal`/`orOnVal` join a column
 * to a bound VALUE (AdonisJS/Knex parity) — the value flows through the join-params
 * channel into the compiled parameter list.
 */
interface JoinBuilder {
	/** A column-to-column part (`value` absent) or a column-to-value part (`value` set). */
	parts: Array<{
		kind: "and" | "or";
		left: string;
		right?: string;
		value?: { v: unknown };
	}>;
	on(left: string, right: string): JoinBuilder;
	andOn(left: string, right: string): JoinBuilder;
	orOn(left: string, right: string): JoinBuilder;
	onVal(left: string, value: unknown): JoinBuilder;
	andOnVal(left: string, value: unknown): JoinBuilder;
	orOnVal(left: string, value: unknown): JoinBuilder;
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
	#metaKeys?: Record<string, string>;

	constructor(
		items: T[],
		base: { total: number; perPage: number; currentPage: number },
		metaKeys?: Record<string, string>,
	) {
		this.items = items;
		const lastPage = Math.max(1, Math.ceil(base.total / base.perPage));
		this.meta = { ...base, lastPage, firstPage: 1 };
		this.#metaKeys = metaKeys;
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

	/**
	 * Build the URL for a page number, honouring `baseUrl` + `queryString`.
	 * Returns `''` when no `baseUrl` was set (AdonisJS `getUrl`).
	 */
	getUrl(page: number): string {
		if (!this.#baseUrl) return "";
		const params = new URLSearchParams();
		for (const [k, v] of Object.entries(this.#queryString))
			params.set(k, String(v));
		params.set("page", String(page));
		return `${this.#baseUrl}?${params.toString()}`;
	}

	/** URL of the next page, or `null` when on the last page (AdonisJS `getNextPageUrl`). */
	getNextPageUrl(): string | null {
		return this.hasMorePages ? this.getUrl(this.meta.currentPage + 1) : null;
	}

	/** URL of the previous page, or `null` when on the first page (AdonisJS `getPreviousPageUrl`). */
	getPreviousPageUrl(): string | null {
		return this.meta.currentPage > 1
			? this.getUrl(this.meta.currentPage - 1)
			: null;
	}

	/** URLs for an inclusive page range, clamped to `[1, lastPage]` (AdonisJS `getUrlsForRange`). */
	getUrlsForRange(
		start: number,
		end: number,
	): Array<{ page: number; url: string; isActive: boolean }> {
		const lo = Math.max(1, start);
		const hi = Math.min(this.meta.lastPage, end);
		const range: Array<{ page: number; url: string; isActive: boolean }> = [];
		for (let page = lo; page <= hi; page++)
			range.push({
				page,
				url: this.getUrl(page),
				isActive: page === this.meta.currentPage,
			});
		return range;
	}

	toJSON(): {
		data: unknown[];
		meta: Record<string, unknown>;
	} {
		const raw: Record<string, unknown> = { ...this.meta };
		if (this.#baseUrl) {
			raw.firstPageUrl = this.getUrl(1);
			raw.lastPageUrl = this.getUrl(this.meta.lastPage);
			const next = this.getNextPageUrl();
			const prev = this.getPreviousPageUrl();
			if (next) raw.nextPageUrl = next;
			if (prev) raw.previousPageUrl = prev;
		}
		// Remap meta key names when the naming strategy customizes them
		// (AdonisJS `paginationMetaKeys`); unknown keys keep their default name.
		const keys = this.#metaKeys;
		if (!keys) return { data: this.items as unknown[], meta: raw };
		const meta: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(raw)) meta[keys[k] ?? k] = v;
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
	#joins: Array<{ sql: string; params: unknown[] }> = [];
	/** Row lock base mode — Story 30.8. */
	#lockMode:
		| "FOR UPDATE"
		| "FOR SHARE"
		| "FOR NO KEY UPDATE"
		| "FOR KEY SHARE"
		| null = null;
	/** Optional lock modifier (SKIP LOCKED / NOWAIT), composed onto {@link #lockMode}. */
	#lockModifier: "SKIP LOCKED" | "NOWAIT" | null = null;
	/** Context threaded onto every hydrated instance's `$sideloaded` — AdonisJS `sideload`. */
	#sideloaded: Record<string, unknown> | null = null;
	/** Per-query debug flag — Story 29.11. */
	#debugFlag = false;
	/** Distinct flag — Story 29.5. */
	#distinct = false;
	/** GROUP BY columns (Lucid parity). */
	#groupBy: string[] = [];
	/** HAVING clauses — structured + raw (Lucid parity). */
	#having: HavingEntry[] = [];
	/** CTEs registered via `.with()` (Lucid parity). */
	#ctes: Array<{ name: string; query: ModelQuery<BaseEntity> }> = [];
	/** UNION / UNION ALL branches (Lucid parity). */
	#unions: Array<{ query: ModelQuery<BaseEntity>; all: boolean }> = [];
	/** m2m pivot-table WHERE constraints — applied to the pivot lookup, not the related query. */
	#pivotWheres: Array<{
		column: string;
		operator: string;
		value: unknown;
		/** AND/OR within the parenthesised pivot-filter group — see `#runInQuery`. */
		type: "and" | "or";
	}> = [];
	/**
	 * Deferred builder for a lazy m2m `related().query()` EXISTS predicate. Set by
	 * the relation proxy's scoped query; invoked at `#buildSpec()` time with the
	 * CURRENT `#pivotWheres` so `.wherePivot()` calls added AFTER the proxy handed
	 * back the query still fold into the pivot EXISTS (a flat `whereRaw` at proxy
	 * time would freeze the predicate before those calls and silently drop them).
	 */
	#pivotExists?: (
		pivotWheres: ReadonlyArray<{
			column: string;
			operator: string;
			value: unknown;
		}>,
	) => { sql: string; bindings: unknown[] };
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
		prepareValue: ValuePreparer = (_c, v) => v,
		onDomainEvents?: (events: DomainEvent[]) => Promise<void>,
	) {
		this.#tableName = tableName;
		this.#db = db;
		this.#hydrateFn = hydrateFn;
		this.#entityClass = entityClass;
		this.#resolveColumn = resolveColumn;
		this.#softDeletes = softDeletes;
		this.#dialect = dialect;
		this.#prepareValue = prepareValue;
		this.#onDomainEvents = onDomainEvents;
	}

	/** @see ValuePreparer — identity unless the owning repository wires prepare in. */
	#prepareValue: ValuePreparer;
	/** Domain-event bus threaded from the owning repository — propagated to preload repos. */
	#onDomainEvents?: (events: DomainEvent[]) => Promise<void>;

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
		const list = Array.isArray(columns)
			? columns
			: columns.split(",").map((c) => c.trim());
		this.#select = list.map((c) => this.#resolveSelect(c));
		return this;
	}

	/**
	 * Resolve a bare model-property select/returning target to its DB column
	 * (honouring `@Column({ columnName })`), leaving expressions / aliases /
	 * qualified names / `*` untouched. A bare identifier IS validated through the
	 * column resolver — so a typo like `select('lable')` raises the same Atlas
	 * error as `where`/`orderBy`, rather than reaching the DB.
	 */
	#resolveSelect(col: string): string {
		if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(col)) return this.#resolveColumn(col);
		// `col as alias` — resolve the (bare) column part to its DB name, keep the
		// alias verbatim, so `select('label as name')` honours a columnName override.
		const aliased = col.match(
			/^([A-Za-z_][A-Za-z0-9_]*)\s+as\s+([A-Za-z_][A-Za-z0-9_]*)$/i,
		);
		if (aliased) return `${this.#resolveColumn(aliased[1])} AS ${aliased[2]}`;
		return col;
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

	// ─── AND aliases ──────────────────────────────────────────
	//
	// Lucid documents an `and*` spelling alongside every `where*`. They are
	// exact synonyms — the base methods already default to AND — and exist so a
	// chain can say so out loud: `.where(a).andWhere(b)`. Kept as thin
	// delegations rather than duplicated bodies, so they cannot drift.

	andWhere(callback: WhereCallback): this;
	andWhere(column: string, value: unknown): this;
	andWhere(column: string, operator: string, value: unknown): this;
	andWhere(
		columnOrCb: string | WhereCallback,
		operatorOrValue?: unknown,
		value?: unknown,
	): this {
		// The 2-arg overload must not forward a phantom third argument: `where`
		// switches on `value === undefined` to tell `(col, value)` from
		// `(col, operator, value)`.
		return typeof columnOrCb === "function"
			? this.where(columnOrCb)
			: value === undefined
				? this.where(columnOrCb, operatorOrValue)
				: this.where(columnOrCb, operatorOrValue as string, value);
	}

	/** Alias of {@link whereNot} (Lucid parity). */
	andWhereNot(column: string, value: unknown): this {
		return this.whereNot(column, value);
	}

	/** Alias of {@link whereIn} (Lucid parity). */
	andWhereIn(column: string, values: readonly unknown[]): this {
		return this.whereIn(column, values);
	}

	/** Alias of {@link whereNotIn} (Lucid parity). */
	andWhereNotIn(column: string, values: readonly unknown[]): this {
		return this.whereNotIn(column, values);
	}

	/** Alias of {@link whereNull} (Lucid parity). */
	andWhereNull(column: string): this {
		return this.whereNull(column);
	}

	/** Alias of {@link whereNotNull} (Lucid parity). */
	andWhereNotNull(column: string): this {
		return this.whereNotNull(column);
	}

	/** Alias of {@link whereBetween} (Lucid parity). */
	andWhereBetween(column: string, range: readonly [unknown, unknown]): this {
		return this.whereBetween(column, range);
	}

	/** Alias of {@link whereNotBetween} (Lucid parity). */
	andWhereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		return this.whereNotBetween(column, range);
	}

	/** Alias of {@link whereLike} (Lucid parity). */
	andWhereLike(column: string, pattern: string): this {
		return this.whereLike(column, pattern);
	}

	/** Alias of {@link whereILike} (Lucid parity). */
	andWhereILike(column: string, pattern: string): this {
		return this.whereILike(column, pattern);
	}

	/** Alias of {@link whereColumn} (Lucid parity). */
	andWhereColumn(left: string, operator: string, right: string): this {
		return this.whereColumn(left, operator, right);
	}

	/** `WHERE col != ?` — negation of `where`. */
	whereNot(column: string, value: unknown): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "!=",
			value: this.#prep(column, value),
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
			value: this.#prep(column, [...source]),
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
			value: this.#prep(column, [...source]),
		});
		return this;
	}

	/** `WHERE col BETWEEN ? AND ?` — inclusive range. */
	whereBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "BETWEEN",
			value: this.#prep(column, [...range]),
		});
		return this;
	}

	/** `WHERE col NOT BETWEEN ? AND ?` */
	whereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({
			type: "and",
			column: this.#resolveColumn(column),
			operator: "NOT BETWEEN",
			value: this.#prep(column, [...range]),
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
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "IS NULL",
			value: null,
		});
		return this;
	}

	/** `OR col IS NOT NULL`. */
	orWhereNotNull(column: string): this {
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "IS NOT NULL",
			value: null,
		});
		return this;
	}

	/** `OR col != ?`. */
	orWhereNot(column: string, value: unknown): this {
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "!=",
			value: this.#prep(column, value),
		});
		return this;
	}

	/** `OR col IN (...)` — array or `ModelQuery` subquery source. */
	orWhereIn(
		column: string,
		source: readonly unknown[] | ModelQuery<BaseEntity>,
	): this {
		if (source instanceof ModelQuery) {
			this.#wheres.push({
				type: "or",
				kind: "inSub",
				negated: false,
				column: this.#resolveColumn(column),
				subquery: source.#buildSpec(),
			});
			return this;
		}
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "IN",
			value: this.#prep(column, [...source]),
		});
		return this;
	}

	/** `OR col NOT IN (...)` — array or `ModelQuery` subquery source. */
	orWhereNotIn(
		column: string,
		source: readonly unknown[] | ModelQuery<BaseEntity>,
	): this {
		if (source instanceof ModelQuery) {
			this.#wheres.push({
				type: "or",
				kind: "inSub",
				negated: true,
				column: this.#resolveColumn(column),
				subquery: source.#buildSpec(),
			});
			return this;
		}
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "NOT IN",
			value: this.#prep(column, [...source]),
		});
		return this;
	}

	/** `OR col BETWEEN ? AND ?`. */
	orWhereBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "BETWEEN",
			value: this.#prep(column, [...range]),
		});
		return this;
	}

	/** `OR col NOT BETWEEN ? AND ?`. */
	orWhereNotBetween(column: string, range: readonly [unknown, unknown]): this {
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "NOT BETWEEN",
			value: this.#prep(column, [...range]),
		});
		return this;
	}

	/** `OR col LIKE ?`. */
	orWhereLike(column: string, pattern: string): this {
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "LIKE",
			value: pattern,
		});
		return this;
	}

	/** `OR col ILIKE ?` (rewritten to LOWER() LIKE LOWER() on sqlite/mysql). */
	orWhereILike(column: string, pattern: string): this {
		this.#wheres.push({
			type: "or",
			column: this.#resolveColumn(column),
			operator: "ILIKE",
			value: pattern,
		});
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
	#pushWhereRaw(
		sql: string,
		bindings: readonly unknown[] = [],
		type: "and" | "or" = "and",
	): this {
		this.#wheres.push({
			type,
			kind: "raw",
			sql,
			bindings: [...bindings],
		});
		return this;
	}

	/** Alias of {@link whereRaw} (Lucid parity). Subject to the same strict-mode gate. */
	andWhereRaw(sql: string, bindings: readonly unknown[] = []): this {
		return this.whereRaw(sql, bindings);
	}

	/**
	 * `OR <raw fragment>` (Lucid parity).
	 *
	 * @unsafe Raw SQL fragment — never concatenate user input into `sql`.
	 * Subject to the same strict-mode gate as {@link whereRaw}.
	 */
	orWhereRaw(sql: string, bindings: readonly unknown[] = []): this {
		this.#assertRawAllowed("orWhereRaw");
		return this.#pushWhereRaw(sql, bindings, "or");
	}

	/** Shared strict-mode gate for the raw WHERE entry points. */
	#assertRawAllowed(method: string): void {
		if (isAtlasStrictMode() && !isInternalBypass()) {
			throw new Error(
				`${method}() is disabled in Atlas strict mode. ` +
					"Use whereExpr() or a structured builder method instead. " +
					"Call setAtlasStrictMode(false) at bootstrap if you truly need raw SQL.",
			);
		}
	}

	/**
	 * Framework-internal: register the deferred m2m EXISTS predicate for a lazy
	 * `related().query()`. The builder is re-invoked on every `#buildSpec()` with
	 * the pivot constraints known at that moment, so `.wherePivot()` added after
	 * the proxy returned still applies. Not exported from the barrel.
	 */
	setPivotExistsBuilder(
		builder: (
			pivotWheres: ReadonlyArray<{
				column: string;
				operator: string;
				value: unknown;
			}>,
		) => { sql: string; bindings: unknown[] },
	): this {
		this.#pivotExists = builder;
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
			// The charset blocks comparison/quote symbols, but bare SQL keywords
			// (OR / AND / IS / NOT / SELECT …) are just letters and would slip
			// through, letting `extra` alter the predicate's logical structure
			// (e.g. `whereExpr('total', 'OR active', '>', 0)`). whereExpr is the
			// SAFE arithmetic alternative to whereRaw, so reject any SQL keyword
			// token — arithmetic on columns/numbers/functions only.
			for (const word of extra.match(/[A-Za-z_][A-Za-z0-9_]*/g) ?? []) {
				if (WHEREEXPR_FORBIDDEN_WORDS.has(word.toUpperCase())) {
					throw new Error(
						`whereExpr: extraExpression '${extra}' contains the SQL keyword '${word}'. ` +
							"whereExpr allows arithmetic expressions only (columns, numbers, + - * / , functions). Use whereRaw() for logical/SQL constructs.",
					);
				}
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
			return this.#pushWhereRaw(`${q} ${extra} ${op} ?`, [
				this.#prep(column, value),
			]);
		}
		this.#wheres.push({
			type: "and",
			column: resolved,
			operator: op,
			value: this.#prep(column, value),
		});
		return this;
	}

	/**
	 * Compare two COLUMNS (AdonisJS/Knex `whereColumn`) — `WHERE "a" op "b"`.
	 * Both sides go through the identifier quoter (injection-safe) and the
	 * operator is allow-listed; nothing is bound (it's a column reference, not a
	 * value), which the standard `where`/`whereExpr` value-binding path can't do.
	 */
	// ─── EXISTS ───────────────────────────────────────────────
	//
	// `whereExists` lived only on the low-level `query/QueryBuilder`, not on the
	// builder `repo.query()` actually hands back, so it was unreachable from
	// normal use. The subquery is another `ModelQuery`; correlate it to the
	// outer table with `whereColumn`:
	//
	//     userRepo.query().whereExists(
	//       postRepo.query().whereColumn('posts.user_id', '=', 'users.id')
	//     )
	//
	// For relation-shaped EXISTS, prefer `whereHas`/`has`, which derive the
	// join predicate from the relation metadata.

	/** `WHERE EXISTS (subquery)` (Lucid parity). */
	whereExists(subquery: ModelQuery<BaseEntity>): this {
		return this.#pushExists("and", false, subquery);
	}

	/** Alias of {@link whereExists} (Lucid parity). */
	andWhereExists(subquery: ModelQuery<BaseEntity>): this {
		return this.#pushExists("and", false, subquery);
	}

	/** `OR EXISTS (subquery)` (Lucid parity). */
	orWhereExists(subquery: ModelQuery<BaseEntity>): this {
		return this.#pushExists("or", false, subquery);
	}

	/** `WHERE NOT EXISTS (subquery)` (Lucid parity). */
	whereNotExists(subquery: ModelQuery<BaseEntity>): this {
		return this.#pushExists("and", true, subquery);
	}

	/** Alias of {@link whereNotExists} (Lucid parity). */
	andWhereNotExists(subquery: ModelQuery<BaseEntity>): this {
		return this.#pushExists("and", true, subquery);
	}

	/** `OR NOT EXISTS (subquery)` (Lucid parity). */
	orWhereNotExists(subquery: ModelQuery<BaseEntity>): this {
		return this.#pushExists("or", true, subquery);
	}

	#pushExists(
		type: "and" | "or",
		negated: boolean,
		subquery: ModelQuery<BaseEntity>,
	): this {
		// `#buildSpec` is private, but private access is per-class, not per
		// instance: another ModelQuery's spec is reachable from here.
		this.#wheres.push({
			type,
			kind: "exists",
			negated,
			subquery: subquery.#buildSpec(),
		});
		return this;
	}

	whereColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("and", left, operator, right);
	}

	/** `OR`-combined {@link whereColumn}. */
	orWhereColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("or", left, operator, right);
	}

	/** `WHERE NOT (left <op> right)` — negation of {@link whereColumn} (Lucid parity). */
	whereNotColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("and", left, operator, right, true);
	}

	/** Alias of {@link whereNotColumn} (Lucid parity). */
	andWhereNotColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("and", left, operator, right, true);
	}

	/** `OR NOT (left <op> right)` (Lucid parity). */
	orWhereNotColumn(left: string, operator: string, right: string): this {
		return this.#whereColumn("or", left, operator, right, true);
	}

	#whereColumn(
		type: "and" | "or",
		left: string,
		operator: string,
		right: string,
		negated = false,
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
			const resolved = this.#resolveColumnReference(name);
			if (
				!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(resolved)
			) {
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
		const predicate = `${safe(left)} ${operator} ${safe(right)}`;
		// Both operands are already validated identifiers and the operator is
		// allow-listed, so wrapping in NOT(...) adds no new surface.
		const sql = negated ? `NOT (${predicate})` : predicate;
		this.#wheres.push({ type, kind: "raw", sql, bindings: [] });
		return this;
	}

	/**
	 * Resolve a column reference that may legitimately point at a table other
	 * than this query's own.
	 *
	 * `#resolveColumn` only knows the entity's own columns, so it rejects
	 * anything qualified. That is right for a value predicate, but wrong for a
	 * column-to-column one: a correlated subquery
	 * (`whereExists(post.query().whereColumn('posts.user_id', '=', 'users.id'))`)
	 * and a joined query both have to name another table, and atlas cannot know
	 * that table's columns. So: an unqualified name resolves as usual (typos
	 * still get the helpful error), and a `table.column` naming a different
	 * table passes through — validated against the identifier charset here and
	 * quoted segment by segment by the caller, never interpolated loose. A typo
	 * in that case surfaces as a database error rather than an atlas one, which
	 * is the unavoidable cost of referencing a table we have no metadata for.
	 */
	#resolveColumnReference(name: string): string {
		const qualified =
			/^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$/.exec(name);
		if (!qualified) return this.#resolveColumn(name);

		const [, table, column] = qualified;
		// Our own table: resolve the column half so `@Column({ columnName })` and
		// the camel→snake convention still apply.
		if (table === this.#tableName) {
			return `${table}.${this.#resolveColumn(column as string)}`;
		}
		// Another table in scope (outer query or JOIN). Charset-checked by the
		// regex above and quoted segment by segment by the caller — strict mode
		// does not apply, since its concern is unvalidated SQL reaching the
		// compiler and this identifier is validated.
		return `${table}.${column}`;
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

	/** Alias of {@link whereHas} (Lucid parity) — `whereHas` is already AND. */
	andWhereHas(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		return this.whereHas(relationName, callback);
	}

	/** Alias of {@link whereDoesntHave} (Lucid parity). */
	andWhereDoesntHave(
		relationName: string,
		callback?: (query: ModelQuery<BaseEntity>) => void,
	): this {
		return this.whereDoesntHave(relationName, callback);
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

	/** `OR NOT EXISTS (...)` — the OR form of {@link doesntHave} (Lucid parity). */
	orDoesntHave(relationName: string): this {
		this.#wheres.push(this.#buildExistsClause("or", true, relationName));
		return this;
	}

	/** Alias of {@link has} (Lucid parity) — `has` is already AND. */
	andHas(
		relationName: string,
		countOp?: string,
		countThreshold?: number,
	): this {
		return this.has(relationName, countOp, countThreshold);
	}

	/** Alias of {@link doesntHave} (Lucid parity). */
	andDoesntHave(relationName: string): this {
		return this.doesntHave(relationName);
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
			column === "*"
				? "COUNT(*)"
				: `COUNT(${this.#quoteCol(this.#resolveColumn(column))})`;
		return Number((await this.#runScalar(expr)) ?? 0);
	}

	async sum(column: string): Promise<number | null> {
		const v = await this.#runScalar(
			`SUM(${this.#quoteCol(this.#resolveColumn(column))})`,
		);
		return v === null || v === undefined ? null : Number(v);
	}

	async avg(column: string): Promise<number | null> {
		const v = await this.#runScalar(
			`AVG(${this.#quoteCol(this.#resolveColumn(column))})`,
		);
		return v === null || v === undefined ? null : Number(v);
	}

	async min(column: string): Promise<number | null> {
		const v = await this.#runScalar(
			`MIN(${this.#quoteCol(this.#resolveColumn(column))})`,
		);
		return v === null || v === undefined ? null : Number(v);
	}

	async max(column: string): Promise<number | null> {
		const v = await this.#runScalar(
			`MAX(${this.#quoteCol(this.#resolveColumn(column))})`,
		);
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

	/**
	 * `GROUP BY col1, col2, …` (AdonisJS/Lucid `groupBy`). Columns are resolved
	 * through the entity's column map (camelCase → snake_case) like `orderBy`.
	 * For a raw grouping expression, use a `whereRaw`-style construct via the
	 * fluent {@link QueryBuilder}.
	 */
	groupBy(...columns: string[]): this {
		for (const c of columns) this.#groupBy.push(this.#resolveColumn(c));
		return this;
	}

	/**
	 * `HAVING <col> <op> ?` — applied after `groupBy` (AdonisJS/Lucid `having`).
	 * A bare model property is resolved through the entity column map (honouring
	 * `@Column({ columnName })`) via {@link #resolveHavingCol}; an aggregate
	 * expression (`COUNT(*)`, `SUM(col)`, …) or a result alias is left verbatim so
	 * `having` can still reference `withCount`/`withAggregate` aliases.
	 */
	having(column: string, operator: string, value: unknown): this {
		this.#having.push({
			column: this.#resolveHavingCol(column),
			operator,
			value: this.#prep(column, value),
			type: "and",
		});
		return this;
	}

	/**
	 * Resolve a HAVING column: a bare model property maps to its DB column
	 * (honouring `@Column({ columnName })`), but an aggregate expression
	 * (`COUNT(*)`), a result alias, or any unknown bare identifier is left verbatim
	 * so `having` can still reference `withCount`/`withAggregate` aliases.
	 */
	#resolveHavingCol(column: string): string {
		if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(column)) return column;
		try {
			return this.#resolveColumn(column);
		} catch {
			return column;
		}
	}

	/** `OR HAVING <col> <op> ?` — OR-combined {@link having}. */
	orHaving(column: string, operator: string, value: unknown): this {
		this.#having.push({
			column: this.#resolveHavingCol(column),
			operator,
			value: this.#prep(column, value),
			type: "or",
		});
		return this;
	}

	/**
	 * **⚠ UNSAFE** — append a raw SQL `HAVING` fragment with `?` bindings
	 * (AdonisJS/Lucid `havingRaw`). The Rust compiler re-indexes the placeholders;
	 * everything else in `sql` is trusted verbatim. All values must go through
	 * `bindings`.
	 *
	 * @unsafe Raw SQL fragment — never concatenate user input into `sql`.
	 */
	havingRaw(sql: string, bindings: readonly unknown[] = []): this {
		// Same strict-mode gate as whereRaw()/joinRaw() — havingRaw is a raw-SQL
		// surface, so prod hardening must be able to neutralise it too.
		if (isAtlasStrictMode() && !isInternalBypass()) {
			throw new Error(
				"havingRaw() is disabled in Atlas strict mode. " +
					"Use having(column, operator, value) instead.",
			);
		}
		this.#having.push({
			kind: "raw",
			sql,
			bindings: [...bindings],
			type: "and",
		});
		return this;
	}

	/**
	 * `UNION (<query>)` (AdonisJS/Lucid `union`). The other query is compiled and
	 * appended as a parenthesised UNION branch; its bindings are re-indexed into
	 * the outer parameter list.
	 */
	union(query: ModelQuery<BaseEntity>): this {
		this.#unions.push({ query, all: false });
		return this;
	}

	/** `UNION ALL (<query>)` — duplicate-preserving {@link union}. */
	unionAll(query: ModelQuery<BaseEntity>): this {
		this.#unions.push({ query, all: true });
		return this;
	}

	/**
	 * `WITH <name> AS (<query>)` — register a Common Table Expression
	 * (AdonisJS/Lucid `with`). The CTE name is validated as an identifier; the
	 * sub-query is compiled and its bindings are re-indexed into the outer list.
	 */
	with(name: string, query: ModelQuery<BaseEntity>): this {
		if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
			throw new Error(`with(): CTE name '${name}' is not a valid identifier`);
		}
		this.#ctes.push({ name, query });
		return this;
	}

	/**
	 * `@ManyToMany` only — filter loaded relations by a PIVOT-table column
	 * (AdonisJS/Lucid `wherePivot`). Recorded separately from the related-table
	 * WHEREs and applied to the pivot lookup query by the m2m preload resolver;
	 * inert on non-m2m relations.
	 *
	 *     userRepo.query().preload('roles', q => q.wherePivot('active', true))
	 */
	wherePivot(column: string, value: unknown): this;
	wherePivot(column: string, operator: string, value: unknown): this;
	wherePivot(column: string, operatorOrValue: unknown, value?: unknown): this {
		return this.#pushPivot("and", column, operatorOrValue, value);
	}

	/** Alias of {@link wherePivot} (Lucid parity) — pivot filters already AND together. */
	andWherePivot(column: string, value: unknown): this;
	andWherePivot(column: string, operator: string, value: unknown): this;
	andWherePivot(
		column: string,
		operatorOrValue: unknown,
		value?: unknown,
	): this {
		return this.#pushPivot("and", column, operatorOrValue, value);
	}

	/**
	 * `@ManyToMany` only — OR form of {@link wherePivot} (Lucid parity).
	 *
	 * The pivot filters are compiled as a parenthesised group, so an OR joins
	 * the other pivot filters and cannot escape the `pivot_fk IN (parents)`
	 * scoping that makes the preload correct.
	 */
	orWherePivot(column: string, value: unknown): this;
	orWherePivot(column: string, operator: string, value: unknown): this;
	orWherePivot(
		column: string,
		operatorOrValue: unknown,
		value?: unknown,
	): this {
		return this.#pushPivot("or", column, operatorOrValue, value);
	}

	/** `@ManyToMany` only — `WHERE <pivotCol> IN (...)` on the pivot table (AdonisJS Lucid `whereInPivot`). */
	whereInPivot(column: string, values: readonly unknown[]): this {
		return this.#pushPivotOp("and", column, "IN", [...values]);
	}

	/** Alias of {@link whereInPivot} (Lucid parity). */
	andWhereInPivot(column: string, values: readonly unknown[]): this {
		return this.#pushPivotOp("and", column, "IN", [...values]);
	}

	/** `@ManyToMany` only — OR form of {@link whereInPivot} (Lucid parity). */
	orWhereInPivot(column: string, values: readonly unknown[]): this {
		return this.#pushPivotOp("or", column, "IN", [...values]);
	}

	/** Alias of {@link whereInPivot} kept for the earlier atlas name. */
	wherePivotIn(column: string, values: readonly unknown[]): this {
		return this.whereInPivot(column, values);
	}

	/** `@ManyToMany` only — `WHERE <pivotCol> != <value>` on the pivot table (AdonisJS Lucid `whereNotPivot`). */
	whereNotPivot(column: string, value: unknown): this {
		return this.#pushPivotOp("and", column, "!=", value);
	}

	/** Alias of {@link whereNotPivot} (Lucid parity). */
	andWhereNotPivot(column: string, value: unknown): this {
		return this.#pushPivotOp("and", column, "!=", value);
	}

	/** `@ManyToMany` only — OR form of {@link whereNotPivot} (Lucid parity). */
	orWhereNotPivot(column: string, value: unknown): this {
		return this.#pushPivotOp("or", column, "!=", value);
	}

	/** `@ManyToMany` only — `WHERE <pivotCol> NOT IN (...)` on the pivot table (AdonisJS Lucid `whereNotInPivot`). */
	whereNotInPivot(column: string, values: readonly unknown[]): this {
		return this.#pushPivotOp("and", column, "NOT IN", [...values]);
	}

	/** Alias of {@link whereNotInPivot} (Lucid parity). */
	andWhereNotInPivot(column: string, values: readonly unknown[]): this {
		return this.#pushPivotOp("and", column, "NOT IN", [...values]);
	}

	/** `@ManyToMany` only — OR form of {@link whereNotInPivot} (Lucid parity). */
	orWhereNotInPivot(column: string, values: readonly unknown[]): this {
		return this.#pushPivotOp("or", column, "NOT IN", [...values]);
	}

	/** `@ManyToMany` only — `WHERE <pivotCol> IS NULL` on the pivot table (Lucid `whereNullPivot`). */
	whereNullPivot(column: string): this {
		return this.#pushPivotOp("and", column, "IS NULL", null);
	}

	/** Alias of {@link whereNullPivot} (Lucid parity). */
	andWhereNullPivot(column: string): this {
		return this.#pushPivotOp("and", column, "IS NULL", null);
	}

	/** `@ManyToMany` only — OR form of {@link whereNullPivot} (Lucid parity). */
	orWhereNullPivot(column: string): this {
		return this.#pushPivotOp("or", column, "IS NULL", null);
	}

	/** `@ManyToMany` only — `WHERE <pivotCol> IS NOT NULL` on the pivot table (Lucid `whereNotNullPivot`). */
	whereNotNullPivot(column: string): this {
		return this.#pushPivotOp("and", column, "IS NOT NULL", null);
	}

	/** Alias of {@link whereNotNullPivot} (Lucid parity). */
	andWhereNotNullPivot(column: string): this {
		return this.#pushPivotOp("and", column, "IS NOT NULL", null);
	}

	/** `@ManyToMany` only — OR form of {@link whereNotNullPivot} (Lucid parity). */
	orWhereNotNullPivot(column: string): this {
		return this.#pushPivotOp("or", column, "IS NOT NULL", null);
	}

	/** Shared `(column, value)` / `(column, operator, value)` overload split for the pivot filters. */
	#pushPivot(
		type: "and" | "or",
		column: string,
		operatorOrValue: unknown,
		value?: unknown,
	): this {
		return value === undefined
			? this.#pushPivotOp(type, column, "=", operatorOrValue)
			: this.#pushPivotOp(type, column, operatorOrValue as string, value);
	}

	#pushPivotOp(
		type: "and" | "or",
		column: string,
		operator: string,
		value: unknown,
	): this {
		this.#pivotWheres.push({ column, operator, value, type });
		return this;
	}

	/** Read-only accessor for pivot constraints — consumed by the m2m preload resolver. */
	get pivotConstraints(): ReadonlyArray<{
		column: string;
		operator: string;
		value: unknown;
		type: "and" | "or";
	}> {
		return this.#pivotWheres;
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
	/**
	 * DB column backing the soft-delete `deletedAt` property — honours a
	 * `@Column({ columnName })` override, read straight from the entity metadata
	 * (not the resolver callback, which is identity for subqueries/preloads).
	 */
	#deletedAtColumn(): string {
		const col = this.#entityClass
			? getColumnMetadata(this.#entityClass).find(
					(c) => c.propertyKey === "deletedAt",
				)
			: undefined;
		return col?.columnName ?? "deleted_at";
	}

	#buildSpec(): SelectSpec {
		// `SKIP LOCKED` / `NOWAIT` are meaningless without a base row lock — and the
		// compiler emits the lock clause only when a base mode is set, so a lone
		// modifier would be a SILENT no-op (dangerous for job-queue polling that
		// believes it skips locked rows). Fail loud instead. Order-independent: this
		// fires whether the modifier was chained before or after the base lock.
		if (this.#lockModifier && !this.#lockMode) {
			throw new Error(
				`${this.#lockModifier} requires a base row lock — call forUpdate()/forShare()/forNoKeyUpdate()/forKeyShare() as well (a modifier alone emits no lock at all).`,
			);
		}
		// With a JOIN and the default `SELECT *`, scope the projection to the base
		// table's declared columns so joined columns can't clobber the model's fields
		// (e.g. `users.id` overwriting `orders.id`) and corrupt the hydrated entity —
		// AdonisJS/Lucid selects the model's own columns. Explicit `select()` wins.
		let selectCols = this.#select;
		if (
			this.#joins.length > 0 &&
			this.#select.length === 1 &&
			this.#select[0] === "*"
		) {
			const cols = getColumnMetadata(this.#entityClass).map(
				(c) =>
					`${this.#tableName}.${c.columnName ?? camelToSnake(c.propertyKey)}`,
			);
			if (cols.length > 0) selectCols = cols;
		} else if (
			!(selectCols.length === 1 && selectCols[0] === "*") &&
			selectCols.every((c) => /^[A-Za-z_][A-Za-z0-9_.]*$/.test(c))
		) {
			// A partial `select()` of PLAIN columns that omits the primary key would
			// hydrate a persisted entity with no PK — a later save() would then INSERT
			// instead of UPDATE (double-write / unique violation / spurious
			// beforeCreate). Auto-include the (base-table-qualified) PK so model
			// entities stay saveable. Aggregate/alias/expression selects are left
			// untouched — use `.pojo()` for those.
			const pkProp = getPrimaryKey(this.#entityClass);
			if (pkProp) {
				const pkCol =
					getColumnMetadata(this.#entityClass).find(
						(c) => c.propertyKey === pkProp,
					)?.columnName ?? camelToSnake(pkProp);
				// The PK counts as present ONLY as the bare column or the BASE-table-
				// qualified column. A joined `other.id` must NOT satisfy it (its leaf
				// collides with the PK name but it's a different table's row) — otherwise
				// we'd skip adding `base.id` and hydrate the wrong PK, corrupting a later
				// save(). Appended last, `base.id` also wins the duplicate result key
				// (rows collect in column order, last-wins) so the base row's PK hydrates.
				const baseQualifiedPk = `${this.#tableName}.${pkCol}`;
				if (!selectCols.some((c) => c === pkCol || c === baseQualifiedPk)) {
					selectCols = [...selectCols, baseQualifiedPk];
				}
			}
		}
		const wheres: WhereClause[] = [...this.#wheres];
		// Lazy m2m `related().query()`: emit the pivot EXISTS now, folding in any
		// `.wherePivot()` recorded since the proxy handed back this query (pushed to
		// the LOCAL copy so repeated #buildSpec calls — count, subquery — don't stack).
		if (this.#pivotExists) {
			const { sql, bindings } = this.#pivotExists(this.#pivotWheres);
			wheres.push({ type: "and", kind: "raw", sql, bindings: [...bindings] });
		}
		// Auto-apply soft-delete scope when the entity opts in via @SoftDeletes.
		// Resolve `deletedAt` through the column resolver so a `@Column({ columnName })`
		// override on the soft-delete column is honoured on the read side too — matching
		// the write side (delete/restore go through #dbColumn).
		if (this.#softDeletes) {
			const deletedAtCol = this.#deletedAtColumn();
			if (this.#softScope === "default") {
				wheres.push({
					type: "and",
					column: deletedAtCol,
					operator: "IS NULL",
					value: null,
				});
			} else if (this.#softScope === "only-trashed") {
				wheres.push({
					type: "and",
					column: deletedAtCol,
					operator: "IS NOT NULL",
					value: null,
				});
			}
			// 'with-trashed' adds no filter
		}

		return {
			kind: "select",
			table: this.#tableName,
			select: selectCols,
			selectSubqueries: this.#selectSubqueries,
			wheres,
			orderBy: this.#orderBys,
			groupBy: this.#groupBy,
			having: this.#having,
			limit: this.#limit ?? null,
			offset: this.#offset ?? null,
			distinct: this.#distinct,
			ctes: this.#ctes.map((c) => {
				const { sql, params } = c.query.toSQL();
				return { name: c.name, sql, params };
			}),
			unions: this.#unions.map((u) => {
				const { sql, params } = u.query.toSQL();
				return { sql, params, all: u.all };
			}),
			joins: this.#joins,
			lockMode: this.#lockMode
				? this.#lockModifier
					? `${this.#lockMode} ${this.#lockModifier}`
					: this.#lockMode
				: null,
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
			// Thread query-level sideloaded context onto each hydrated instance.
			if (this.#sideloaded) entity.$sideloaded = { ...this.#sideloaded };
			return entity;
		});

		// Resolve preloads (eager loading)
		if (this.#preloads.size > 0 && this.#entityClass && entities.length > 0) {
			await this.#resolvePreloads(entities);
		}

		return entities;
	}

	/**
	 * Execute and return PLAIN row objects (raw snake_case DB columns), skipping
	 * model hydration, `@column({ consume })`, dirty-tracking and preloads —
	 * AdonisJS Lucid `pojo()`. Fast read path for reports/exports where model
	 * instances aren't needed.
	 */
	async pojo<R = Record<string, unknown>>(): Promise<R[]> {
		const { sql, params } = this.toSQL();
		return this.#db.query<R>(sql, params);
	}

	/**
	 * Thread arbitrary context onto every instance this query hydrates, exposed as
	 * `entity.$sideloaded` (AdonisJS Lucid `sideload`) — e.g. the current tenant or
	 * user, so hooks/computed can read it. Merges across calls. Chainable.
	 */
	sideload(values: Record<string, unknown>): this {
		this.#sideloaded = { ...this.#sideloaded, ...values };
		return this;
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
		// Boot the related model's metadata on demand (Lucid parity): a preload
		// must not silently no-op just because the related class hasn't been
		// touched yet elsewhere. ensureEntityMetadata synthesizes @Entity from the
		// static table / naming strategy when the decorator hasn't run.
		const relatedMeta = ensureEntityMetadata(relatedClass);

		// Resolve row keys against declared column metadata, NOT `in entity` —
		// entities using Adonis' `declare field: T` pattern have no own-properties
		// on a freshly constructed instance, so `key in entity` is always false and
		// every column would be silently dropped. Mirrors `BaseRepository.#hydrate`.
		const relatedPkName = getPrimaryKey(relatedClass) ?? "id";
		const validColumns = new Set<string>();
		// Reverse map (db column → property) so an explicit `@Column({ columnName })`
		// on the related entity hydrates correctly — mirrors `BaseRepository.#hydrate`.
		const byDbName = new Map<string, string>();
		// Capture the related model's `@Column({ consume })` adapters + its date
		// columns so preloaded rows hydrate identically to a direct query — dates
		// become Chronos DateTime, decimal/etc adapters run. Without this, a
		// preloaded relation left column values raw (Lucid parity bug + a runtime
		// footgun for getters/serializers/hooks). Mirrors BaseRepository.#applyConsume.
		const consumes = new Map<string, (v: unknown) => unknown>();
		let relatedPkDb = camelToSnake(relatedPkName);
		for (const col of getColumnMetadata(relatedClass)) {
			const db = col.columnName ?? camelToSnake(col.propertyKey);
			validColumns.add(col.propertyKey);
			validColumns.add(db);
			byDbName.set(db, col.propertyKey);
			if (col.consume) consumes.set(col.propertyKey, col.consume);
			// The related PK may be multi-word (postId→post_id) or columnName-mapped;
			// its DB column name is what the WHERE + row indexing must use.
			if (col.propertyKey === relatedPkName) relatedPkDb = db;
		}
		validColumns.add(relatedPkName);
		validColumns.add(camelToSnake(relatedPkName));
		const dateCols = getDateColumnConfig(relatedClass);
		const consumeValue = (prop: string, value: unknown): unknown => {
			const c = consumes.get(prop);
			if (c) return c(value);
			if (dateCols[prop] && value != null)
				return dateTimeAtlasAdapter.consume(value);
			return value;
		};

		// A repository for the related model so preloaded instances are hydrated with
		// the SAME lifecycle state as a direct query: `$isPersisted`/not-`$isNew`,
		// not-`$isLocal`, a clean dirty snapshot, and a REPO_REF backing
		// refresh()/fresh()/load()/related(). Without this a preloaded relation
		// looked $isNew/$isLocal/$dirty and a later save() over-updated it.
		const relatedRepo = new BaseRepository(relatedClass, this.#db, {
			dialect: this.#dialect,
		});
		// Propagate the domain-event bus so save()/create() from a preloaded relation
		// still dispatch events (a fresh repo has none by default).
		relatedRepo.onDomainEvents = this.#onDomainEvents;
		const hydrate = (row: Record<string, unknown>): BaseEntity => {
			const entity = new relatedClass();
			for (const [key, value] of Object.entries(row)) {
				const camelKey = snakeToCamel(key);
				const targetKey =
					byDbName.get(key) ??
					(validColumns.has(camelKey)
						? camelKey
						: validColumns.has(key)
							? key
							: null);
				if (targetKey !== null)
					entity.setProp(targetKey, consumeValue(targetKey, value));
			}
			// Freeze the clean snapshot + mark persisted/from-DB, and back-reference
			// the related repo (mirrors BaseRepository.#hydrate).
			entity.markAsPersisted();
			entity.markAsFromDatabase();
			Object.defineProperty(entity, REPO_REF, {
				value: relatedRepo,
				enumerable: false,
				configurable: true,
			});
			return entity;
		};

		return {
			relation,
			relationName,
			relatedClass,
			relatedTable: relatedMeta.tableName,
			// DB column name (not property) — used as the WHERE column in the related
			// query AND to index the returned DB rows by their PK value.
			relatedPk: relatedPkDb,
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
		const throughMeta = ensureEntityMetadata(throughClass);
		const throughTable = throughMeta.tableName;
		const throughPk = getPrimaryKey(throughClass) ?? "id";
		const parentLocal =
			relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
		const firstKey =
			relation.firstKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
		const secondKey =
			relation.secondKey ?? `${camelToSnake(throughClass.name)}_id`;
		// secondLocal indexes the THROUGH row (`row[secondLocal]`), so it must be a
		// DB column — resolve the through model's key (default: its PK), honouring a
		// multi-word / columnName PK. (parentLocal stays a property: it's read off
		// the parent ENTITY, not a row.)
		const secondLocal = buildColumnResolver(throughClass)(
			relation.secondLocalKey ?? throughPk,
		);

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
		// The pivot FK stores `parent[localKey]` (default PK) — attach() writes it,
		// so preload MUST read back with the SAME key, else a custom-localKey m2m
		// writes `user_code = code` but reads `user_code IN (id)` and never matches.
		const pk =
			ctx.relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";

		const ids = entities.map((e) => e[pk]).filter((v) => v != null);
		if (ids.length === 0) return [];

		// Extract PIVOT-table constraints (wherePivot / wherePivotIn) from the
		// preload callback by replaying it on a throwaway builder. The callback
		// also runs (again) inside runRelationQuery against the related table; both
		// runs are pure builder mutations, and pivot constraints are inert there.
		const pivotWheres: Array<{
			column: string;
			operator: string;
			value: unknown;
		}> = [];
		if (ctx.nestedCallback) {
			const scratch = new ModelQuery<BaseEntity>(
				ctx.relatedTable,
				this.#db,
				(r) => r as BaseEntity,
				ctx.relatedClass,
				buildColumnResolver(ctx.relatedClass),
				false,
				this.#dialect,
				buildValuePreparer(ctx.relatedClass),
			);
			ctx.nestedCallback(scratch);
			// Apply the pivot column adapters' `prepare` to wherePivot values, so a
			// filter like wherePivot('amount', new Money(1)) matches what attach()/
			// sync() stored (they prepare the same extras on write).
			const pivotAdapters = pivot.pivotColumnAdapters ?? {};
			for (const c of scratch.pivotConstraints) {
				const prep = pivotAdapters[c.column]?.prepare;
				// Same guards as the attach()/sync() write path: wrap a throwing
				// adapter with a column-annotated error and reject async adapters,
				// so filter and write agree on the adapter contract.
				const apply = (v: unknown): unknown => {
					if (!prep) return v;
					let out: unknown;
					try {
						out = prep(v);
					} catch (err) {
						throw wrapAdapterError("prepare", c.column, err);
					}
					assertNotPromise("prepare", c.column, out);
					return out;
				};
				const value = Array.isArray(c.value)
					? c.value.map(apply)
					: apply(c.value);
				pivotWheres.push({ ...c, value });
			}
		}

		// Step 1 — pivot table: find (foreignKey → otherKey) pairs (+ wherePivot)
		const pivotRows = await this.#runInQuery(
			pivot.pivotTable,
			foreignKey,
			ids,
			pivotWheres,
		);
		if (pivotRows.length === 0) {
			for (const entity of entities) entity.setProp(relationName, []);
			return [];
		}
		const otherIds = [
			...new Set(pivotRows.map((r) => r[otherKey]).filter((v) => v != null)),
		];

		// Step 2 — load all related entities in one query
		const relRows = await ctx.runRelationQuery(ctx.relatedPk, otherIds);
		const pivotCols = pivot.pivotColumns ?? [];
		const pivotAdapters = pivot.pivotColumnAdapters ?? {};
		// When pivot extras are projected, each (parent, related) edge gets its OWN
		// hydrated instance so per-edge `$extras.pivot_<col>` values never clobber
		// across parents (Lucid gives distinct pivot-bearing instances). Otherwise a
		// single shared instance per related PK is reused (cheaper, current behaviour).
		const projectPivot = pivotCols.length > 0;
		const rawByRelatedPk = new Map<unknown, Record<string, unknown>>();
		const byRelatedPk = new Map<unknown, BaseEntity>();
		const allRelated: BaseEntity[] = [];
		for (const row of relRows) {
			rawByRelatedPk.set(row[ctx.relatedPk], row);
			if (!projectPivot) {
				const hydrated = ctx.hydrate(row);
				byRelatedPk.set(row[ctx.relatedPk], hydrated);
				allRelated.push(hydrated);
			}
		}

		// Step 3 — group via the pivot, projecting declared pivotColumns into
		// `$extras.pivot_<col>` (running each column's `consume` adapter if any).
		const grouped = new Map<unknown, BaseEntity[]>();
		for (const pivotRow of pivotRows) {
			let related: BaseEntity | undefined;
			if (projectPivot) {
				const raw = rawByRelatedPk.get(pivotRow[otherKey]);
				if (!raw) continue;
				related = ctx.hydrate(raw);
				for (const col of pivotCols) {
					const rawVal = pivotRow[col];
					const adapter = pivotAdapters[col];
					related.setExtra(
						`pivot_${col}`,
						adapter?.consume ? adapter.consume(rawVal) : rawVal,
					);
				}
				allRelated.push(related);
			} else {
				related = byRelatedPk.get(pivotRow[otherKey]);
			}
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
			buildColumnResolver(ctx.relatedClass),
			hasSoftDeletes(ctx.relatedClass),
			this.#dialect,
			buildValuePreparer(ctx.relatedClass),
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
		extraWheres: ReadonlyArray<{
			column: string;
			operator: string;
			value: unknown;
			type?: "and" | "or";
		}> = [],
	): Promise<Record<string, unknown>[]> {
		const wheres: Array<Record<string, unknown>> = [
			{ column, operator: "IN", value: values, type: "and" },
		];
		// The caller's filters go in a parenthesised group, never flat beside the
		// `IN`. Flat, an `orWherePivot` would read as
		// `WHERE fk IN (parents) OR active = 1` and hand back rows belonging to
		// other parents; grouped, it is `WHERE fk IN (parents) AND (… OR …)`.
		// With every filter ANDed the two forms are equivalent, so this changes
		// no existing query.
		if (extraWheres.length > 0) {
			wheres.push({
				kind: "group",
				type: "and",
				conditions: extraWheres.map((w) => ({
					column: w.column,
					operator: w.operator,
					value: w.value,
					type: w.type ?? "and",
				})),
			});
		}
		const spec = {
			kind: "select",
			table,
			select: ["*"],
			selectSubqueries: [],
			wheres,
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
			// Resolve columns + prepare values against the RELATED model so a preload
			// constraint (onQuery / callback) targeting a columnName-mapped or date
			// column compiles/binds like a direct query on that model.
			buildColumnResolver(relatedClass),
			// Propagate the RELATED entity's soft-delete flag — hardcoding
			// false here meant `preload('posts')` returned soft-deleted
			// posts even when Post is @SoftDeletes (a data leak). The
			// related query now applies its own `deleted_at IS NULL` filter,
			// matching a direct query on that entity. (with-trashed on the
			// related set, if ever needed, would be opted-in via the
			// preload callback.)
			hasSoftDeletes(relatedClass),
			this.#dialect,
			buildValuePreparer(relatedClass),
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
		const relatedMeta = ensureEntityMetadata(relatedClass);
		const relatedTable = relatedMeta.tableName;
		const parentPk = getPrimaryKey(this.#entityClass) ?? "id";
		const parentTable = this.#tableName;
		// Strict single-segment identifier quote. This builds a RAW correlated
		// subquery fragment (no bind params for identifiers), so every segment must
		// be validated — a table/key from relation metadata carrying a quote/backtick
		// would otherwise emit invalid or injectable SQL. Same policy as
		// BaseRepository's lazy m2m path.
		const q = (name: string): string => {
			if (!/^[A-Za-z0-9_]+$/.test(name)) {
				throw new Error(`Unsafe identifier in relation metadata: '${name}'`);
			}
			return this.#dialect === "mysql" ? `\`${name}\`` : `"${name}"`;
		};
		// Table identifiers may be schema-qualified (`schema.table`) — quote each
		// dotted segment on its own (`"schema"."table"`), else a Postgres pivot like
		// `public.users_roles` gets wrapped as ONE identifier and silently targets a
		// table literally named with a dot. Each segment still passes the strict
		// guard above. Columns stay single-segment via `q`.
		const qTable = (name: string): string => name.split(".").map(q).join(".");

		const sub = new ModelQuery<BaseEntity>(
			relatedTable,
			this.#db,
			(row) => row as BaseEntity,
			relatedClass,
			// whereHas/withCount constraints run against the RELATED model — resolve
			// its columns (columnName/multi-word) and prepare its values like a direct query.
			buildColumnResolver(relatedClass),
			false,
			this.#dialect,
			buildValuePreparer(relatedClass),
		);

		// `localKey`/`ownerKey`/`secondLocalKey` are MODEL properties (default to a
		// PK); resolve each to its DB column via the owning model so a multi-word or
		// `@Column({ columnName })` key produces valid SQL. `foreignKey`/`otherKey`/
		// `firstKey`/`secondKey` are DB column names already — left as-is.
		const resolveParent = buildColumnResolver(this.#entityClass);
		switch (relation.type) {
			case "hasOne":
			case "hasMany": {
				// Honour custom foreignKey/localKey exactly like the eager loader —
				// hard-coding them here produced silently-wrong whereHas/withCount SQL.
				const fk =
					relation.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
				const localKey = resolveParent(relation.localKey ?? parentPk);
				sub.#pushWhereRaw(
					`${qTable(relatedTable)}.${q(fk)} = ${qTable(parentTable)}.${q(localKey)}`,
				);
				break;
			}
			case "belongsTo": {
				const fk =
					relation.foreignKey ?? `${camelToSnake(relatedClass.name)}_id`;
				const ownerKey = buildColumnResolver(relatedClass)(
					relation.ownerKey ?? getPrimaryKey(relatedClass) ?? "id",
				);
				sub.#pushWhereRaw(
					`${qTable(relatedTable)}.${q(ownerKey)} = ${qTable(parentTable)}.${q(fk)}`,
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
				const relatedPkProp = getPrimaryKey(relatedClass) ?? "id";
				const relatedPk =
					getColumnMetadata(relatedClass).find(
						(c) => c.propertyKey === relatedPkProp,
					)?.columnName ?? camelToSnake(relatedPkProp);
				const localKey = resolveParent(relation.localKey ?? parentPk);
				sub.#pushWhereRaw(
					`${qTable(relatedTable)}.${q(relatedPk)} IN ` +
						`(SELECT ${q(otherKey)} FROM ${qTable(pivot.pivotTable)} ` +
						`WHERE ${qTable(pivot.pivotTable)}.${q(foreignKey)} = ${qTable(parentTable)}.${q(localKey)})`,
				);
				break;
			}
			case "hasOneThrough":
			case "hasManyThrough": {
				// Two-hop correlated EXISTS: parent → through → related. Mirrors the
				// eager loader's key resolution (`#resolveThrough`) exactly so
				// whereHas/withCount agree with what preload() would return.
				if (!relation.through) {
					throw new Error(
						`@HasOneThrough/@HasManyThrough '${relationName}' requires a through model`,
					);
				}
				const throughClass = relation.through() as new () => BaseEntity;
				const throughMeta = ensureEntityMetadata(throughClass);
				const throughTable = throughMeta.tableName;
				const throughPk = getPrimaryKey(throughClass) ?? "id";
				const parentLocal = resolveParent(relation.localKey ?? parentPk);
				const firstKey =
					relation.firstKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
				const secondKey =
					relation.secondKey ?? `${camelToSnake(throughClass.name)}_id`;
				const secondLocal = buildColumnResolver(throughClass)(
					relation.secondLocalKey ?? throughPk,
				);
				sub.#pushWhereRaw(
					`${qTable(relatedTable)}.${q(secondKey)} IN ` +
						`(SELECT ${q(secondLocal)} FROM ${qTable(throughTable)} ` +
						`WHERE ${qTable(throughTable)}.${q(firstKey)} = ${qTable(parentTable)}.${q(parentLocal)})`,
				);
				break;
			}
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
		const tq = this.#quoteCol(table);
		this.#joins.push({ sql: `CROSS JOIN ${tq}`, params: [] });
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
	joinRaw(fragment: string, bindings: readonly unknown[] = []): this {
		if (isAtlasStrictMode() && !isInternalBypass()) {
			throw new Error(
				"joinRaw() is disabled in Atlas strict mode. " +
					"Use joinOn() or the callback form of innerJoin/leftJoin/rightJoin instead.",
			);
		}
		this.#joins.push({ sql: fragment, params: [...bindings] });
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
			(await this.#runScalar(
				`COUNT(DISTINCT ${this.#quoteCol(this.#resolveColumn(column))})`,
			)) ?? 0,
		);
	}

	/** `SUM(DISTINCT col)` (Lucid parity). */
	async sumDistinct(column: string): Promise<number | null> {
		const v = await this.#runScalar(
			`SUM(DISTINCT ${this.#quoteCol(this.#resolveColumn(column))})`,
		);
		return v === null || v === undefined ? null : Number(v);
	}

	/** `AVG(DISTINCT col)` (Lucid parity). */
	async avgDistinct(column: string): Promise<number | null> {
		const v = await this.#runScalar(
			`AVG(DISTINCT ${this.#quoteCol(this.#resolveColumn(column))})`,
		);
		return v === null || v === undefined ? null : Number(v);
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
		// COUNT(*) + data fetch
		const countQ = this.clone();
		countQ.#limit = undefined;
		countQ.#offset = undefined;
		countQ.#orderBys = [];
		let cSql: string;
		let cParams: unknown[];
		if (countQ.#groupBy.length > 0) {
			// A flat `SELECT COUNT(*) … GROUP BY x` returns one row PER GROUP (each the
			// group's own size), so `rows[0].count` would be the first group's size, not
			// the number of pages. Lucid counts via a subquery: wrap the grouped query
			// (select + groupBy + having preserved) and count its rows = group count.
			const inner = countQ.toSQL();
			cSql = `SELECT COUNT(*) AS count FROM (${inner.sql}) AS __paginate_count`;
			cParams = inner.params;
		} else {
			countQ.#select = ["COUNT(*) AS count"];
			const flat = countQ.toSQL();
			cSql = flat.sql;
			cParams = flat.params;
		}
		const cRows = await this.#db.query<Record<string, unknown>>(cSql, cParams);
		const total = Number(cRows[0]?.count ?? 0);

		const dataQ = this.clone();
		dataQ.#limit = pp;
		dataQ.#offset = (p - 1) * pp;
		// `#doExec` (not `exec`) so the generic beforeFetch/afterFetch don't fire on
		// top of the paginate hooks — paginate is its own terminal.
		const items = await dataQ.#doExec();
		await fireHooks(this.#entityClass, "afterPaginate", items);
		const metaKeys = this.#entityClass
			? getNamingStrategy(this.#entityClass).paginationMetaKeys?.()
			: undefined;
		return new Paginator<T>(
			items,
			{ total, perPage: pp, currentPage: p },
			metaKeys,
		);
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
		// Keep BOTH forms: `props` (model property names) to read the cursor value
		// off the hydrated entity, and `cols` (resolved DB columns) for the SQL
		// ORDER BY / WHERE. Mixing them up made a columnName/camelCase order key
		// encode `undefined` into the cursor (entity exposes the property, not the
		// DB column) — an unstable / stuck cursor.
		const props = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy];
		const cols = props.map((c) => this.#resolveColumn(c));
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
				? Buffer.from(
						JSON.stringify({ v: props.map((p) => last[p]) }),
					).toString("base64")
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
			this.#prepareValue,
			this.#onDomainEvents,
		);
		c.#softScope = this.#softScope;
		c.#wheres = structuredCloneSafe(this.#wheres);
		c.#orderBys = [...this.#orderBys];
		c.#select = [...this.#select];
		c.#limit = this.#limit;
		c.#offset = this.#offset;
		c.#preloads = new Map(this.#preloads);
		c.#selectSubqueries = structuredClone(this.#selectSubqueries);
		c.#joins = this.#joins.map((j) => ({ sql: j.sql, params: [...j.params] }));
		c.#lockMode = this.#lockMode;
		c.#lockModifier = this.#lockModifier;
		c.#sideloaded = this.#sideloaded ? { ...this.#sideloaded } : null;
		c.#distinct = this.#distinct;
		c.#groupBy = [...this.#groupBy];
		c.#having = structuredCloneSafe(this.#having);
		c.#ctes = this.#ctes.map((e) => ({ name: e.name, query: e.query.clone() }));
		c.#unions = this.#unions.map((u) => ({
			query: u.query.clone(),
			all: u.all,
		}));
		c.#pivotWheres = structuredCloneSafe(this.#pivotWheres);
		// Pure closure over pivot metadata — safe to share by reference; it reads the
		// clone's own #pivotWheres at build time (passed in), holding no query state.
		c.#pivotExists = this.#pivotExists;
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
		// Lower each value through prepare (DateTime → ISO, @Column adapters) exactly
		// like BaseRepository's write paths — the fluent update() must not bypass it.
		const setPairs = Object.entries(patch).map(
			([k, v]) =>
				[this.#resolveColumn(k), this.#prepareValue(k, v)] as [string, unknown],
		);
		const spec = {
			kind: "update",
			table: this.#tableName,
			set: setPairs,
			wheres: this.#wheresForDml(),
			returning: (returning ?? []).map((c) => this.#resolveSelect(c)),
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

	/**
	 * Execute a fluent DELETE. For a `@SoftDeletes` model this SOFT-deletes the
	 * scoped rows (stamps `deleted_at`) — consistent with the entity-level
	 * `delete()`; use {@link forceDelete} for a hard `DELETE`. For a non-soft-delete
	 * model it issues a hard `DELETE`. Returns affected rows (or rows when
	 * `returning` is set).
	 */
	async delete(
		returning?: string[],
	): Promise<number | Record<string, unknown>[]> {
		if (this.#softDeletes) {
			const spec = {
				kind: "update",
				table: this.#tableName,
				set: [[this.#deletedAtColumn(), new Date().toISOString()]],
				wheres: this.#wheresForDml(),
				returning: (returning ?? []).map((c) => this.#resolveSelect(c)),
			};
			return this.#runDml(spec, returning);
		}
		return this.forceDelete(returning);
	}

	/** Hard `DELETE` of the scoped rows, bypassing `@SoftDeletes` (AdonisJS/Lucid `forceDelete`). */
	async forceDelete(
		returning?: string[],
	): Promise<number | Record<string, unknown>[]> {
		const spec = {
			kind: "delete",
			table: this.#tableName,
			wheres: this.#wheresForDml(),
			returning: (returning ?? []).map((c) => this.#resolveSelect(c)),
		};
		return this.#runDml(spec, returning);
	}

	/**
	 * Bulk restore: clear `deleted_at` on the trashed rows matching the user's
	 * predicates (the soft-delete counterpart of {@link delete}). No-op count `0`
	 * on a non-soft-delete model. Independent of the current soft-scope — it always
	 * targets trashed rows (`deleted_at IS NOT NULL`).
	 */
	async restore(
		returning?: string[],
	): Promise<number | Record<string, unknown>[]> {
		if (!this.#softDeletes) return 0;
		const wheres = this.#userWheresForDml();
		wheres.push({
			column: this.#deletedAtColumn(),
			operator: "IS NOT NULL",
			value: null,
			type: "and",
		});
		const spec = {
			kind: "update",
			table: this.#tableName,
			set: [[this.#deletedAtColumn(), null]],
			wheres,
			returning: (returning ?? []).map((c) => this.#resolveSelect(c)),
		};
		return this.#runDml(spec, returning);
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

	/** Postgres `FOR NO KEY UPDATE` — a weaker lock that doesn't block FK checks (AdonisJS/Knex). */
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

	/** Postgres `FOR KEY SHARE` — the weakest share lock (AdonisJS/Knex). */
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

	/**
	 * Append `SKIP LOCKED` to the lock clause — locked rows are skipped instead of
	 * waited on (AdonisJS/Knex). Requires a base lock (`forUpdate`/`forShare`/…).
	 */
	skipLocked(): this {
		if (this.#dialect === "sqlite") {
			console.warn("[atlas] skipLocked ignored on sqlite (no row-level lock)");
		} else {
			this.#lockModifier = "SKIP LOCKED";
		}
		return this;
	}

	/**
	 * Append `NOWAIT` to the lock clause — error immediately instead of waiting on
	 * a locked row (AdonisJS/Knex). Requires a base lock (`forUpdate`/`forShare`/…).
	 */
	noWait(): this {
		if (this.#dialect === "sqlite") {
			console.warn("[atlas] noWait ignored on sqlite (no row-level lock)");
		} else {
			this.#lockModifier = "NOWAIT";
		}
		return this;
	}

	// === Private helpers ==============================================================================

	#quote(name: string): string {
		return this.#dialect === "mysql" ? `\`${name}\`` : `"${name}"`;
	}

	/** Quote a `table.column` reference on both sides of the dot. */
	#quoteCol(ref: string): string {
		// Validate BEFORE quoting — `#quote` only wraps in quotes/backticks, so an
		// identifier smuggling a `"`/backtick would break out of the quoting on the
		// join path (which the Rust screen doesn't re-validate). Strict
		// `[[schema.]table.]column` grammar (up to 3 dot segments); keeps join
		// helpers injection-safe. Use joinRaw() for anything more complex.
		if (!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/.test(ref)) {
			throw new Error(
				`Invalid join/column identifier '${ref}' — expected [[schema.]table.]column (letters, digits, underscore). Use joinRaw() for anything else.`,
			);
		}
		return ref
			.split(".")
			.map((seg) => this.#quote(seg))
			.join(".");
	}

	#pushJoin(
		kind: "INNER" | "LEFT" | "RIGHT",
		table: string,
		leftOrBuild: string | ((j: JoinBuilder) => void),
		right?: string,
	): this {
		const tq = this.#quoteCol(table);
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
				orOn(l: string, r: string) {
					this.parts.push({ kind: "or", left: l, right: r });
					return this;
				},
				onVal(l: string, v: unknown) {
					this.parts.push({ kind: "and", left: l, value: { v } });
					return this;
				},
				andOnVal(l: string, v: unknown) {
					this.parts.push({ kind: "and", left: l, value: { v } });
					return this;
				},
				orOnVal(l: string, v: unknown) {
					this.parts.push({ kind: "or", left: l, value: { v } });
					return this;
				},
			};
			leftOrBuild(jb);
			// Collect the bound values in placeholder order as the fragment is built.
			const params: unknown[] = [];
			const on = jb.parts
				.map((p, i) => {
					const prefix = i === 0 ? "ON" : p.kind === "or" ? "OR" : "AND";
					if (p.value) {
						// A BASE-table column runs the full model prepare (DateTime→ISO +
						// @Column adapters/casts), keyed by its property. A FOREIGN join
						// column must NOT borrow the root model's adapter for a same-named
						// column on another table — apply only universal type-lowering
						// (Date/DateTime→ISO), matching Knex's model-agnostic join binding.
						const dot = p.left.lastIndexOf(".");
						const tablePrefix = dot >= 0 ? p.left.slice(0, dot) : "";
						const leaf = dot >= 0 ? p.left.slice(dot + 1) : p.left;
						const isBaseColumn =
							tablePrefix === "" || sameTableRef(tablePrefix, this.#tableName);
						params.push(
							isBaseColumn
								? this.#prepareValue(leaf, p.value.v)
								: lowerJoinValue(p.value.v),
						);
						return `${prefix} ${this.#quoteCol(p.left)} = ?`;
					}
					return `${prefix} ${this.#quoteCol(p.left)} = ${this.#quoteCol(p.right ?? "")}`;
				})
				.join(" ");
			this.#joins.push({ sql: `${kind} JOIN ${tq} ${on}`, params });
			return this;
		}
		if (right === undefined)
			throw new Error(
				"join() with string form requires both left and right operands",
			);
		this.#joins.push({
			sql: `${kind} JOIN ${tq} ON ${this.#quoteCol(leftOrBuild)} = ${this.#quoteCol(right)}`,
			params: [],
		});
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
	/** The user's own WHERE predicates mapped for DML (no soft-delete scope). */
	#userWheresForDml(): Array<Record<string, unknown>> {
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

	#wheresForDml(): Array<Record<string, unknown>> {
		const out = this.#userWheresForDml();
		// Mirror the read scope (`#buildSpec`): a `@SoftDeletes` model's bulk
		// update/delete/increment/decrement must NOT touch trashed rows under the
		// default scope — otherwise `query().where(x)` would denote a different row
		// set for `.exec()` than for `.update()`/`.delete()`. `.withTrashed()` widens,
		// `.onlyTrashed()` restricts to trashed (mirrors reads).
		if (this.#softDeletes) {
			const deletedAtCol = this.#deletedAtColumn();
			if (this.#softScope === "default") {
				out.push({
					column: deletedAtCol,
					operator: "IS NULL",
					value: null,
					type: "and",
				});
			} else if (this.#softScope === "only-trashed") {
				out.push({
					column: deletedAtCol,
					operator: "IS NOT NULL",
					value: null,
					type: "and",
				});
			}
		}
		return out;
	}

	/** Compile + run a DML spec: returns affected-row count, or rows when `returning` is set. */
	async #runDml(
		spec: Record<string, unknown>,
		returning?: string[],
	): Promise<number | Record<string, unknown>[]> {
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
			this.#prepareValue,
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
				value: this.#prep(column, operatorOrValue),
			});
		} else {
			this.#wheres.push({
				type,
				column: resolved,
				operator: operatorOrValue as string,
				value: this.#prep(column, value),
			});
		}
		return this;
	}

	/**
	 * Lower a WHERE/search value (or each element of an array) to its DB form via
	 * the prepare hook — so a `@column.dateTime` DateTime or a `@Column({ prepare })`
	 * adapter column used as a predicate binds the same shape the write path stores.
	 */
	#prep(column: string, value: unknown): unknown {
		return Array.isArray(value)
			? value.map((v) => this.#prepareValue(column, v))
			: this.#prepareValue(column, value);
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
