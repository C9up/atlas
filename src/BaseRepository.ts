/**
 * BaseRepository — Data Mapper ORM with typed CRUD, soft deletes, and domain events.
 *
 * @implements FR29, FR31, FR35
 */

import { randomUUID } from "node:crypto";
import { DateTime } from "@c9up/chronos";
import { dateTimeAtlasAdapter } from "@c9up/chronos/atlas";
import type {
	QueryMeta,
	TransactionOptions,
} from "./adapters/NapiDbAdapter.js";
import type {
	BaseEntity,
	BelongsToRelationProxy,
	DomainEvent,
	HasManyRelationProxy,
	HasManyThroughRelationProxy,
	HasOneRelationProxy,
	ManyToManyRelationProxy,
	RelationProxy,
} from "./BaseEntity.js";
import { REPO_REF } from "./BaseEntity.js";
import {
	type DateColumnConfig,
	ensureEntityMetadata,
	getColumnMetadata,
	getDateColumnConfig,
	getPrimaryKey,
	getPrimaryKeyGenerator,
	getRelationMetadata,
	hasSoftDeletes,
	type PrimaryKeyGenerator,
} from "./decorators/entity.js";
import { fireHooks } from "./decorators/hooks.js";
import { AtlasError, EntityNotFoundError } from "./errors.js";
import { isAtlasStrictMode, ModelQuery } from "./ModelQuery.js";
import { DatabaseQueryBuilder } from "./query/DatabaseQueryBuilder.js";
import {
	type AtlasDialect,
	compileStatementNative,
	getAtlasDialect,
	registerColumnCast,
	registerTableCasts,
} from "./query/native.js";
import { type TransactionClient, transaction } from "./Transaction.js";
import { camelToSnake, snakeToCamel } from "./utils/casing.js";
import { isTransactionClient } from "./utils/transactionBrand.js";

type EntityConstructor<T extends BaseEntity> = new () => T;

/**
 * String-keyed bag of values — covers the recurring DB-shaped objects:
 * row dictionaries, parameter maps, JSON column blobs. Duplicated locally
 * (mirror of `Dict` in `@c9up/ream`) to keep atlas import-graph agnostic.
 */
export type Dict<V = string> = Record<string, V>;

/** Convenience alias for a DB row (column name → value). */
export type Row = Dict<unknown>;

/**
 * What the engine surfaces about a freshly inserted row. `row` is set when
 * the dialect supports `RETURNING` (postgres / sqlite); `lastInsertRowid`
 * is the better-sqlite3 / mysql fallback for the new auto-increment id.
 */
interface InsertOutcome {
	row?: Row;
	lastInsertRowid?: number | bigint;
}

/**
 * Coerce a `lastInsertRowid` to a JS number when it fits, leaving large
 * mysql/sqlite values as bigint so callers don't silently lose precision.
 */
function normalizeRowid(rowid: number | bigint): number | bigint {
	if (typeof rowid === "number") return rowid;
	return rowid <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(rowid) : rowid;
}

/**
 * Whether a primary-key value should be treated as supplied. Distinguishes
 * "explicit zero / empty-string id" from "unset" — only `null`/`undefined`
 * route through the INSERT path; every other value is a candidate UPDATE.
 */
function isProvidedPk(pk: unknown): pk is string | number | bigint {
	return pk !== undefined && pk !== null;
}

/**
 * Detect a unique-key / primary-key violation from the underlying driver
 * error. Used by `save()` to recover from a TOCTOU race between the
 * `find(pk)` check and the `INSERT`: a concurrent insert that wins the PK
 * race surfaces as one of these codes, and we fall back to UPDATE rather
 * than propagate a DB constraint error.
 *
 *   - PostgreSQL: SQLSTATE `23505` (`unique_violation`)
 *   - SQLite:     `SQLITE_CONSTRAINT_PRIMARYKEY` / `SQLITE_CONSTRAINT_UNIQUE`
 *   - MySQL:      `ER_DUP_ENTRY` (named) / errno `1062` (numeric)
 */
function isUniqueKeyViolation(err: unknown): boolean {
	if (err === null || typeof err !== "object") return false;
	const e = err as Record<string, unknown>;
	const code = e.code;
	const errno = e.errno;
	return (
		code === "23505" ||
		code === "SQLITE_CONSTRAINT_PRIMARYKEY" ||
		code === "SQLITE_CONSTRAINT_UNIQUE" ||
		code === "ER_DUP_ENTRY" ||
		errno === 1062
	);
}

/**
 * Async database connection — matches the `AsyncDatabaseConnection` shape
 * exposed by `AtlasProvider` (Rust-backed napi adapter). All BaseRepository
 * I/O is async (`execute` for writes, `query` for reads). The legacy sync
 * `prepare()` API was removed in favour of this surface to align with the
 * actual binding produced by the provider.
 *
 * Drivers backed by `AsyncDatabaseConnection` (`createNapiConnection`)
 * satisfy this interface out-of-the-box.
 */
export interface DatabaseConnection {
	/**
	 * Run a write statement; returns rowsAffected.
	 *
	 * `meta` is optional context for the `db:query` event (model, method,
	 * per-query debug). A connection that ignores it — every test fake — is
	 * still a valid `DatabaseConnection`.
	 */
	execute(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<{ rowsAffected: number }>;
	/** Run a SELECT and return all rows. See {@link execute} for `meta`. */
	query<T = Row>(
		sql: string,
		params?: unknown[],
		meta?: QueryMeta,
	): Promise<T[]>;
	/**
	 * Optional — open an interactive transaction pinned to ONE connection
	 * (Lucid's `db.transaction`: manual without a callback, managed with one).
	 * Present on napi-backed connections (`createNapiConnection`); absent on
	 * minimal/test fakes, which are single-connection anyway so the standalone
	 * `transaction()` falls back to issuing BEGIN/COMMIT over this same handle.
	 */
	transaction?<T>(
		callback: (trx: TransactionClient) => Promise<T> | T,
		options?: TransactionOptions,
	): Promise<T>;
}

// ─── Repository ─────────────────────────────────────────────

/** Logical column types Postgres won't coerce from a text-bound param. */
const POSTGRES_CAST_TYPES = new Set([
	"timestamp",
	"timestamptz",
	"datetime",
	"date",
	"time",
	"uuid",
	"json",
	"jsonb",
	"numeric",
	"decimal",
	// Nullable integer columns (opt-in via `@Column({ type: 'integer' })`): a JS
	// number binds as a real int, but a JS `null` binds as text, which Postgres
	// won't coerce to int on assignment. Untyped `@Column()` int columns never
	// reach here, so their plain-number bind is untouched.
	"integer",
	"int",
	"bigint",
	"smallint",
	// Nullable boolean / float columns hit the same text-bound-NULL issue.
	"boolean",
	"bool",
	"real",
	"float4",
	"double precision",
	"double",
	"float8",
	"float",
]);

/**
 * Snake column → logical type for params needing a Postgres `$N::<type>` cast.
 * sqlx binds JS strings as `text`; Postgres won't implicitly coerce that to
 * timestamp/uuid/date. The Rust compiler applies these on Postgres only.
 */
export function computeCastTypes(
	entityClass: Parameters<typeof getColumnMetadata>[0],
): Record<string, string> {
	const out: Record<string, string> = {};
	// Resolve each property to its real DB column, honouring `@Column({ columnName })`
	// — the cast MUST key off the column name that actually appears in the SQL.
	const dbNameOf = new Map<string, string>();
	for (const col of getColumnMetadata(entityClass)) {
		dbNameOf.set(
			col.propertyKey,
			col.columnName ?? camelToSnake(col.propertyKey),
		);
	}
	for (const col of getColumnMetadata(entityClass)) {
		const t = col.type?.toLowerCase();
		if (t && POSTGRES_CAST_TYPES.has(t)) {
			out[dbNameOf.get(col.propertyKey) ?? camelToSnake(col.propertyKey)] = t;
		}
	}
	// `@column.date()` / `@column.dateTime()` columns are tracked in a SEPARATE
	// metadata map (DATE_COLUMNS_KEY) and routinely declared with no explicit
	// `type` — e.g. `@column.dateTime() declare emailVerifiedAt: Date | null`.
	// Their values bind as ISO strings (text), so Postgres needs an explicit
	// `::date` / `::timestamp` cast exactly like the `col.type`-flagged columns
	// above; without it sqlx's text bind hits `column is of type timestamp but
	// expression is of type text`. An explicit recognized `col.type` already set
	// in the loop above wins via `??=`.
	for (const [prop, cfg] of Object.entries(getDateColumnConfig(entityClass))) {
		out[dbNameOf.get(prop) ?? camelToSnake(prop)] ??= cfg.dateOnly
			? "date"
			: "timestamp";
	}
	// A uuid-strategy primary key is generated app-side as a string.
	if (getPrimaryKeyGenerator(entityClass) === "uuid") {
		const pk = getPrimaryKey(entityClass) ?? "id";
		out[dbNameOf.get(pk) ?? camelToSnake(pk)] ??= "uuid";
	}
	return out;
}

export class BaseRepository<T extends BaseEntity> {
	#entityClass: EntityConstructor<T>;
	#tableName: string;
	#primaryKey: string;
	#columns: string[];
	#db: DatabaseConnection;
	#softDeletes: boolean;
	#validColumns: Set<string>;
	#columnMap: Map<string, string>; // property/db name → resolved db column (cached)
	#columnByDbName: Map<string, string>; // resolved db column → property (for hydrate)
	#dateColumns: Record<string, DateColumnConfig>;
	/** Snake column → logical type for params needing a Postgres `::cast`. */
	#castTypes: Record<string, string>;
	/**
	 * Per-property `prepare` (model → DB) callbacks lifted directly from
	 * `@Column({ prepare })` metadata. Keyed by camelCase `propertyKey`.
	 * Mirror of Adonis Lucid's `@column.prepare`. Story 35.10.
	 */
	#columnPrepares: Map<
		string,
		(value: unknown, attribute?: string, model?: unknown) => unknown
	>;
	/**
	 * Per-property `consume` (DB → model) callbacks lifted directly from
	 * `@Column({ consume })` metadata. Keyed by camelCase `propertyKey`.
	 * Mirror of Adonis Lucid's `@column.consume`. Story 35.10.
	 */
	#columnConsumes: Map<
		string,
		(value: unknown, attribute?: string, model?: unknown) => unknown
	>;
	/**
	 * SQL dialect used by this repository. Resolved at construction time from
	 * the connection (if it exposes a `dialect` property) or from the explicit
	 * `options.dialect` override, falling back to the process-wide default as
	 * the last resort. Passed to every `compileStatementNative` call so that
	 * multi-connection apps with heterogeneous dialects (postgres + mysql, …)
	 * compile each query with the correct target.
	 */
	#dialect: AtlasDialect;

	/** Callback to dispatch domain events (set by framework integration). */
	onDomainEvents?: (events: DomainEvent[]) => Promise<void>;

	/**
	 * The durable (non-transactional) repo a `useTransaction(trx)` copy was forked
	 * from. Lucid resets a model's `$trx` on commit AND rollback, so after a manual
	 * transaction ends every entity persisted through the trx-bound repo must have
	 * its REPO_REF re-pointed here — otherwise related()/refresh() run on a finished
	 * transaction. Undefined on a durable repo (it IS the durable parent).
	 */
	#durableParent?: BaseRepository<T>;

	constructor(
		entityClass: EntityConstructor<T>,
		db: DatabaseConnection,
		options?: { dialect?: AtlasDialect },
	) {
		this.#entityClass = entityClass;
		if (db == null) {
			// A null/undefined connection almost always means IoC constructor
			// injection failed (missing decorator metadata) — fail with a clear
			// message instead of the cryptic `reading 'dialect' of undefined`.
			throw new AtlasError(
				"MISSING_CONNECTION",
				`BaseRepository for '${entityClass.name}' requires a DatabaseConnection (got ${db === null ? "null" : "undefined"}).`,
				{
					hint: "The connection was not injected. Check IoC constructor injection is wired — decorator metadata (emitDecoratorMetadata) or @Inject(token) on the connection parameter.",
				},
			);
		}
		this.#db = db;

		// Dialect resolution order: explicit option > connection.dialect > process default.
		const connDialect = (db as { dialect?: AtlasDialect }).dialect;
		this.#dialect = options?.dialect ?? connDialect ?? getAtlasDialect();

		// Infer the table name (naming strategy / `static table`) when @Entity is
		// absent — AdonisJS Lucid parity, shared with BaseModel via one helper so
		// the Data-Mapper and Active-Record paths agree on the convention.
		const meta = ensureEntityMetadata(entityClass);

		this.#tableName = meta.tableName;
		this.#primaryKey = getPrimaryKey(entityClass) ?? "id";
		const columnsMeta = getColumnMetadata(entityClass);
		this.#columns = columnsMeta.map((c) => c.propertyKey);
		this.#softDeletes = hasSoftDeletes(entityClass);
		this.#dateColumns = getDateColumnConfig(entityClass);

		// Lift per-column `prepare` / `consume` callbacks directly from metadata.
		// No global registry, no late-registration concern: callbacks are baked
		// into the entity definition. Mirrors Adonis Lucid's `@column.prepare` /
		// `@column.consume` pattern.
		this.#columnPrepares = new Map<
			string,
			(value: unknown, attribute?: string, model?: unknown) => unknown
		>();
		this.#columnConsumes = new Map<
			string,
			(value: unknown, attribute?: string, model?: unknown) => unknown
		>();
		for (const col of columnsMeta) {
			if (col.prepare) this.#columnPrepares.set(col.propertyKey, col.prepare);
			if (col.consume) this.#columnConsumes.set(col.propertyKey, col.consume);
		}

		// Pre-compute column mappings for validation + hydration.
		// Snapshot is frozen at construction — `@Column` decorators that run
		// AFTER the repository instance is created (e.g. lazy/dynamic
		// definitions) are invisible to the validator and will be rejected
		// by `#resolveColumn`. Decorators must run at class-body evaluation
		// time, before any repository for that entity is instantiated.
		this.#validColumns = new Set<string>();
		this.#columnMap = new Map<string, string>();
		this.#columnByDbName = new Map<string, string>();
		for (const col of columnsMeta) {
			const prop = col.propertyKey;
			// Explicit `@Column({ columnName })` wins over the snake_case convention.
			const db = col.columnName ?? camelToSnake(prop);
			this.#validColumns.add(prop);
			this.#validColumns.add(db);
			this.#columnMap.set(prop, db);
			this.#columnMap.set(db, db);
			// Reverse map for hydration — a DB row keyed by the real column name maps
			// back to the TS property (covers explicit overrides AND the default,
			// where `snakeToCamel(db)` would otherwise mis-resolve an override).
			this.#columnByDbName.set(db, prop);
		}
		// PK is registered as a column too (via @PrimaryKey → Column), so the loop
		// above already mapped it, honouring any columnName. Fall back for the rare
		// PK declared outside the column metadata.
		this.#validColumns.add(this.#primaryKey);
		if (!this.#columnMap.has(this.#primaryKey)) {
			const pkDb = camelToSnake(this.#primaryKey);
			this.#validColumns.add(pkDb);
			this.#columnMap.set(this.#primaryKey, pkDb);
			this.#columnMap.set(pkDb, pkDb);
			this.#columnByDbName.set(pkDb, this.#primaryKey);
		}

		// Postgres cast hints: sqlx binds JS strings as `text`, which Postgres
		// won't coerce to timestamp/uuid/date. See `computeCastTypes`.
		this.#castTypes = computeCastTypes(entityClass);
		// Publish this table's casts to the compile-time registry so EVERY
		// statement on this table — including the fluent `ModelQuery` and relation
		// loaders, which never receive `#castTypes` directly — gets `$N::uuid`
		// casts on its params. Without this, `repo.query().where('id', uuid)` and
		// relation WHEREs fail on Postgres with `operator does not exist: uuid = text`.
		registerTableCasts(this.#tableName, this.#castTypes);
		// Publish relation FK column casts to the (merging) registry so eager AND
		// lazy relation WHEREs on a uuid FK get `::uuid` even when the FK column
		// wasn't explicitly `@Column({ type })`-typed. A FK references a typed PK
		// whose logical type we know. (m2m FKs live on the pivot table — handled
		// separately via `pivotKeyCasts`.)
		for (const rel of getRelationMetadata(entityClass)) {
			if (rel.type === "manyToMany") continue;
			const related = rel.target();
			if (rel.type === "belongsTo") {
				// FK lives on THIS table, references the related (owner) PK.
				const fk = rel.foreignKey ?? `${camelToSnake(related.name)}_id`;
				const ownerKey = rel.ownerKey ?? getPrimaryKey(related) ?? "id";
				const ownerDb =
					getColumnMetadata(related).find((c) => c.propertyKey === ownerKey)
						?.columnName ?? camelToSnake(ownerKey);
				const cast = computeCastTypes(related)[ownerDb];
				if (cast) registerColumnCast(this.#tableName, fk, cast);
			} else {
				// hasOne / hasMany: FK lives on the RELATED table, references THIS PK.
				const fk = rel.foreignKey ?? `${camelToSnake(entityClass.name)}_id`;
				const localKey = rel.localKey ?? this.#primaryKey;
				const cast = this.#castTypes[this.#dbColumn(localKey)];
				// Boot the related model on demand (Lucid lazy-boot): a related model
				// with only `static table` (no @Entity yet) would otherwise miss its FK
				// cast → a uuid FK relation query compiles without `::uuid` and breaks on
				// Postgres. Mirrors relatedProxy / the preload paths.
				const relatedMeta = ensureEntityMetadata(related);
				if (cast) {
					registerColumnCast(relatedMeta.tableName, fk, cast);
				}
			}
		}
	}

	// ─── Column validation ────────────────────────────────────

	/** Resolve a column name to snake_case. Throws on invalid column. */
	#resolveColumn(column: string): string {
		const mapped = this.#columnMap.get(column);
		if (mapped) return mapped;

		const snake = camelToSnake(column);
		if (this.#validColumns.has(snake)) return snake;

		throw new AtlasError(
			"E_INVALID_COLUMN",
			`Column '${column}' does not exist on ${this.#entityClass.name}`,
			{
				hint: `Valid columns: ${this.#columns.join(", ")}`,
			},
		);
	}

	/**
	 * Resolve a KNOWN property (from `this.#columns`) to its real DB column name,
	 * honouring `@Column({ columnName })`. Non-throwing — used on the write path
	 * where the column set is already trusted. Falls back to the snake convention.
	 */
	#dbColumn(prop: string): string {
		return this.#columnMap.get(prop) ?? camelToSnake(prop);
	}

	/**
	 * Normalise a mass-assignment key to its TS property. A payload may key by the
	 * DB column name (incl. an explicit `columnName`); without this, `create({
	 * full_label: 'x' })` would set a `full_label` property that the INSERT (which
	 * reads declared properties) then drops silently.
	 */
	#toProperty(key: string): string {
		return this.#columnByDbName.get(key) ?? key;
	}

	// ─── Query builder ────────────────────────────────────────

	query(): ModelQuery<T> {
		return new ModelQuery<T>(
			this.#tableName,
			this.#db,
			(row) => this.#hydrate(row),
			this.#entityClass,
			(col) => this.#resolveColumn(col),
			this.#softDeletes,
			this.#dialect,
			(prop, value) => this.#applyPrepare(prop, value),
			this.onDomainEvents,
		);
	}

	// ─── Transaction ──────────────────────────────────────────

	useTransaction(trx: DatabaseConnection): BaseRepository<T> {
		// Propagate the owning repo's dialect so the transactional copy stays on
		// the correct SQL flavour — critical for multi-connection apps where the
		// primary is postgres but a tenant runs on sqlite (or vice versa).
		// Without this the transactional repo silently fell back to the global
		// default and compiled mis-quoted SQL.
		const repo = new BaseRepository<T>(this.#entityClass, trx, {
			dialect: this.#dialect,
		});
		repo.onDomainEvents = this.onDomainEvents;
		// Chain back to the true durable root (a nested useTransaction forwards it)
		// so post-transaction REPO_REF restoration always lands on a live connection.
		repo.#durableParent = this.#durableParent ?? this;
		return repo;
	}

	// ─── Finders ──────────────────────────────────────────────

	async find(id: string | number | bigint): Promise<T | null> {
		// AdonisJS Lucid throws on `undefined`/`null` rather than silently running
		// `WHERE pk = NULL` (which matches nothing) — a common typo footgun.
		if (id === undefined || id === null) {
			throw new AtlasError(
				"E_INVALID_FIND_VALUE",
				`${this.#entityClass.name}.find() expects a value, received ${String(id)}.`,
			);
		}
		// Route through the query builder so the read hooks (beforeFind/afterFind)
		// fire and a `beforeFind` hook can mutate the query — the previous direct
		// `#compileSelect` fast path bypassed every read hook silently.
		return this.query().where(this.#primaryKey, id).first();
	}

	async findOrFail(id: string | number): Promise<T> {
		const entity = await this.find(id);
		if (!entity) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: id,
			});
		}
		return entity;
	}

	async findBy(column: string, value: unknown): Promise<T | null>;
	async findBy(clause: Record<string, unknown>): Promise<T | null>;
	async findBy(
		columnOrClause: string | Record<string, unknown>,
		value?: unknown,
	): Promise<T | null> {
		// Through the builder for read-hook parity (see `find`).
		let q = this.query();
		if (typeof columnOrClause === "string") {
			q = q.where(columnOrClause, value);
		} else {
			for (const [k, v] of Object.entries(columnOrClause)) q = q.where(k, v);
		}
		return q.first();
	}

	/** Find by a column/clause or throw `EntityNotFoundError` (AdonisJS `findByOrFail`). */
	async findByOrFail(column: string, value: unknown): Promise<T>;
	async findByOrFail(clause: Record<string, unknown>): Promise<T>;
	async findByOrFail(
		columnOrClause: string | Record<string, unknown>,
		value?: unknown,
	): Promise<T> {
		const entity =
			typeof columnOrClause === "string"
				? await this.findBy(columnOrClause, value)
				: await this.findBy(columnOrClause);
		if (!entity) {
			const criteria =
				typeof columnOrClause === "string"
					? { [columnOrClause]: value }
					: columnOrClause;
			throw new EntityNotFoundError(this.#entityClass.name, criteria);
		}
		return entity;
	}

	/** Find many rows by primary key (AdonisJS `findMany`), ordered PK desc. */
	async findMany(ids: Array<string | number>): Promise<T[]> {
		if (ids.length === 0) return [];
		return this.query()
			.whereIn(this.#primaryKey, ids)
			.orderBy(this.#primaryKey, "desc")
			.exec();
	}

	/** Find many rows by a column IN values, or by an object clause (AdonisJS `findManyBy`). */
	async findManyBy(
		column: string,
		values: Array<string | number>,
	): Promise<T[]>;
	async findManyBy(clause: Record<string, unknown>): Promise<T[]>;
	async findManyBy(
		columnOrClause: string | Record<string, unknown>,
		values?: Array<string | number>,
	): Promise<T[]> {
		if (typeof columnOrClause === "string") {
			if (!values || values.length === 0) return [];
			return this.query().whereIn(columnOrClause, values).exec();
		}
		let q = this.query();
		for (const [k, v] of Object.entries(columnOrClause)) q = q.where(k, v);
		return q.exec();
	}

	async all(): Promise<T[]> {
		// Through the builder so beforeFetch/afterFetch fire. The builder applies
		// the soft-delete scope by default, exactly like the old fast path.
		// Ordered PK desc for AdonisJS Lucid `all()` parity (newest first).
		return this.query().orderBy(this.#primaryKey, "desc").exec();
	}

	/**
	 * Empty this model's table (AdonisJS Lucid `Model.truncate`). Postgres/MySQL
	 * issue `TRUNCATE TABLE` (fast, resets identity); SQLite has no TRUNCATE so it
	 * falls back to `DELETE FROM`. `cascade` is Postgres-only (truncates dependent
	 * FK tables). The table name comes from entity metadata — never user input.
	 */
	async truncate(cascade = false): Promise<void> {
		// Quote each dotted segment so a schema-qualified table (`reporting.events`)
		// becomes `"reporting"."events"`, not one dotted identifier that TRUNCATEs
		// the wrong (nonexistent) table. Validate each segment even though the table
		// name is app metadata: a bulletproof ORM must never emit malformed/injectable
		// raw SQL from a `static table = 'x"; DROP…'` slip (same policy as qTable).
		const wrap = (seg: string): string => {
			if (!/^[A-Za-z0-9_]+$/.test(seg)) {
				throw new Error(`Unsafe table identifier: '${seg}'`);
			}
			return this.#dialect === "mysql" ? `\`${seg}\`` : `"${seg}"`;
		};
		const quoted = this.#tableName.split(".").map(wrap).join(".");
		if (this.#dialect === "sqlite") {
			await this.#db.query(`DELETE FROM ${quoted}`, []);
			return;
		}
		const suffix = cascade && this.#dialect === "postgres" ? " CASCADE" : "";
		await this.#db.query(`TRUNCATE TABLE ${quoted}${suffix}`, []);
	}

	async allWithTrashed(): Promise<T[]> {
		return this.query().withTrashed().exec();
	}

	async onlyTrashed(): Promise<T[]> {
		if (!this.#softDeletes) return [];
		return this.query().onlyTrashed().exec();
	}

	async where(column: string, value: unknown): Promise<T[]> {
		// No implicit ORDER BY — the row order is left to the database, matching
		// Lucid's query-builder `where` (only `all`/`findMany` order by PK desc,
		// which Lucid itself does). Add `.orderBy()` explicitly when order matters.
		// Through the builder for read-hook parity (see `find`).
		return this.query().where(column, value).exec();
	}

	// ─── Create / Save / Delete ───────────────────────────────

	/**
	 * Build an entity from a plain object and persist it. Fires `beforeCreate` →
	 * `beforeSave` → INSERT → `afterCreate` → `afterSave` (AdonisJS/Lucid order:
	 * the specific hook runs before the general `beforeSave`).
	 */
	async create(
		data: Partial<Record<string, unknown>>,
		quiet = false,
	): Promise<T> {
		const entity = new this.#entityClass();
		for (const [key, value] of Object.entries(data)) {
			if (
				this.#validColumns.has(key) ||
				this.#validColumns.has(camelToSnake(key))
			) {
				const prop = this.#toProperty(key);
				entity.assertMassAssignable(prop);
				entity.setProp(prop, value);
			}
		}
		if (!quiet) {
			await fireHooks(this.#entityClass, "beforeCreate", entity);
			await fireHooks(this.#entityClass, "beforeSave", entity);
		}
		await this.#insert(entity);
		this.#attachRepoRef(entity);
		if (!quiet) {
			await fireHooks(this.#entityClass, "afterCreate", entity);
			await fireHooks(this.#entityClass, "afterSave", entity);
		}
		await this.#dispatchOrDefer(entity, true);
		return entity;
	}

	/** {@link create} without firing lifecycle hooks (AdonisJS Lucid `createQuietly`). */
	createQuietly(data: Partial<Record<string, unknown>>): Promise<T> {
		return this.create(data, true);
	}

	/**
	 * Persist an entity. Insert if PK is missing or row doesn't exist, update
	 * otherwise. Fires (`beforeCreate` | `beforeUpdate`) → `beforeSave` → DB →
	 * (`afterCreate` | `afterUpdate`) → `afterSave` (AdonisJS/Lucid order: the
	 * specific hook runs before the general `beforeSave`), then dispatches
	 * accumulated domain events through `onDomainEvents`.
	 *
	 * Race-safety: the `find(pk)` → branch decision has a TOCTOU window. If a
	 * concurrent save inserts the same PK between our `find` and our `#insert`,
	 * the INSERT hits a unique-key violation; we catch it and fall back to the
	 * UPDATE path. The race-loser still fires `beforeCreate` before the
	 * recovery (its hook ran once before the conflict surfaced) — design
	 * `beforeCreate` hooks to be idempotent or move side-effects into
	 * `afterCreate` / `afterSave` where they only fire on commit.
	 */
	async save(entity: T, quiet = false): Promise<void> {
		// A deleted instance must not be resurrected (AdonisJS Lucid parity —
		// `save()` throws once `$isDeleted` is set). Prevents recreating a row the
		// caller believes is gone, or clobbering one deleted concurrently.
		if (entity.$isDeleted) {
			throw new AtlasError(
				"E_MODEL_DELETED",
				`Cannot save a deleted ${this.#entityClass.name} instance.`,
				{
					hint: "The instance was already deleted; re-fetch it before saving again.",
				},
			);
		}
		const pk = entity[this.#primaryKey];
		// A DB-originated entity whose PK wasn't loaded (an aggregate/alias partial
		// projection) must NOT be treated as new — that would INSERT a duplicate.
		// Fail loud: re-fetch it fully or use `.pojo()` for projections.
		if (entity.$isPersisted && !isProvidedPk(pk)) {
			throw new AtlasError(
				"E_MISSING_PRIMARY_KEY",
				`Cannot save a ${this.#entityClass.name} loaded without its primary key ('${this.#primaryKey}').`,
				{
					hint: "Select the primary key (plain-column projections auto-include it) or use query().pojo() for aggregate/alias projections.",
				},
			);
		}
		// Decide insert-vs-update from the in-memory `$isPersisted` flag, exactly as
		// AdonisJS/Lucid does — NOT a `find(pk)` SELECT probe. The old probe fired the
		// `beforeFind`/`afterFind` read hooks on every `save()` (a spurious side
		// effect: `save()` isn't a find) and cost an extra round-trip. A brand-new
		// entity whose manual PK collides with an existing row still resolves to an
		// UPDATE via the unique-violation fallback below.
		const isUpdate = entity.$isPersisted;

		// Snapshot the domain-event queue BEFORE hooks/write add to it, so a rollback
		// (see #dispatchOrDefer) drops only this save's events, not ones the caller
		// queued earlier.
		const eventFloor = entity.domainEventCount();

		if (!quiet) {
			// AdonisJS/Lucid order: the SPECIFIC before-hook fires first, then the
			// general `beforeSave`, then the DB write.
			await fireHooks(
				this.#entityClass,
				isUpdate ? "beforeUpdate" : "beforeCreate",
				entity,
			);
			await fireHooks(this.#entityClass, "beforeSave", entity);
		}
		// Whether THIS call inserted a brand-new row (vs updated an existing one).
		// Drives the manual-transaction rollback restore: only a fresh INSERT's row
		// vanishes on rollback, so only it reverts to not-persisted. The race-recovery
		// fallback below stays false — the row pre-existed (a concurrent writer).
		let didInsert = false;
		if (isUpdate) {
			await this.#runUpdateBranch(entity, quiet);
		} else {
			try {
				await this.#runInsertBranch(entity, quiet);
				didInsert = true;
			} catch (err) {
				// Race recovery: the row didn't exist when we checked, but a
				// concurrent insert beat us to it. Only fall back when the PK
				// was explicitly provided (auto-generated PK can't collide on
				// a fresh insert — DB generates a unique one per call).
				if (isProvidedPk(pk) && isUniqueKeyViolation(err)) {
					// We already fired `beforeCreate`; fire `beforeUpdate` too so the
					// update branch's contract holds (documented race quirk).
					if (!quiet)
						await fireHooks(this.#entityClass, "beforeUpdate", entity);
					await this.#runUpdateBranch(entity, quiet);
				} else {
					throw err;
				}
			}
		}
		this.#attachRepoRef(entity);
		if (!quiet) await fireHooks(this.#entityClass, "afterSave", entity);

		await this.#dispatchOrDefer(entity, didInsert, eventFloor);
	}

	/** {@link save} without firing lifecycle hooks (AdonisJS Lucid `saveQuietly`). */
	saveQuietly(entity: T): Promise<void> {
		return this.save(entity, true);
	}

	/**
	 * When true (a repo bound to an atlas-managed transaction), `create`/`save`/
	 * `createMany` BUFFER domain events on the entity instead of dispatching them
	 * inline. The managed helper flushes them only AFTER the transaction commits, so
	 * a rollback never emits events for rows that were rolled back.
	 */
	#deferDomainEvents = false;

	/**
	 * When set (a trx-bound repo whose owner wants to undo fresh inserts on
	 * rollback), every successful fresh INSERT through this repo pushes its entity
	 * here. The owner (a managed batch or a relation write) then reverts exactly
	 * these entities — the ones whose row provably did not exist before — to $isNew
	 * if the transaction rolls back, without a DB probe or find-vs-create bookkeeping.
	 * Undefined on a durable repo (nothing to undo — its writes are their own commit).
	 */
	#insertTracker?: BaseEntity[];

	/** Record a fresh INSERT so its owner can revert it on rollback (see {@link #insertTracker}). */
	#trackInsert(entity: BaseEntity): void {
		this.#insertTracker?.push(entity);
	}

	/**
	 * Flush the entity's accumulated domain events through `onDomainEvents`.
	 * On dispatch failure the events are re-queued on the entity and the error
	 * propagates, so a caller can retry without losing them. Shared by `save`,
	 * `create` and `createMany` — every persistence path that produces a live
	 * entity must dispatch, otherwise events silently vanish on batch inserts.
	 */
	async #dispatchDomainEvents(entity: BaseEntity): Promise<void> {
		const events = entity.flushDomainEvents();
		if (events.length > 0 && this.onDomainEvents) {
			try {
				await this.onDomainEvents([...events]);
			} catch (err) {
				for (const e of events) entity.addDomainEvent(e.name, e.data);
				throw err;
			}
		}
	}

	/**
	 * Dispatch an entity's domain events AND restore its in-memory state across a
	 * transaction boundary, honouring the post-commit contract in EVERY context
	 * (BaseEntity documents post-commit flush):
	 *  - inside a MANAGED batch (`#inManagedTx` set `#deferDomainEvents`): skip —
	 *    that helper flushes `collect(result)` on `trx.after('commit')`, and its
	 *    callers (`#inManagedTx` re-attach / `saveMany` rollback catch) restore
	 *    REPO_REF + $isPersisted + events themselves.
	 *  - inside a MANUAL transaction (`repo.useTransaction(trx).create(...)`): the
	 *    repo's `#db` IS the trx. Register post-transaction hooks:
	 *    · commit  → re-point REPO_REF at the durable repo (Lucid resets `$trx` on
	 *      commit) then flush events (a rollback thus publishes NOTHING).
	 *    · rollback → re-point REPO_REF at the durable repo (Lucid also resets
	 *      `$trx` on rollback); revert a fresh INSERT to not-persisted — the row
	 *      never existed, and keeping `$isPersisted` would let a later
	 *      `entity.related('x').create()` skip the parent save and write a child
	 *      with a phantom FK (named data-integrity deviation vs Lucid, same class
	 *      as the saveMany rollback fix); and clear the queued domain events — they
	 *      describe a write that didn't happen, so leaving them would double-publish
	 *      on a re-save.
	 *  - no transaction: dispatch immediately.
	 */
	async #dispatchOrDefer(
		entity: BaseEntity,
		wasInsert: boolean,
		eventFloor = 0,
	): Promise<void> {
		if (this.#deferDomainEvents) return;
		if (isTransactionClient(this.#db)) {
			const durable = this.#durableParent ?? this;
			this.#db.after("commit", async () => {
				durable.#attachRepoRef(entity);
				await this.#dispatchDomainEvents(entity);
			});
			this.#db.after("rollback", () => {
				durable.#attachRepoRef(entity);
				if (wasInsert) entity.markAsNotPersisted();
				// Drop only the events THIS write queued (from `eventFloor` on), not the
				// ones the caller queued before entering the transaction — those describe
				// work outside the rolled-back write and must survive.
				entity.restoreDomainEventsTo(eventFloor);
			});
			return;
		}
		await this.#dispatchDomainEvents(entity);
	}

	// The specific `beforeCreate`/`beforeUpdate` hook is fired by `save()` BEFORE
	// `beforeSave` (Lucid order), so these branches only do the write + after-hook.
	async #runInsertBranch(entity: T, quiet = false): Promise<void> {
		await this.#insert(entity);
		if (!quiet) await fireHooks(this.#entityClass, "afterCreate", entity);
	}

	async #runUpdateBranch(entity: T, quiet = false): Promise<void> {
		await this.#update(entity);
		if (!quiet) await fireHooks(this.#entityClass, "afterUpdate", entity);
	}

	/**
	 * Insert many rows in a single multi-row INSERT. Fires beforeSave/beforeCreate
	 * on each hydrated entity, then hydrates from the RETURNING clause (postgres +
	 * sqlite) before firing afterCreate/afterSave. On mysql, falls back to N single
	 * INSERTs (documented limitation).
	 *
	 * @implements Story 30.1 + 30.5
	 */
	async createMany(
		rows: Array<Partial<Record<string, unknown>>>,
		quiet = false,
	): Promise<T[]> {
		if (rows.length === 0) return [];
		const entities: T[] = rows.map((r) => {
			const e = new this.#entityClass();
			for (const [k, v] of Object.entries(r)) {
				if (
					this.#validColumns.has(k) ||
					this.#validColumns.has(camelToSnake(k))
				) {
					const prop = this.#toProperty(k);
					e.assertMassAssignable(prop);
					e.setProp(prop, v);
				}
			}
			return e;
		});
		// All-or-nothing (Lucid parity, same as saveMany): run the batch INSERT *and*
		// its afterCreate/afterSave hooks inside ONE managed transaction, so a hook that
		// throws rolls the whole batch back. Previously #persistFreshBatch ran the insert
		// then the after-hooks with no surrounding transaction, so a failing after-hook
		// left the rows committed while createMany rejected. The built entities are
		// internal (returned only on success), so — unlike saveMany, whose instances the
		// caller keeps — no rollback-restore of caller state is needed; the nested-under-
		// external case is already handled by #inManagedTx's tracker.
		return this.#inManagedTx(
			(repo) => repo.#persistFreshBatch(entities, quiet),
			(result) => result,
		);
	}

	/**
	 * Persist a batch of NEW entity INSTANCES: fire create/save hooks, batch-INSERT
	 * (multi-row RETURNING; mysql falls back to N inserts in one managed tx), fire
	 * the after hooks, wire the repo ref, and dispatch domain events (unless
	 * deferred). Shared by `createMany` (which builds instances from rows) and
	 * `saveMany` (which passes the CALLER's own fresh instances) so hook mutations
	 * and hook-generated domain events always land on the exact objects the caller
	 * holds — never on discarded clones.
	 */
	async #persistFreshBatch(entities: T[], quiet: boolean): Promise<T[]> {
		if (entities.length === 0) return [];
		if (!quiet) {
			for (const e of entities) {
				await fireHooks(this.#entityClass, "beforeCreate", e);
				await fireHooks(this.#entityClass, "beforeSave", e);
			}
		}

		if (this.#dialect === "mysql") {
			// mysql has no multi-row RETURNING, so insert row-by-row — but inside a
			// single managed transaction so the batch is all-or-nothing (Lucid parity;
			// a mid-batch failure must not leave a partial insert committed).
			await transaction(this.#db, async (trx) => {
				const r = this.useTransaction(trx);
				for (const e of entities) await r.#insert(e);
			});
		} else {
			const specRows = entities.map((e) => this.#entityToRowPairs(e));
			const spec = {
				kind: "insert",
				table: this.#tableName,
				rows: specRows,
				casts: this.#castTypes,
				returning: [
					this.#dbColumn(this.#primaryKey),
					...this.#columns.map((c) => this.#dbColumn(c)),
				],
			};
			const compiled = compileStatementNative(spec, this.#dialect);
			const returned = await this.#db.query<Record<string, unknown>>(
				compiled.statements[0],
				compiled.params,
			);
			returned.forEach((row, i) => {
				for (const [k, v] of Object.entries(row)) {
					const prop = this.#columnByDbName.get(k) ?? snakeToCamel(k);
					// Run the DB value through consume so date columns come back as
					// Chronos DateTime (not the raw ISO string) — mirrors #hydrate.
					entities[i].setProp(prop, this.#applyConsume(prop, v, entities[i]));
				}
				entities[i].markAsPersisted();
			});
		}

		if (!quiet) {
			for (const e of entities) {
				await fireHooks(this.#entityClass, "afterCreate", e);
				await fireHooks(this.#entityClass, "afterSave", e);
			}
		}
		for (const e of entities) this.#attachRepoRef(e);
		// Record the fresh inserts on THIS repo (the mysql path ran #insert on a nested
		// trx repo, so track here uniformly for both dialects) so the owning managed
		// batch can revert them on rollback.
		for (const e of entities) this.#trackInsert(e);
		for (const e of entities) await this.#dispatchOrDefer(e, true);
		return entities;
	}

	/** {@link createMany} without firing lifecycle hooks (AdonisJS Lucid `createManyQuietly`). */
	createManyQuietly(
		rows: Array<Partial<Record<string, unknown>>>,
	): Promise<T[]> {
		return this.createMany(rows, true);
	}

	/**
	 * Persist many already-constructed entity instances. Same hooks + batching
	 * as `createMany`, but accepts prebuilt entities so dirty tracking works.
	 *
	 * @implements Story 30.5
	 */
	async saveMany(entities: T[]): Promise<T[]> {
		if (entities.length === 0) return [];
		// All-or-nothing, like Lucid: `createMany` and every batch helper run in a
		// managed transaction, so a mid-batch failure rolls the WHOLE batch back
		// (verified against the Lucid CRUD docs). Fresh inserts AND dirty updates
		// commit together or not at all — previously the dirty ones were saved one
		// by one OUTSIDE any transaction, leaving earlier rows persisted on a later
		// failure. Events flush post-commit via #inManagedTx (deferred inside).
		// #inManagedTx re-points each returned entity's REPO_REF at the durable repo
		// after commit, so related()/refresh() work on the instances we hand back.
		// Split BEFORE the batch so the rollback path still knows which were fresh
		// (once #persistFreshBatch runs markAsPersisted, the flag flips). Classify by
		// `$isPersisted`, NOT by an empty `$original`: an aggregate/alias PROJECTION is
		// hydrated persisted but with `$original = {}`, so the old empty-$original test
		// misrouted it into the fresh INSERT batch — bypassing save()'s
		// E_MISSING_PRIMARY_KEY guard and turning a keyless projection into an INSERT.
		// A persisted projection now lands in `dirty` → save() → the guard fires.
		const fresh: T[] = [];
		const dirty: T[] = [];
		for (const e of entities) {
			if (!e.$isPersisted) fresh.push(e);
			else dirty.push(e);
		}
		// Snapshot each caller instance's domain-event floor BEFORE the batch, so a
		// rollback drops only the events this batch queued, keeping any the caller
		// queued earlier (#8).
		const eventFloors = new Map<BaseEntity, number>();
		for (const e of entities) eventFloors.set(e, e.domainEventCount());
		try {
			return await this.#inManagedTx(
				async (repo) => {
					// Persist the caller's OWN fresh instances (not clones): hook mutations
					// and hook-generated domain events stay on the objects we return.
					if (fresh.length > 0) await repo.#persistFreshBatch(fresh, false);
					for (const d of dirty) await repo.save(d);
					return entities;
				},
				(result) => result,
				eventFloors,
			);
		} catch (err) {
			// Rollback recovery. Re-point every instance's REPO_REF at the durable repo
			// (it was stamped at the now-finished trx) — Lucid resets `$trx` the same
			// way. And REVERT the FRESH instances to not-persisted: their INSERT was
			// rolled back, so keeping `$isPersisted` (Lucid does) would let a later
			// `fresh.related('x').create()` skip re-saving the parent and write a child
			// with a phantom foreign key. Reverting only the FRESH ones (provably
			// unpersisted before the batch) is a NAMED safety deviation; DIRTY rows
			// keep `$isPersisted` — their row still exists with its rolled-back values.
			for (const e of entities) this.#attachRepoRef(e);
			for (const e of fresh) e.markAsNotPersisted();
			// Drop the events THIS batch queued (from each instance's pre-batch floor):
			// the whole batch rolled back, so those describe writes that didn't happen.
			// Leaving them would double-publish when the caller re-saves the same
			// instance (its hooks re-queue the event). Events queued BEFORE the batch
			// survive (#8) — they describe work outside this rolled-back batch.
			for (const e of entities)
				e.restoreDomainEventsTo(eventFloors.get(e) ?? 0);
			throw err;
		}
	}

	/**
	 * Dialect-aware upsert. postgres + sqlite emit `ON CONFLICT DO UPDATE`; mysql
	 * emits `ON DUPLICATE KEY UPDATE`. Empty `updateColumns` = DO NOTHING.
	 *
	 * @implements Story 30.4
	 */
	async upsert(
		data: Record<string, unknown> | Array<Record<string, unknown>>,
		conflictColumns: string[],
		updateColumns: string[] = [],
	): Promise<number> {
		const rowsArr = Array.isArray(data) ? data : [data];
		const rows = rowsArr.map((r) => this.#plainToRowPairs(r));
		const spec = {
			kind: "upsert",
			table: this.#tableName,
			rows,
			conflictColumns: conflictColumns.map((c) => this.#resolveColumn(c)),
			updateColumns: updateColumns.map((c) => this.#resolveColumn(c)),
			casts: this.#castTypes,
		};
		const compiled = compileStatementNative(spec, this.#dialect);
		const result = await this.#db.execute(
			compiled.statements[0],
			compiled.params,
		);
		return result.rowsAffected;
	}

	/**
	 * Find a row matching `search` or create one merged with `defaults`.
	 *
	 * @implements Story 30.6
	 */
	async firstOrCreate(
		search: Record<string, unknown>,
		defaults: Record<string, unknown> = {},
	): Promise<T> {
		// Atomic (AdonisJS Lucid parity): find-under-lock then create inside one
		// transaction, so two concurrent callers can't both miss and both INSERT.
		return this.#inManagedTx(
			async (repo) => {
				const existing = await repo.#findBySearch(search, true);
				if (existing) return existing;
				return repo.create({ ...search, ...defaults });
			},
			(r) => [r],
		);
	}

	/**
	 * Run `body` inside an atlas-managed transaction whose trx-bound repo DEFERS
	 * domain-event dispatch, then flush the collected entities' events AFTER the
	 * commit — so a rollback emits no events for rows that were rolled back
	 * (previously each create/save dispatched in-loop, before the batch committed).
	 * `collect` picks the entities whose events flush post-commit.
	 */
	async #inManagedTx<R>(
		body: (repo: BaseRepository<T>) => Promise<R>,
		collect: (result: R) => BaseEntity[],
		eventFloors?: ReadonlyMap<BaseEntity, number>,
	): Promise<R> {
		// Records every fresh INSERT the body performs through the trx-bound repo, so
		// we can revert exactly those (not the found-and-updated rows) on rollback.
		const freshInserts: BaseEntity[] = [];
		const result = await transaction(this.#db, async (trx) => {
			const repo = this.useTransaction(trx);
			repo.#deferDomainEvents = true;
			repo.#insertTracker = freshInserts;
			const r = await body(repo);
			// Flush AFTER the transaction is durable. Registering on the trx (rather
			// than awaiting after `transaction(...)` returns) is what makes this
			// correct inside an EXTERNAL transaction: there `transaction()` only
			// opens a SAVEPOINT, so a post-return flush would fire before the outer
			// commit — and emit events for rows a later outer rollback discards.
			trx.after("commit", async () => {
				for (const e of collect(r)) await this.#dispatchDomainEvents(e);
			});
			return r;
		});
		// Every entity produced here was created / hydrated through the trx-bound
		// repo, so its REPO_REF points at the (now-finished inner) transaction. Re-point
		// it at `this` so related()/refresh()/fresh() work on the returned instance —
		// covers firstOrCreate/updateOrCreate/*Many/saveMany.
		const produced = collect(result);
		for (const e of produced) this.#attachRepoRef(e);
		// When we ran NESTED inside an external transaction, `this.#db` is that outer
		// trx and the re-attach above pointed REPO_REF at the outer-trx repo (correct
		// while still inside it). But the inner SAVEPOINT's RELEASE is NOT durable — the
		// root can still roll back. Lucid resets `$trx` once the transaction it was bound
		// to resolves, either way, so re-point REPO_REF at the durable repo on BOTH the
		// outer commit and the outer rollback; otherwise the ref dangles on a finished
		// transaction ("transaction already finished") on any later related()/refresh().
		// AND on rollback, revert the rows that were FRESHLY INSERTED (the tracker proves
		// exactly which — found-and-updated rows still exist and stay persisted): keeping
		// $isPersisted on a row that no longer exists would let a later related().create()
		// skip re-saving the parent and orphan the FK (named data-integrity deviation, now
		// closed for the nested managed path too — freshness is proven, no longer at Lucid
		// parity as in the initial R21 pass).
		if (isTransactionClient(this.#db)) {
			const durable = this.#durableParent ?? this;
			this.#db.after("commit", () => {
				for (const e of produced) durable.#attachRepoRef(e);
			});
			this.#db.after("rollback", () => {
				for (const e of produced) {
					durable.#attachRepoRef(e);
					// Every produced entity was written (inserted OR updated) in the
					// rolled-back trx, so any event THIS batch queued describes a write that
					// never committed — drop it (else a later re-save double-publishes: a
					// beforeUpdate hook on a found+updated row is the canonical trigger).
					// Restore to the caller's pre-batch floor so events queued BEFORE the
					// batch (e.g. a caller's manual addDomainEvent) survive; absent a floor
					// the entity was tx-internal (floor 0 = clear).
					e.restoreDomainEventsTo(eventFloors?.get(e) ?? 0);
				}
				// Fresh inserts additionally revert to $isNew — their row is gone.
				// Found+updated rows keep $isPersisted (their row still exists).
				// (restoreDomainEventsTo is idempotent — safe even if a fresh insert is not
				// among `produced`, e.g. an internal side-write not returned by collect.)
				for (const e of freshInserts) {
					e.markAsNotPersisted();
					e.restoreDomainEventsTo(eventFloors?.get(e) ?? 0);
				}
			});
		}
		return result;
	}

	/** Find a row or build an in-memory instance without persisting. */
	async firstOrNew(
		search: Record<string, unknown>,
		defaults: Record<string, unknown> = {},
	): Promise<T> {
		const existing = await this.#findBySearch(search);
		if (existing) return existing;
		const e = new this.#entityClass();
		for (const [k, v] of Object.entries({ ...search, ...defaults })) {
			if (
				this.#validColumns.has(k) ||
				this.#validColumns.has(camelToSnake(k))
			) {
				const prop = this.#toProperty(k);
				e.assertMassAssignable(prop);
				e.setProp(prop, v);
			}
		}
		return e;
	}

	/** Atomic find-or-update-or-insert (AdonisJS Lucid parity — locked + transactional). */
	async updateOrCreate(
		search: Record<string, unknown>,
		values: Record<string, unknown>,
	): Promise<T> {
		return this.#inManagedTx(
			async (repo) => {
				const existing = await repo.#findBySearch(search, true);
				if (existing) {
					for (const [k, v] of Object.entries(values)) {
						const prop = this.#toProperty(k);
						existing.assertMassAssignable(prop);
						existing.setProp(prop, v);
					}
					await repo.save(existing);
					return existing;
				}
				return repo.create({ ...search, ...values });
			},
			(r) => [r],
		);
	}

	/** Extract the search clause (the unique key column(s)) from a row. */
	#pickKeys(
		row: Record<string, unknown>,
		key: string | string[],
	): Record<string, unknown> {
		const keys = Array.isArray(key) ? key : [key];
		const search: Record<string, unknown> = {};
		for (const k of keys) {
			// The predicate key AND the row may each be a TS property or a DB column
			// name. Normalise both to the property so `updateOrCreateMany('label', [{
			// full_label: 'x' }])` matches — mirrors the create() key normalization.
			const prop = this.#toProperty(k);
			const dbName = this.#dbColumn(prop);
			let value: unknown;
			if (k in row) value = row[k];
			else if (prop in row) value = row[prop];
			else value = row[dbName];
			search[prop] = value;
		}
		return search;
	}

	/**
	 * Bulk find-or-update-or-insert, keyed by a unique column (or columns), in ONE
	 * transaction — all-or-nothing (AdonisJS Lucid `updateOrCreateMany`).
	 */
	async updateOrCreateMany(
		key: string | string[],
		rows: Array<Record<string, unknown>>,
	): Promise<T[]> {
		if (rows.length === 0) return [];
		return this.#inManagedTx(
			async (repo) => {
				const out: T[] = [];
				for (const row of rows) {
					const existing = await repo.#findBySearch(
						this.#pickKeys(row, key),
						true,
					);
					if (existing) {
						for (const [k, v] of Object.entries(row)) {
							const prop = this.#toProperty(k);
							existing.assertMassAssignable(prop);
							existing.setProp(prop, v);
						}
						await repo.save(existing);
						out.push(existing);
					} else {
						out.push(await repo.create(row));
					}
				}
				return out;
			},
			(out) => out,
		);
	}

	/**
	 * Bulk find-or-create keyed by a unique column(s) — existing rows are returned
	 * untouched — in one transaction (AdonisJS Lucid `fetchOrCreateMany`).
	 */
	async fetchOrCreateMany(
		key: string | string[],
		rows: Array<Record<string, unknown>>,
	): Promise<T[]> {
		if (rows.length === 0) return [];
		return this.#inManagedTx(
			async (repo) => {
				const out: T[] = [];
				for (const row of rows) {
					const existing = await repo.#findBySearch(
						this.#pickKeys(row, key),
						true,
					);
					out.push(existing ?? (await repo.create(row)));
				}
				return out;
			},
			(out) => out,
		);
	}

	/**
	 * Bulk find-or-new keyed by a unique column(s): existing rows are returned,
	 * misses become UNPERSISTED in-memory instances (AdonisJS `fetchOrNewUpMany`).
	 */
	async fetchOrNewUpMany(
		key: string | string[],
		rows: Array<Record<string, unknown>>,
	): Promise<T[]> {
		const out: T[] = [];
		for (const row of rows) {
			out.push(await this.firstOrNew(this.#pickKeys(row, key), row));
		}
		return out;
	}

	async #findBySearch(
		search: Record<string, unknown>,
		lock = false,
	): Promise<T | null> {
		let q = this.query();
		for (const [k, v] of Object.entries(search)) q = q.where(k, v);
		// `forUpdate()` row-locks the matched row so a concurrent updateOrCreate
		// serializes behind it (no-op on SQLite, which serializes writes anyway).
		if (lock) q = q.forUpdate();
		return q.first();
	}

	/**
	 * Apply `@Column({ prepare })` (model → DB) when declared. Adonis Lucid's
	 * contract — callback receives the raw value (including null/undefined) and
	 * decides what to do with it.
	 */
	#applyPrepare(key: string, value: unknown, model?: unknown): unknown {
		// Callers may pass a DB column name (e.g. updateWhere("starts_at", …) or a
		// `@Column({ columnName })` column) — prepare/dateColumns are keyed by the TS
		// property, so normalise via the reverse map first, else the adapter/date
		// conversion is silently skipped.
		const propertyKey = this.#columnByDbName.get(key) ?? key;
		const prepare = this.#columnPrepares.get(propertyKey);
		if (prepare) {
			let result: unknown;
			try {
				// Adonis Lucid signature: (value, attribute, model). `model` is
				// undefined on query-builder paths that carry no instance.
				result = prepare(value, propertyKey, model);
			} catch (err) {
				throw wrapAdapterError("prepare", propertyKey, err);
			}
			assertNotPromise("prepare", propertyKey, result);
			return result;
		}
		// No explicit `@Column({ prepare })`: lower a `@column.date()` /
		// `@column.dateTime()` value to its ISO 8601 string for the SQL bind. A raw
		// JS `Date` is accepted leniently; otherwise the Chronos adapter's prepare
		// serialises a `DateTime` — via a STRUCTURAL check, so an instance from a
		// duplicated `@c9up/chronos` copy (another realm) round-trips instead of
		// being passed raw to the N-API bind.
		if (this.#dateColumns[propertyKey] && value != null) {
			if (value instanceof Date) return value.toISOString();
			return dateTimeAtlasAdapter.prepare(value);
		}
		return value;
	}

	#applyConsume(propertyKey: string, value: unknown, model?: unknown): unknown {
		const consume = this.#columnConsumes.get(propertyKey);
		if (consume) {
			let result: unknown;
			try {
				// Adonis Lucid signature: (value, attribute, model).
				result = consume(value, propertyKey, model);
			} catch (err) {
				throw wrapAdapterError("consume", propertyKey, err);
			}
			assertNotPromise("consume", propertyKey, result);
			return result;
		}
		// No explicit `@Column({ consume })`: a `@column.date()` / `@column.dateTime()`
		// column hydrates its DB value into a Chronos `DateTime` — mirroring Adonis
		// Lucid, which hydrates date columns to a Luxon `DateTime` (here the Ream
		// date engine `@c9up/chronos` plays Luxon's role). The Chronos adapter's
		// consume is idempotent and uses a structural check, so a `DateTime` from a
		// different realm (duplicated package copy) is recognised too.
		if (this.#dateColumns[propertyKey] && value != null) {
			return dateTimeAtlasAdapter.consume(value);
		}
		return value;
	}

	#plainToRowPairs(obj: Record<string, unknown>): Array<[string, unknown]> {
		const pairs: Array<[string, unknown]> = [];
		for (const [k, v] of Object.entries(obj)) {
			// Skip explicit `undefined` so we don't emit `undefined` as a SQL bind —
			// the Rust DML compiler / NAPI layer rejects it. `null` is allowed
			// through because that's a meaningful SQL value.
			if (v === undefined) continue;
			// `#applyPrepare` normalises the key (property / snake / columnName) via
			// the reverse map, so pass the raw key straight through.
			pairs.push([this.#resolveColumn(k), this.#applyPrepare(k, v)]);
		}
		return pairs;
	}

	#entityToRowPairs(entity: T): Array<[string, unknown]> {
		const pairs: Array<[string, unknown]> = [];
		for (const col of this.#columns) {
			const v = entity[col];
			if (v !== undefined)
				pairs.push([this.#dbColumn(col), this.#applyPrepare(col, v)]);
		}
		return pairs;
	}

	/** Delete the entity. Fires `beforeDelete` → DB → `afterDelete`. Soft-delete aware. */
	async delete(entity: T, quiet = false): Promise<void> {
		// Guard BEFORE hooks — a projection entity with no PK must not fire
		// beforeDelete against a phantom row, then delete WHERE pk IS NULL.
		this.#assertPersistedRow(entity, entity[this.#primaryKey], "delete()");
		if (!quiet) await fireHooks(this.#entityClass, "beforeDelete", entity);
		const pk = entity[this.#primaryKey];
		if (this.#softDeletes) {
			const now = DateTime.now();
			await this.#runUpdate(
				[[this.#dbColumn("deletedAt"), now.toISO()]],
				[{ column: this.#primaryKey, operator: "=", value: pk, type: "and" }],
			);
			// In-memory value is a Chronos DateTime, matching how date columns hydrate.
			entity.setProp("deletedAt", now);
		} else {
			await this.#runDelete([
				{ column: this.#primaryKey, operator: "=", value: pk, type: "and" },
			]);
		}
		entity.markAsDeleted();
		if (!quiet) await fireHooks(this.#entityClass, "afterDelete", entity);
	}

	/** {@link delete} without firing lifecycle hooks (AdonisJS Lucid `deleteQuietly`). */
	deleteQuietly(entity: T): Promise<void> {
		return this.delete(entity, true);
	}

	/** Permanently delete (bypasses soft delete). Fires `beforeDelete` / `afterDelete` hooks. */
	async forceDelete(entity: T): Promise<void> {
		this.#assertPersistedRow(entity, entity[this.#primaryKey], "forceDelete()");
		await fireHooks(this.#entityClass, "beforeDelete", entity);
		await this.#runDelete([
			{
				column: this.#primaryKey,
				operator: "=",
				value: entity[this.#primaryKey],
				type: "and",
			},
		]);
		entity.markAsDeleted();
		await fireHooks(this.#entityClass, "afterDelete", entity);
	}

	async restore(entity: T): Promise<void> {
		if (!this.#softDeletes) return;
		this.#assertPersistedRow(entity, entity[this.#primaryKey], "restore()");
		await this.#runUpdate(
			[[this.#dbColumn("deletedAt"), null]],
			[
				{
					column: this.#primaryKey,
					operator: "=",
					value: entity[this.#primaryKey],
					type: "and",
				},
			],
		);
		entity.setProp("deletedAt", null);
	}

	// ─── Bulk updates ─────────────────────────────────────────

	async updateById(
		id: string | number,
		data: Partial<Record<string, unknown>>,
	): Promise<void> {
		const set = this.#buildSetPairs(data);
		await this.#runUpdate(set, [
			{ column: this.#primaryKey, operator: "=", value: id, type: "and" },
		]);
	}

	async updateWhere(
		column: string,
		columnValue: unknown,
		data: Partial<Record<string, unknown>>,
	): Promise<void> {
		const whereCol = this.#resolveColumn(column);
		const set = this.#buildSetPairs(data);
		await this.#runUpdate(set, [
			{
				column: whereCol,
				operator: "=",
				// Prepare the filter value like the query()/where() path (DateTime→ISO,
				// @Column adapters) so updateWhere matches query().where().update().
				value: this.#applyPrepare(column, columnValue),
				type: "and",
			},
		]);
	}

	/**
	 * Atomically increment one or more columns on a single row.
	 * Emits `UPDATE … SET col = col + ? WHERE pk = ?` — no read-modify-write,
	 * safe under concurrent updates.
	 *
	 *     await repo.increment(userId, 'views', 1)
	 *     await repo.increment(userId, { balance: 10, credits: 5 })
	 *
	 * @implements Story 30.3
	 */
	increment(
		id: string | number,
		column: string,
		amount?: number,
	): Promise<void>;
	increment(
		id: string | number,
		columns: Record<string, number>,
	): Promise<void>;
	async increment(
		id: string | number,
		columnOrMap: string | Record<string, number>,
		amount = 1,
	): Promise<void> {
		const set = this.#buildIncrementPairs(columnOrMap, amount, "increment");
		await this.#runUpdate(set, [
			{ column: this.#primaryKey, operator: "=", value: id, type: "and" },
		]);
	}

	/** Symmetrical to `increment` — emits `SET col = col - ?`. */
	decrement(
		id: string | number,
		column: string,
		amount?: number,
	): Promise<void>;
	decrement(
		id: string | number,
		columns: Record<string, number>,
	): Promise<void>;
	async decrement(
		id: string | number,
		columnOrMap: string | Record<string, number>,
		amount = 1,
	): Promise<void> {
		const set = this.#buildIncrementPairs(columnOrMap, amount, "decrement");
		await this.#runUpdate(set, [
			{ column: this.#primaryKey, operator: "=", value: id, type: "and" },
		]);
	}

	// ─── Raw ──────────────────────────────────────────────────

	async raw(sql: string, ...params: unknown[]): Promise<T[]> {
		// Strict mode hardens the repository's raw surfaces (parity with
		// whereRaw/joinRaw/havingRaw): `raw()` splices a whole hand-written SQL
		// statement into the typed repo and hydrates it, so it's the widest raw
		// entry point of all. Block it and point at the connection-level break-glass
		// (`db.query()`/`db.execute()`, explicitly parameterised) — that stays the
		// sanctioned, greppable escape hatch, never a silent bypass of strict mode.
		if (isAtlasStrictMode()) {
			throw new AtlasError(
				"E_STRICT_MODE",
				`raw() is disabled in Atlas strict mode on ${this.#entityClass.name}.`,
				{
					hint: "Use the typed query() builder, or db.query()/db.execute() with bound params for a deliberate break-glass query. Call setAtlasStrictMode(false) at bootstrap if you truly need repo.raw().",
				},
			);
		}
		const rows = await this.#db.query<Row>(sql, params);
		return rows.map((r) => this.#hydrate(r));
	}

	// ─── Accessors ────────────────────────────────────────────

	getTableName(): string {
		return this.#tableName;
	}
	getPrimaryKeyColumn(): string {
		return this.#primaryKey;
	}

	// ─── Private helpers ──────────────────────────────────────

	/**
	 * Guard for an op PREMISED on an existing DB row (refresh/fresh/delete/
	 * forceDelete/restore/load*). These require a genuine database row, so the
	 * entity must be `$isPersisted` — a locally-built instance with a manual PK is
	 * NOT a row: deleting/refreshing off it would silently hit an unrelated row (or
	 * none) and fire hooks against a hollow object. Mirrors Lucid, whose `refresh()`
	 * rejects a non-persisted instance and whose destructive ops always run on a
	 * loaded model; the extra strictness on delete/restore is a named safety
	 * deviation. A persisted-but-keyless entity (aggregate/alias projection) is also
	 * rejected, with the projection diagnostic.
	 */
	#assertPersistedRow(
		entity: BaseEntity,
		key: unknown,
		op: string,
		keyName: string = this.#primaryKey,
	): void {
		if (!entity.$isPersisted) {
			throw new AtlasError(
				"E_MODEL_NOT_PERSISTED",
				`Cannot ${op} a ${this.#entityClass.name} that is not persisted.`,
				{
					hint: "Load it from the database (find/query) first — a locally-built instance with a manual primary key is not a database row.",
				},
			);
		}
		if (!isProvidedPk(key)) {
			// Name the ACTUAL missing key — a relation with a custom `localKey` isn't
			// missing its primary key, it's missing that local key ('code', …).
			const isPk = keyName === this.#primaryKey;
			throw new AtlasError(
				"E_MISSING_PRIMARY_KEY",
				`Cannot ${op} a ${this.#entityClass.name} loaded without its ${isPk ? "primary key" : "key"} ('${keyName}').`,
				{
					hint: "This entity came from an aggregate/alias projection. Select the key or use query().pojo() for projections.",
				},
			);
		}
	}

	async #runDelete(wheres: Array<Record<string, unknown>>): Promise<void> {
		const compiled = compileStatementNative(
			{ kind: "delete", table: this.#tableName, wheres },
			this.#dialect,
		);
		await this.#db.execute(compiled.statements[0], compiled.params);
	}

	/**
	 * Emit an UPDATE. Each entry in `set` is either `[col, rawValue]` (plain
	 * binding — `SET col = ?`) or `[col, { op: 'increment' | 'decrement', value }]`
	 * (atomic expression — `SET col = col ± ?`). The Rust compiler picks the
	 * right SQL via `SetValue::Value` / `SetValue::Expression`.
	 */
	async #runUpdate(
		set: Array<
			[string, unknown | { op: "increment" | "decrement"; value: unknown }]
		>,
		wheres: Array<Record<string, unknown>>,
	): Promise<void> {
		if (set.length === 0) return;
		const compiled = compileStatementNative(
			{
				kind: "update",
				table: this.#tableName,
				set,
				wheres,
				casts: this.#castTypes,
			},
			this.#dialect,
		);
		await this.#db.execute(compiled.statements[0], compiled.params);
	}

	/**
	 * Execute the INSERT and return whatever the engine surfaces about the
	 * fresh row: the RETURNING projection (postgres / sqlite) when available,
	 * otherwise just the rowsAffected count (mysql doesn't support RETURNING).
	 * Caller decides how much to rehydrate.
	 */
	async #runInsert(values: Array<[string, unknown]>): Promise<InsertOutcome> {
		if (values.length === 0) return {};
		const supportsReturning = this.#dialect !== "mysql";
		const spec = supportsReturning
			? {
					kind: "insert",
					table: this.#tableName,
					values,
					casts: this.#castTypes,
					returning: [
						this.#dbColumn(this.#primaryKey),
						...this.#columns.map((c) => this.#dbColumn(c)),
					],
				}
			: {
					kind: "insert",
					table: this.#tableName,
					values,
					casts: this.#castTypes,
				};
		const compiled = compileStatementNative(spec, this.#dialect);
		if (supportsReturning) {
			const rows = await this.#db.query<Row>(
				compiled.statements[0],
				compiled.params,
			);
			const first = rows[0];
			return first ? { row: first } : {};
		}
		await this.#db.execute(compiled.statements[0], compiled.params);
		// MySQL path: napi adapter doesn't surface lastInsertRowid through
		// `execute()`. Callers that need it must use an explicit dialect-
		// specific query (e.g. `SELECT LAST_INSERT_ID()`). For Atlas's
		// public surface, the entity carries the PK already (either set
		// by the caller or generated client-side as a UUID).
		return {};
	}

	async #insert(entity: T): Promise<void> {
		// Auto-generate the PK when declared via `@PrimaryKey({ generated })`.
		this.#applyPrimaryKeyGenerator(entity);
		// Auto-populate @column.dateTime({ autoCreate: true }) fields before building the row.
		this.#applyAutoTimestamps(entity, "insert");
		const data = this.#entityToRow(entity);
		const result = await this.#runInsert(Object.entries(data));
		// Hydrate DB-generated values (auto-increment ids, default columns) so
		// callers see them on the entity without an extra `find()`. Mirrors
		// `createMany`, where the multi-row path already does this.
		if (result.row) {
			for (const [k, v] of Object.entries(result.row)) {
				const prop = this.#columnByDbName.get(k) ?? snakeToCamel(k);
				// Consume so date columns hydrate to Chronos DateTime, not raw ISO.
				entity.setProp(prop, this.#applyConsume(prop, v, entity));
			}
		} else if (
			result.lastInsertRowid !== undefined &&
			!isProvidedPk(entity[this.#primaryKey])
		) {
			entity.setProp(this.#primaryKey, normalizeRowid(result.lastInsertRowid));
		}
		// After a successful INSERT, the entity is now persisted — snapshot
		// its columns so subsequent dirty checks compare against the DB state.
		entity.markAsPersisted();
		this.#trackInsert(entity);
	}

	/**
	 * UPDATE the entity — emits only the dirty columns (story 32.2).
	 *
	 * If no column is dirty, skips the query entirely (common case when a
	 * `save()` is called defensively without any real mutation).
	 */
	async #update(entity: T): Promise<void> {
		const forced = entity.$consumeForceUpdate();
		const pk = entity[this.#primaryKey];

		// Compute the REAL dirt BEFORE stamping autoUpdate, so a genuinely-clean
		// save() is a no-op — no `updated_at` bump, no query (AdonisJS Lucid parity;
		// stamping first would make every save() on an autoUpdate model dirty).
		const preDirty = entity.$dirty;
		delete preDirty[this.#primaryKey];
		if (Object.keys(preDirty).length === 0 && !forced) return; // nothing changed

		// A real change (or a forced update) is happening — now stamp
		// @column.dateTime({ autoUpdate: true }) so it lands in the SET.
		this.#applyAutoTimestamps(entity, "update");
		const dirty = entity.$dirty;
		// Primary key is never part of the SET — it's the WHERE.
		delete dirty[this.#primaryKey];

		// Map dirty camelCase keys to snake_case DB columns. `$dirty` keys are
		// already camelCase (they come from `entity.setProp` / direct assignment),
		// so the prepare lookup uses `k` as-is. Skip explicit `undefined`
		// assignments to mirror `#buildSetPairs` / `#plainToRowPairs` — the
		// Rust DML compiler / NAPI layer rejects `undefined` binds.
		const setPairs: Array<[string, unknown]> = [];
		for (const [k, v] of Object.entries(dirty)) {
			if (v === undefined) continue;
			setPairs.push([this.#dbColumn(k), this.#applyPrepare(k, v)]);
		}
		// enableForceUpdate() with nothing dirty: re-persist the current non-PK
		// column values so an UPDATE still runs (fires triggers / bumps autoUpdate).
		if (setPairs.length === 0 && forced) {
			for (const col of this.#columns) {
				if (col === this.#primaryKey) continue;
				const v = entity[col];
				if (v !== undefined)
					setPairs.push([this.#dbColumn(col), this.#applyPrepare(col, v)]);
			}
		}
		if (setPairs.length === 0) {
			// All dirty entries were `undefined` (skipped above). Re-snapshot
			// anyway: without this, `$dirty` keeps reporting the same
			// undefined keys forever and a caller checking `entity.isDirty()`
			// loops on a no-op save.
			entity.markAsPersisted();
			return;
		}

		await this.#runUpdate(setPairs, [
			{ column: this.#primaryKey, operator: "=", value: pk, type: "and" },
		]);
		// Re-snapshot after a successful UPDATE.
		entity.markAsPersisted();
	}

	/**
	 * Generate the primary key on INSERT when the entity declares
	 * `@PrimaryKey({ generated: 'uuid' })` and no value is set. Caller-supplied
	 * PKs win — we only fill in when the field is `undefined`.
	 */
	#applyPrimaryKeyGenerator(entity: T): void {
		const strategy: PrimaryKeyGenerator | undefined = getPrimaryKeyGenerator(
			this.#entityClass,
		);
		if (!strategy) return;
		if (entity[this.#primaryKey] !== undefined) return;
		if (strategy === "uuid") {
			entity.setProp(this.#primaryKey, randomUUID());
		}
	}

	/**
	 * Apply auto-timestamp columns (`@column.dateTime({ autoCreate, autoUpdate })`)
	 * on the entity before persistence. Called from `#insert` and `#update`.
	 */
	#applyAutoTimestamps(entity: T, phase: "insert" | "update"): void {
		// A Chronos `DateTime` (not a JS `Date`) so `autoCreate`/`autoUpdate` values
		// match the type `@column.dateTime` columns hydrate to — Adonis Lucid parity.
		const now = DateTime.now();
		for (const [prop, cfg] of Object.entries(this.#dateColumns)) {
			if (phase === "insert") {
				if (cfg.autoCreate && entity[prop] === undefined) {
					entity.setProp(prop, now);
				}
				if (cfg.autoUpdate && entity[prop] === undefined) {
					entity.setProp(prop, now);
				}
			} else if (phase === "update" && cfg.autoUpdate) {
				entity.setProp(prop, now);
			}
		}
	}

	#hydrate(row: Record<string, unknown>): T {
		const entity = new this.#entityClass();
		for (const [key, value] of Object.entries(row)) {
			// Resolve against declared column metadata, not `in entity` — fields
			// using Adonis' `declare field: T` pattern are not own-properties of
			// a freshly constructed instance. The reverse db→property map is
			// consulted first so an explicit `columnName` override resolves to the
			// right property (where `snakeToCamel` alone would not).
			const camelKey = snakeToCamel(key);
			const targetKey =
				this.#columnByDbName.get(key) ??
				(this.#validColumns.has(camelKey)
					? camelKey
					: this.#validColumns.has(key)
						? key
						: null);
			if (!targetKey) continue;
			// Apply `@Column({ consume })` if declared on this property. Unlike the
			// previous registry-based design, the callback receives every value
			// including `null` / `undefined` — the user's `consume` is responsible
			// for its own null-handling, matching Adonis Lucid's contract.
			entity.setProp(targetKey, this.#applyConsume(targetKey, value, entity));
		}
		// Freeze the original snapshot — from now on, only columns changed AFTER
		// hydration are considered dirty by `entity.$dirty`.
		entity.markAsPersisted();
		entity.markAsFromDatabase();
		this.#attachRepoRef(entity);
		return entity;
	}

	/**
	 * Back-pointer so a persisted instance can `related()` / `refresh()` / `fresh()`
	 * / `load*()` without being re-fetched — AdonisJS Lucid parity: a model returned
	 * by find/query AND by create/save/createMany/saveMany carries its query client.
	 * Non-enumerable so it never serializes; `configurable` so re-persisting the same
	 * instance is idempotent.
	 */
	#attachRepoRef(entity: BaseEntity): void {
		Object.defineProperty(entity, REPO_REF, {
			value: this,
			enumerable: false,
			configurable: true,
		});
	}

	/**
	 * Re-read the entity's row from the database and mutate the instance in place.
	 * Used by `entity.refresh()` — not normally called directly.
	 *
	 * @implements Story 32.6
	 */
	async refresh(entity: BaseEntity): Promise<void> {
		const pk = entity[this.#primaryKey];
		this.#assertPersistedRow(entity, pk, "refresh()");
		const fresh = await this.find(pk as string | number);
		if (!fresh) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
		// Copy all column values from the fresh row onto the existing instance.
		for (const col of this.#columns) {
			entity.setProp(col, fresh[col]);
		}
		entity.setProp(this.#primaryKey, fresh[this.#primaryKey]);
		entity.markAsPersisted();
	}

	/**
	 * Re-read the entity's row and return a NEW instance (the input is untouched).
	 *
	 * @implements Story 32.6
	 */
	/**
	 * Lazy-load a relation count into `entity.$extras[alias ?? `${relationName}_count`]`.
	 * Uses `ModelQuery.withCount` with a restrictive `WHERE pk = ?` so it reads
	 * one entity's row back with the aggregate column attached.
	 *
	 * @implements Story 29.2
	 */
	async loadCount(
		entity: BaseEntity,
		relationName: string,
		alias?: string,
	): Promise<void> {
		const pk = entity[this.#primaryKey];
		this.#assertPersistedRow(entity, pk, "loadCount()");
		const finalAlias = alias ?? `${relationName}_count`;
		const q = this.query()
			.where(this.#primaryKey, pk)
			.withCount(relationName, (sub) => {
				sub.as(finalAlias);
			});
		const [refreshed] = await q.exec();
		if (refreshed) entity.setExtra(finalAlias, refreshed.getExtra(finalAlias));
	}

	/**
	 * Lazy-load a relation aggregate. The builder callback sets the aggregate via
	 * `.sum/.avg/.min/.max/.count` and the alias via `.as('name')`.
	 *
	 * @implements Story 29.2
	 */
	async loadAggregate(
		entity: BaseEntity,
		relationName: string,
		build: (q: unknown) => void,
	): Promise<void> {
		const pk = entity[this.#primaryKey];
		this.#assertPersistedRow(entity, pk, "loadAggregate()");
		let capturedAlias: string | undefined;
		const q = this.query()
			.where(this.#primaryKey, pk)
			.withAggregate(relationName, (sub) => {
				build(sub);
				capturedAlias = (sub as ModelQuery<BaseEntity>).subqueryAlias;
			});
		const [refreshed] = await q.exec();
		const alias = capturedAlias ?? relationName;
		if (refreshed) entity.setExtra(alias, refreshed.getExtra(alias));
	}

	/**
	 * Lazy-load a relation onto an already-fetched entity. Re-uses the preload
	 * resolver by running a fresh query with `.where(pk = entity.pk).preload(...)`.
	 *
	 * @implements Story 31.10
	 */
	async loadRelation(
		entity: BaseEntity,
		relationName: string,
		callback?: (q: unknown) => void,
	): Promise<void> {
		const pk = entity[this.#primaryKey];
		this.#assertPersistedRow(entity, pk, "loadRelation()");
		const q = this.query().where(this.#primaryKey, pk);
		if (callback)
			q.preload(relationName, callback as (q: ModelQuery<BaseEntity>) => void);
		else q.preload(relationName);
		const [hydrated] = await q.exec();
		if (hydrated) {
			// Copy the loaded relation onto the caller's instance.
			const value = (hydrated as Record<string, unknown>)[relationName];
			entity.setProp(relationName, value);
		}
	}

	/**
	 * Return a thin relation proxy bound to the given parent instance. Only
	 * `hasOne` / `hasMany` (and trivially `manyToMany` insert paths) are wired
	 * here; richer operations (attach/detach/sync) live in Story 31.7's proxy.
	 *
	 * @implements Story 31.5
	 */
	relatedProxy(entity: BaseEntity, relationName: string): RelationProxy {
		const relations = getRelationMetadata(this.#entityClass);
		const relation = relations.find((r) => r.propertyKey === relationName);
		if (!relation)
			throw new Error(
				`Relation '${relationName}' not found on ${this.#entityClass.name}`,
			);
		const relatedClass = relation.target() as new () => BaseEntity;
		// Synthesize the related model's @Entity metadata on demand (static `table`
		// / naming strategy) — a related model referenced ONLY through this relation
		// may never have been instantiated, so `getEntityMetadata` alone would be
		// empty and related()/create-through would wrongly fail. Mirrors how the repo
		// constructor boots its own class (AdonisJS Lucid lazy-boots models).
		const relatedTable = ensureEntityMetadata(relatedClass).tableName;
		const parentPk =
			relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
		// Read the parent's key LAZILY, at operation time — not once at proxy
		// creation. Lucid resolves the pivot value when the query runs, so mutating
		// the parent's (custom local) key between `user.related('roles')` and a later
		// `.attach()` must target the CURRENT key, never a captured stale one.
		const readParentId = (): unknown => entity[parentPk];
		const keyLabel =
			parentPk === this.#primaryKey ? "primary key" : `key '${parentPk}'`;
		const relatedRepo = new BaseRepository<BaseEntity>(relatedClass, this.#db, {
			dialect: this.#dialect,
		});
		// Propagate the domain-event sink so entities persisted through a relation
		// proxy (user.related('posts').create(...)) dispatch their events too —
		// otherwise the related entity's events silently vanish.
		relatedRepo.onDomainEvents = this.onDomainEvents;
		const db = this.#db;

		// FK column naming: belongsTo stores the FK on THIS side; has* / m2m on the OTHER side.
		const fkCol =
			relation.foreignKey ??
			(relation.type === "belongsTo"
				? `${camelToSnake(relatedClass.name)}_id`
				: `${camelToSnake(this.#entityClass.name)}_id`);
		const fkProp = snakeToCamel(fkCol);

		const injectFk = (
			data: Record<string, unknown>,
			fkValue: unknown,
		): Record<string, unknown> => ({
			...data,
			[fkCol]: fkValue,
			[fkProp]: fkValue,
		});

		/**
		 * Lucid persists the parent FIRST (inside a managed transaction) so its key
		 * is available, then sets the child FK and writes the child — atomic, rolled
		 * back on any failure. An already-persisted parent skips the save; a
		 * persisted-but-keyless projection is rejected loud. Runs `body` with the
		 * parent's now-guaranteed key and a trx-bound related repo.
		 */
		const flushEvents = async (entities: BaseEntity[]): Promise<void> => {
			for (const e of entities) await this.#dispatchDomainEvents(e);
		};
		const withParentSaved = <R>(
			body: (
				fkValue: unknown,
				relRepoTx: BaseRepository<BaseEntity>,
				trx: TransactionClient,
				relatedFloors: Map<BaseEntity, number>,
			) => Promise<R>,
		): Promise<R> =>
			transaction(this.#db, async (trx) => {
				// Snapshot BEFORE the save flips the flag — the parent's events flush
				// ONLY if WE persisted it here. An already-persisted parent may carry
				// unrelated in-memory events that belong to whoever saves it; a child
				// mutation must not emit them as a side effect.
				const savedParentHere = !entity.$isPersisted;
				const parentDurable = this.#durableParent ?? this;
				if (savedParentHere) {
					// Floor the parent's event queue BEFORE we persist it, so rollback drops
					// only the events this write queues, keeping any the caller queued
					// earlier (#8).
					const parentEventFloor = entity.domainEventCount();
					// Persist the parent on the SAME trx. Build a BaseEntity-typed repo
					// for the parent class (mirrors `relatedRepo`) so `save(entity)`
					// accepts the generic `BaseEntity` without widening `this`.
					const parentRepoTx = new BaseRepository<BaseEntity>(
						this.#entityClass,
						trx,
						{ dialect: this.#dialect },
					);
					parentRepoTx.onDomainEvents = this.onDomainEvents;
					parentRepoTx.#deferDomainEvents = true;
					await parentRepoTx.save(entity);
					// Register the parent's rollback restore IMMEDIATELY after its insert —
					// the parentPk check just below can throw (a custom `localKey` left unset
					// after the save), and that throw must still revert the freshly-inserted
					// parent instead of leaving it lying $isPersisted (same gotcha as
					// associate(): register the restore before ANY later throwable line).
					trx.after("rollback", () => {
						parentDurable.#attachRepoRef(entity);
						entity.markAsNotPersisted();
						entity.restoreDomainEventsTo(parentEventFloor);
					});
				}
				const fkValue = entity[parentPk];
				if (!isProvidedPk(fkValue)) {
					throw new AtlasError(
						"E_MISSING_PRIMARY_KEY",
						`Cannot use related('${relationName}') on a ${this.#entityClass.name} with no ${keyLabel}.`,
						{
							hint: "The parent is an aggregate/alias projection with no key. Select the key or use query().pojo().",
						},
					);
				}
				const relTx = relatedRepo.useTransaction(trx);
				relTx.#deferDomainEvents = true;
				// Track related rows inserted DIRECTLY through relTx (single create/save;
				// the batch helpers route through #inManagedTx, which tracks + reverts them
				// itself on this same trx). A caller-passed related instance we insert here
				// must, on rollback, revert to $isNew — its row is gone, and keeping
				// $isPersisted would orphan a later relation write (a M2M pivot-insert
				// failure AFTER `rel.save(related)` is the canonical trigger) — and drop its
				// queued events. On commit, re-point its REPO_REF at the durable related repo
				// (it was bound to the now-finished trx, so refresh()/related() would
				// otherwise throw "transaction already finished").
				const relInserts: BaseEntity[] = [];
				relTx.#insertTracker = relInserts;
				const relDurable = relatedRepo.#durableParent ?? relatedRepo;
				// Per-related event floor, populated by a caller-instance write (save):
				// a fresh child built by create() has floor 0 (clear), but a caller's own
				// instance passed to save() may carry events queued before the write (#8).
				const relatedFloors = new Map<BaseEntity, number>();
				trx.after("commit", () => {
					for (const r of relInserts) relDurable.#attachRepoRef(r);
				});
				trx.after("rollback", () => {
					for (const r of relInserts) {
						relDurable.#attachRepoRef(r);
						r.markAsNotPersisted();
						r.restoreDomainEventsTo(relatedFloors.get(r) ?? 0);
					}
				});
				// Parent COMMIT restore (its rollback restore is registered above, right
				// after the insert). ONLY if WE persisted it here (`parentRepoTx.save`
				// flipped it to $isPersisted with REPO_REF bound to the trx repo). Lucid
				// resets `$trx` on commit → re-point REPO_REF at the durable repo, then
				// flush the parent's events (a rollback thus publishes nothing). An
				// already-persisted parent is left untouched: its events belong to whoever
				// saves it, and its row already exists.
				if (savedParentHere) {
					trx.after("commit", () => {
						parentDurable.#attachRepoRef(entity);
						return this.#dispatchDomainEvents(entity);
					});
				}
				return body(fkValue, relTx, trx, relatedFloors);
			});

		// Shared "has" proxy methods (create/createMany/save/saveMany +
		// firstOrCreate/updateOrCreate scoped to this parent's FK). Each persists the
		// parent first (Lucid parity) and writes the child with the FK set, atomically,
		// then flushes the child's domain events AFTER the transaction commits.
		const hasOps = {
			create: (data: Record<string, unknown>) =>
				withParentSaved(async (fk, rel, trx) => {
					const child = await rel.create(injectFk(data, fk));
					trx.after("commit", () => flushEvents([child]));
					return child;
				}),
			createMany: (rows: Array<Record<string, unknown>>) =>
				withParentSaved(async (fk, rel, _trx) => {
					// NO wrapper flush: since createMany now runs through #inManagedTx it
					// ALREADY dispatches the children's events post-commit (like
					// firstOrCreate/updateOrCreate/saveMany). A second flush would
					// re-dispatch events the first hook re-queued on a partial sink failure.
					return rel.createMany(rows.map((r) => injectFk(r, fk)));
				}),
			// Scope the search to the parent's FK column so the lookup only sees this
			// parent's rows; inject the FK into the created/updated row.
			firstOrCreate: (
				search: Record<string, unknown>,
				defaults: Record<string, unknown> = {},
			) =>
				withParentSaved(async (fk, rel, _trx) => {
					// NO wrapper flush here: unlike create/save, rel.firstOrCreate goes
					// through #inManagedTx, which ALREADY registers its own post-commit
					// dispatch for the child. A second flush would re-dispatch events the
					// first hook re-queued on a partial sink failure → bus duplication.
					return rel.firstOrCreate(
						{ ...search, [fkCol]: fk },
						injectFk(defaults, fk),
					);
				}),
			updateOrCreate: (
				search: Record<string, unknown>,
				values: Record<string, unknown>,
			) =>
				withParentSaved(async (fk, rel, _trx) => {
					// NO wrapper flush: rel.updateOrCreate goes through #inManagedTx which
					// already dispatches the child's events post-commit (see firstOrCreate).
					return rel.updateOrCreate(
						{ ...search, [fkCol]: fk },
						injectFk(values, fk),
					);
				}),
			save: (related: BaseEntity) =>
				withParentSaved(async (fk, rel, trx, relatedFloors) => {
					related.setProp(fkCol, fk);
					related.setProp(fkProp, fk);
					// Floor BEFORE the write so a rollback keeps events the caller queued on
					// this instance earlier, dropping only what this save adds (#8).
					relatedFloors.set(related, related.domainEventCount());
					await rel.save(related);
					trx.after("commit", () => flushEvents([related]));
				}),
			saveMany: (related: BaseEntity[]) =>
				withParentSaved(async (fk, rel, _trx) => {
					for (const r of related) {
						r.setProp(fkCol, fk);
						r.setProp(fkProp, fk);
					}
					// NO wrapper flush: rel.saveMany now runs through #inManagedTx (it's
					// all-or-nothing), which ALREADY dispatches these instances' events
					// post-commit. A second flush would re-dispatch on a partial sink
					// failure (round-13 double-flush class).
					return rel.saveMany(related);
				}),
		};

		// Scoped query builder (Story 31.9) — pre-applies the FK predicate
		// (or pivot JOIN for m2m) so downstream filters/updates/deletes stay
		// inside the relation boundary.
		const scopedQuery = (): ModelQuery<BaseEntity> => {
			const q = relatedRepo.query();
			if (relation.type === "manyToMany") {
				if (!relation.pivot)
					throw new Error(`@ManyToMany ${relationName} requires pivot options`);
				const pivot = relation.pivot;
				const pivotFk =
					pivot.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
				const pivotOther =
					pivot.otherKey ?? `${camelToSnake(relatedClass.name)}_id`;
				// Resolve the related PK to its DB column (multi-word / columnName),
				// mirroring the eager-preload fix — a raw property name here targets
				// the wrong column in the correlated EXISTS.
				// The related-side key the pivot's otherKey references — the related
				// PK unless `relatedKey` overrides it (Adonis Lucid `relatedKey`).
				const relatedPkProp =
					pivot.relatedKey ?? getPrimaryKey(relatedClass) ?? "id";
				const relatedPk =
					getColumnMetadata(relatedClass).find(
						(c) => c.propertyKey === relatedPkProp,
					)?.columnName ?? camelToSnake(relatedPkProp);
				// Inline validated quote (same policy as the m2m branch below).
				const dialect = this.#dialect;
				const quote = (name: string): string => {
					if (!/^[A-Za-z0-9_]+$/.test(name)) {
						throw new Error(`Unsafe identifier in pivot metadata: '${name}'`);
					}
					return dialect === "mysql" ? `\`${name}\`` : `"${name}"`;
				};
				// Table identifiers may be schema-qualified (`schema.table`, e.g. a
				// Postgres `public.users_roles`) — quote each dotted segment on its own
				// so it becomes `"schema"."table"`, while EVERY segment still passes the
				// strict single-identifier guard above (no injection surface). Columns
				// stay single-segment via `quote`.
				const quoteTable = (name: string): string =>
					name.split(".").map(quote).join(".");
				// The bound `?` carries the parent PK type (often uuid). A raw `?`
				// can't be cast by the structured `casts` mechanism, so emit the
				// `::uuid` inline — `whereRaw` rewrites `?`→`$N`, yielding `$N::uuid`.
				// Postgres-only; sqlite/mysql coerce. Without it: `pivotFk = $N` is
				// `uuid = text`.
				// Cast keys off the RESOLVED parent key (localKey ?? PK), not always the
				// PK — an m2m with a custom localKey binds `entity[localKey]` into the
				// pivot FK, so the `::cast` must match that column's type.
				const parentPkCast = this.#castTypes[this.#dbColumn(parentPk)];
				const ph =
					dialect === "postgres" && parentPkCast ? `?::${parentPkCast}` : "?";
				// EXISTS (SELECT 1 FROM pivot WHERE pivot.pivotFk = ? AND pivot.pivotOther = related.pk
				//         [AND pivot.col <op> ?]…)
				// Deferred (not an eager whereRaw): a `.wherePivot()` chained on the
				// query the proxy hands back must fold into THIS subquery, so we build it
				// at #buildSpec time with the pivot constraints known then. Identifiers
				// are validated by `quote`; values bind as params (no injection surface),
				// so this internal fragment needs no strict-mode bypass.
				const pivotTable = pivot.pivotTable;
				q.setPivotExistsBuilder((pivotWheres) => {
					const base =
						`EXISTS (SELECT 1 FROM ${quoteTable(pivotTable)} ` +
						`WHERE ${quoteTable(pivotTable)}.${quote(pivotFk)} = ${ph} ` +
						`AND ${quoteTable(pivotTable)}.${quote(pivotOther)} = ${quoteTable(relatedTable)}.${quote(relatedPk)}`;
					const bindings: unknown[] = [readParentId()];
					let extra = "";
					for (const w of pivotWheres) {
						const col = `${quoteTable(pivotTable)}.${quote(w.column)}`;
						if (w.operator === "IN" || w.operator === "NOT IN") {
							const vals = Array.isArray(w.value) ? w.value : [w.value];
							if (vals.length === 0) {
								// IN () matches nothing; NOT IN () matches everything.
								if (w.operator === "IN") extra += " AND 1 = 0";
								continue;
							}
							extra += ` AND ${col} ${w.operator} (${vals.map(() => "?").join(", ")})`;
							bindings.push(...vals);
						} else {
							extra += ` AND ${col} ${w.operator} ?`;
							bindings.push(w.value);
						}
					}
					return { sql: `${base}${extra})`, bindings };
				});
			} else if (relation.type === "belongsTo") {
				const ownerKey =
					relation.ownerKey ?? getPrimaryKey(relatedClass) ?? "id";
				q.where(ownerKey, entity[fkProp] ?? entity[fkCol]);
			} else if (
				relation.type === "hasOneThrough" ||
				relation.type === "hasManyThrough"
			) {
				// Lucid's read-only two-hop traversal (verified): the related rows are
				// reached VIA the intermediate ("through") table, never a direct FK.
				//   related WHERE secondKey IN
				//     (SELECT secondLocal FROM through WHERE firstKey = parent[localKey])
				// Same key resolution as the eager `#resolveThrough` loader so lazy and
				// eager agree. Returns a chainable ModelQuery (`.orderBy().limit()` …).
				if (!relation.through) {
					throw new Error(
						`@HasOneThrough/@HasManyThrough '${relationName}' requires a through model`,
					);
				}
				const throughClass = relation.through() as new () => BaseEntity;
				const throughRepo = new BaseRepository<BaseEntity>(throughClass, db, {
					dialect: this.#dialect,
				});
				const throughPk = getPrimaryKey(throughClass) ?? "id";
				const parentLocal =
					relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
				const firstKey =
					relation.firstKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
				const secondKey =
					relation.secondKey ?? `${camelToSnake(throughClass.name)}_id`;
				const secondLocal = relation.secondLocalKey ?? throughPk;
				q.whereIn(
					secondKey,
					throughRepo
						.query()
						.select(secondLocal)
						.where(firstKey, entity[parentLocal]),
				);
			} else {
				// hasOne / hasMany
				q.where(fkCol, readParentId());
			}
			return q;
		};

		if (relation.type === "belongsTo") {
			// Story 31.6 — associate / dissociate set the FK on THIS entity and save
			// it through the outer repository. Both methods close over `parentRepo`,
			// which is the repo that owns `entity` (i.e. `this`). The double cast is
			// the standard TS idiom for widening a generic `this` — safe because
			// `T extends BaseEntity`.
			const parentRepo = this as BaseRepository<BaseEntity>;
			// create/save/createMany/saveMany are INVALID for a belongsTo: the FK is on
			// THIS model, so `...hasOps` would inject the FK into the owner table and
			// save the current model before it has an owner. Reject them (same throwing
			// pattern as @HasOne's bulk methods); the only writes are associate /
			// dissociate.
			const rejectWrite = async (op: string): Promise<never> => {
				throw new Error(
					`related('${relationName}').${op}() is not supported on @BelongsTo — ` +
						`the foreign key is on this model; use associate() / dissociate().`,
				);
			};
			const proxy: BelongsToRelationProxy = {
				type: "belongsTo",
				query: scopedQuery,
				create: () => rejectWrite("create"),
				save: () => rejectWrite("save"),
				createMany: () => rejectWrite("createMany"),
				saveMany: () => rejectWrite("saveMany"),
				async associate(model: BaseEntity) {
					if (model === null || model === undefined) {
						throw new Error(
							`related('${relationName}').associate() rejects null/undefined — use dissociate() instead`,
						);
					}
					const ownerKey =
						relation.ownerKey ?? getPrimaryKey(relatedClass) ?? "id";
					// Lucid: persist an unsaved owner FIRST (so a generated key exists),
					// set the parent FK to the owner's key, then save the parent — all in
					// ONE transaction (atomic, rolled back on failure). Reject a keyless
					// owner instead of silently setting the FK to `undefined` (which the
					// UPDATE would skip → stale/absent association). Events flush post-commit.
					await transaction(db, async (trx) => {
						const ownerTx = relatedRepo.useTransaction(trx);
						ownerTx.#deferDomainEvents = true;
						// Snapshot BEFORE the save: associate() only persists an unsaved
						// owner, so an already-persisted owner's pending domain events are
						// NOT ours to flush (same side-effect fix as withParentSaved).
						const savedOwnerHere = !model.$isPersisted;
						const ownerDurable = relatedRepo.#durableParent ?? relatedRepo;
						// Floor the owner's events before we persist it (#8).
						const ownerEventFloor = model.domainEventCount();
						if (savedOwnerHere) {
							await ownerTx.save(model);
							// Register the owner's rollback restore IMMEDIATELY after its
							// insert. The ownerKey check just below AND the parent save later
							// can BOTH throw after this point, and either must still revert the
							// freshly-inserted owner (Lucid resets `$trx` on rollback): re-point
							// its REPO_REF at the durable repo, revert it to $isNew (its row
							// vanished), and drop its queued events. Registering it here (not
							// after the checks) is the fix — a throw between the save and a
							// later registration would leave the owner lying $isPersisted.
							trx.after("rollback", () => {
								ownerDurable.#attachRepoRef(model);
								model.markAsNotPersisted();
								model.restoreDomainEventsTo(ownerEventFloor);
							});
						}
						const fkValue = model[ownerKey];
						if (!isProvidedPk(fkValue)) {
							throw new AtlasError(
								"E_MISSING_OWNER_KEY",
								`Cannot associate('${relationName}'): the owner ${relatedClass.name} has no ${ownerKey} to reference.`,
								{
									hint: "Pass an owner whose key is set — a keyless aggregate/alias projection can't be a foreign-key target.",
								},
							);
						}
						entity.setProp(fkCol, fkValue);
						entity.setProp(fkProp, fkValue);
						const parentTx = parentRepo.useTransaction(trx);
						parentTx.#deferDomainEvents = true;
						// Snapshot BEFORE the save flips the flag: the parent reverts on
						// rollback ONLY if WE inserted it here (a fresh parent). An
						// already-persisted parent's row survives the rollback.
						const savedParentHere = !entity.$isPersisted;
						const parentDurable = parentRepo.#durableParent ?? parentRepo;
						// Floor the parent's events before its save (#8).
						const parentEventFloor = entity.domainEventCount();
						// Register the parent resolution hooks BEFORE the risky parent save —
						// that save can throw (a beforeUpdate hook, a constraint) AFTER the
						// owner was inserted, and a throw here must still restore state.
						trx.after("commit", async () => {
							// Lucid resets `$trx` on commit → re-point REPO_REF at the durable
							// repo (else a post-commit refresh() hits the finished trx), then
							// flush events. The parent is always saved here → always flush its
							// events. The owner's events flush only if WE saved the owner.
							parentDurable.#attachRepoRef(entity);
							if (savedOwnerHere) {
								ownerDurable.#attachRepoRef(model);
								await parentRepo.#dispatchDomainEvents(model);
							}
							await parentRepo.#dispatchDomainEvents(entity);
						});
						trx.after("rollback", () => {
							// Restore the PARENT (the owner's restore is registered above, right
							// after its insert). We do NOT revert the parent's FK value — Lucid
							// never reverts attribute values on rollback.
							parentDurable.#attachRepoRef(entity);
							if (savedParentHere) entity.markAsNotPersisted();
							entity.restoreDomainEventsTo(parentEventFloor);
						});
						await parentTx.save(entity);
					});
				},
				async dissociate() {
					entity.setProp(fkCol, null);
					entity.setProp(fkProp, null);
					await parentRepo.save(entity);
				},
			};
			return proxy;
		}

		if (relation.type === "manyToMany") {
			if (!relation.pivot)
				throw new Error(`@ManyToMany ${relationName} requires pivot options`);
			const pivot = relation.pivot;
			const pivotTable = pivot.pivotTable;
			const pivotFk =
				pivot.foreignKey ?? `${camelToSnake(this.#entityClass.name)}_id`;
			const pivotOther =
				pivot.otherKey ?? `${camelToSnake(relatedClass.name)}_id`;
			const tsConfig = pivot.pivotTimestamps;
			const pivotAdapters = pivot.pivotColumnAdapters;
			const dialect = this.#dialect;

			// Postgres cast hints for the two pivot FK columns — they reference the
			// parent / related PK types (often uuid). The pivot table is NOT an
			// entity table, so the compile-time cast registry never covers it;
			// every pivot statement (sync's currentIds SELECT, detach DELETE, attach
			// INSERT) must carry these explicitly, else `pivotFk = $1` is `uuid = text`.
			const pivotKeyCasts: Record<string, string> = {};
			// Cast keys off the RESOLVED parent key (localKey ?? PK), not always the
			// PK — an m2m with a custom localKey binds `entity[localKey]` into the
			// pivot FK, so the `::cast` must match that column's type.
			const parentPkCast = this.#castTypes[this.#dbColumn(parentPk)];
			if (parentPkCast) pivotKeyCasts[pivotFk] = parentPkCast;
			const relatedPk = pivot.relatedKey ?? getPrimaryKey(relatedClass) ?? "id";
			const relatedPkDb =
				getColumnMetadata(relatedClass).find((c) => c.propertyKey === relatedPk)
					?.columnName ?? camelToSnake(relatedPk);
			const relatedPkCast = computeCastTypes(relatedClass)[relatedPkDb];
			if (relatedPkCast) pivotKeyCasts[pivotOther] = relatedPkCast;

			/**
			 * Pivot timestamp column names, resolved once from the decorator config.
			 *
			 * Three forms supported:
			 *   - `pivotTimestamps: true`           → { created_at, updated_at } default names
			 *   - `pivotTimestamps: { createdAt: false, updatedAt: 'updated_on' }` → opt-out / rename
			 *   - `pivotTimestamps: undefined`      → no timestamps written
			 *
			 * `false` opts a timestamp out; a string overrides the column name;
			 * `undefined` falls back to the default name.
			 */
			let createdCol: string | null = null;
			let updatedCol: string | null = null;
			if (tsConfig === true) {
				createdCol = "created_at";
				updatedCol = "updated_at";
			} else if (tsConfig) {
				createdCol =
					tsConfig.createdAt === false
						? null
						: (tsConfig.createdAt ?? "created_at");
				updatedCol =
					tsConfig.updatedAt === false
						? null
						: (tsConfig.updatedAt ?? "updated_at");
			}
			const tsColumnSet = new Set(
				[createdCol, updatedCol].filter((c): c is string => c !== null),
			);
			// INSERT (attach) stamps both created_at + updated_at; UPDATE (sync's
			// attribute refresh) bumps only updated_at — Adonis Lucid pivot semantics.
			const timestampValues = (
				mode: "insert" | "update",
			): Record<string, unknown> => {
				if (!tsConfig) return {};
				const now = new Date().toISOString();
				const out: Record<string, unknown> = {};
				if (mode === "insert" && createdCol) out[createdCol] = now;
				if (updatedCol) out[updatedCol] = now;
				return out;
			};

			// Object literal keys are ALWAYS strings, so `sync({ 1: {…} })` /
			// `attach({ 1: {…} })` arrive with id "1", not 1. Bound as text, a numeric
			// pivot FK column fails on Postgres (`text` ≠ `integer`, no implicit cast)
			// and the sync diff mis-compares "1" against the numeric id the DB returns.
			// Coerce a *canonical* integer back to a number; the round-trip guard
			// leaves uuid / zero-padded / oversized string keys (`"01234"`, `"abc"`)
			// untouched so they still bind as text.
			const canonicalizeId = (id: string | number): string | number => {
				if (typeof id === "number") return id;
				return /^-?\d+$/.test(id) &&
					Number.isSafeInteger(Number(id)) &&
					String(Number(id)) === id
					? Number(id)
					: id;
			};

			const normalizeAttach = (
				arg: Array<string | number> | Record<string, Record<string, unknown>>,
			): Array<{ id: string | number; extras: Record<string, unknown> }> => {
				if (Array.isArray(arg))
					return arg.map((id) => ({ id: canonicalizeId(id), extras: {} }));
				return Object.entries(arg).map(([id, extras]) => ({
					id: canonicalizeId(id),
					extras,
				}));
			};

			// Apply a pivot column's `prepare` adapter (model → DB), shared by the
			// INSERT (attach) and UPDATE (sync) write paths.
			const encodeExtra = (k: string, raw: unknown): unknown => {
				const prepare = pivotAdapters?.[k]?.prepare;
				if (!prepare) return raw;
				let encoded: unknown;
				try {
					// Adonis Lucid signature: (value, attribute, model). A pivot-row
					// write carries no single model instance.
					encoded = prepare(raw, k, undefined);
				} catch (err) {
					throw wrapAdapterError("prepare", k, err);
				}
				assertNotPromise("prepare", k, encoded);
				return encoded;
			};

			// Reject an extras key colliding with a reserved pivot column. Without
			// this guard the FK case would silently override `parentIdValue`
			// (corrupting the join) and the timestamp case would duplicate the column
			// (driver-dependent failure or last-wins overwrite).
			const assertExtraKeyAllowed = (k: string): void => {
				if (k === pivotFk || k === pivotOther) {
					throw new Error(
						`Pivot extras key '${k}' collides with the ${k === pivotFk ? "foreignKey" : "otherKey"} column on '${pivotTable}'. Reserved keys MUST NOT appear in attach()/sync() extras.`,
					);
				}
				if (tsColumnSet.has(k)) {
					throw new Error(
						`Pivot extras key '${k}' collides with a pivotTimestamps column on '${pivotTable}'. Disable the timestamp in the relation options or rename your extra.`,
					);
				}
			};

			// Narrow an unknown pivot id to a bindable scalar without an `as` cast.
			const asId = (v: unknown): string | number =>
				typeof v === "number" ? v : String(v);

			// Current pivot rows for this parent — the other-key plus any attribute
			// columns the caller needs (so sync() can diff changed pivot rows).
			// Compiled through the Rust SELECT path so the pivot identifiers go
			// through `quote_identifier` (rejects anything outside `[A-Za-z0-9_]`),
			// never the ad-hoc `quote` helper. Runs on `conn` — a transaction inside
			// sync(), the pool otherwise.
			const currentPivotRows = async (
				attrCols: string[],
				conn: DatabaseConnection = db,
			): Promise<
				Array<{ id: string | number; row: Record<string, unknown> }>
			> => {
				const selectSpec = {
					kind: "select",
					table: pivotTable,
					select: [pivotOther, ...attrCols],
					wheres: [
						{
							column: pivotFk,
							operator: "=",
							value: readParentId(),
							type: "and",
						},
					],
					selectSubqueries: [],
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
					casts: pivotKeyCasts,
				};
				const compiled = compileStatementNative(selectSpec, dialect);
				const rows = await conn.query<Record<string, unknown>>(
					compiled.statements[0],
					compiled.params,
				);
				return rows.map((r) => ({ id: asId(r[pivotOther]), row: r }));
			};

			// Delete via the Rust DELETE compiler so the pivot table + columns get
			// `quote_identifier` validation (rejects `"`, `;`, etc.) — safer than
			// the previous hand-built SQL with a dumb `"` wrapper.
			const detach = async (
				ids?: Array<string | number>,
				conn: DatabaseConnection = db,
			): Promise<void> => {
				const wheres: Array<Record<string, unknown>> = [
					{
						column: pivotFk,
						operator: "=",
						value: readParentId(),
						type: "and",
					},
				];
				if (ids && ids.length > 0) {
					wheres.push({
						column: pivotOther,
						operator: "IN",
						value: ids,
						type: "and",
					});
				}
				const spec = {
					kind: "delete",
					table: pivotTable,
					wheres,
					returning: [],
					casts: pivotKeyCasts,
				};
				const compiled = compileStatementNative(spec, dialect);
				await conn.execute(compiled.statements[0], compiled.params);
			};

			const attach = async (
				ids: Array<string | number> | Record<string, Record<string, unknown>>,
				conn: DatabaseConnection = db,
				parentFk: unknown = readParentId(),
			): Promise<void> => {
				const entries = normalizeAttach(ids);
				if (entries.length === 0) return;
				const ts = timestampValues("insert");
				// Union of extra keys across all entries; back-fill missing keys with
				// `null` so every row in the multi-insert shares the same column set
				// (required by the Rust compiler's homogeneity check).
				const extraKeys = new Set<string>();
				for (const e of entries) {
					for (const k of Object.keys(e.extras)) extraKeys.add(k);
				}
				for (const k of extraKeys) assertExtraKeyAllowed(k);
				const rowPairs = entries.map((e) => {
					const pairs: Array<[string, unknown]> = [
						[pivotFk, parentFk],
						[pivotOther, e.id],
					];
					for (const k of extraKeys)
						pairs.push([k, encodeExtra(k, e.extras[k] ?? null)]);
					for (const [k, v] of Object.entries(ts)) pairs.push([k, v]);
					return pairs;
				});
				// Postgres casts for the pivot row: the two FK columns (parent /
				// related PK types, often uuid) reuse `pivotKeyCasts`; pivot
				// timestamps are bound strings — all need `$N::<type>` on Postgres.
				const pivotCasts: Record<string, string> = { ...pivotKeyCasts };
				for (const k of Object.keys(ts)) pivotCasts[k] = "timestamp";
				const spec = {
					kind: "insert",
					table: pivotTable,
					rows: rowPairs,
					casts: pivotCasts,
				};
				const compiled = compileStatementNative(spec, dialect);
				await conn.execute(compiled.statements[0], compiled.params);
			};

			// Refresh one already-attached pivot row's attributes (sync's update arm,
			// Adonis Lucid parity): set the provided extras (adapter-encoded) and bump
			// only updated_at.
			const updatePivot = async (
				id: string | number,
				extras: Record<string, unknown>,
				conn: DatabaseConnection = db,
			): Promise<void> => {
				const ts = timestampValues("update");
				const set: Array<[string, unknown]> = [];
				for (const [k, raw] of Object.entries(extras)) {
					assertExtraKeyAllowed(k);
					set.push([k, encodeExtra(k, raw ?? null)]);
				}
				for (const [k, v] of Object.entries(ts)) set.push([k, v]);
				if (set.length === 0) return;
				const casts: Record<string, string> = { ...pivotKeyCasts };
				for (const k of Object.keys(ts)) casts[k] = "timestamp";
				const spec = {
					kind: "update",
					table: pivotTable,
					set,
					wheres: [
						{
							column: pivotFk,
							operator: "=",
							value: readParentId(),
							type: "and",
						},
						{ column: pivotOther, operator: "=", value: id, type: "and" },
					],
					returning: [],
					casts,
				};
				const compiled = compileStatementNative(spec, dialect);
				await conn.execute(compiled.statements[0], compiled.params);
			};

			/**
			 * Diff the current pivot state against a target set and apply the minimum
			 * insert / update / delete to converge (Adonis Lucid `sync`): rows missing
			 * from the pivot are attached, already-attached rows whose pivot attributes
			 * changed are updated, and rows absent from the target are detached (unless
			 * `additive`). The read and all three writes run inside ONE managed
			 * transaction — atomic and rolled back on any failure, so a concurrent
			 * writer can't wedge the pivot into a half-synced state.
			 */
			const sync = async (
				target:
					| Array<string | number>
					| Record<string, Record<string, unknown>>,
				additive = false,
			): Promise<void> => {
				const entries = normalizeAttach(target);
				// Attribute columns to read back so we can detect changed pivot rows.
				const attrCols = new Set<string>();
				for (const e of entries) {
					for (const k of Object.keys(e.extras)) attrCols.add(k);
				}
				const desiredIds = new Set(entries.map((e) => String(e.id)));

				await transaction(db, async (trx) => {
					const current = await currentPivotRows([...attrCols], trx);
					const currentById = new Map<
						string,
						{ id: string | number; row: Record<string, unknown> }
					>();
					for (const c of current) currentById.set(String(c.id), c);

					// Diff by String(id): the DB returns numeric ids for an integer
					// pivot column while object-form targets carry canonicalized ids —
					// stringifying both sides keeps the comparison type-agnostic.
					const toDetach = additive
						? []
						: current
								.filter((c) => !desiredIds.has(String(c.id)))
								.map((c) => c.id);
					const toAttach = entries.filter(
						(e) => !currentById.has(String(e.id)),
					);
					const toUpdate = entries.filter((e) => {
						if (Object.keys(e.extras).length === 0) return false;
						const cur = currentById.get(String(e.id));
						if (!cur) return false;
						// Only rewrite when a provided attribute actually differs — a
						// no-op sync must not churn rows or bump updated_at. Compare
						// nullish and empty-string as DISTINCT (a `String(x ?? "")`
						// collapse would treat `null` and `""` as equal and miss a real
						// attribute change from one to the other).
						return Object.keys(e.extras).some((k) => {
							const stored = cur.row[k];
							const next = encodeExtra(k, e.extras[k] ?? null);
							const storedNull = stored === null || stored === undefined;
							const nextNull = next === null || next === undefined;
							if (storedNull || nextNull) return storedNull !== nextNull;
							return String(stored) !== String(next);
						});
					});

					if (toDetach.length > 0) await detach(toDetach, trx);
					for (const e of toUpdate) await updatePivot(e.id, e.extras, trx);
					if (toAttach.length > 0) {
						const attachArg: Record<string, Record<string, unknown>> = {};
						for (const e of toAttach) attachArg[String(e.id)] = e.extras;
						await attach(attachArg, trx);
					}
				});
			};

			// m2m create/save persist the related row THEN insert a pivot row —
			// NOT `hasOps.injectFk`, which would write a bogus `<parent>_id` column
			// onto the related table and never touch the pivot (silent corruption).
			// The whole chain (persist unsaved parent → write related → insert pivot)
			// runs in ONE transaction via `withParentSaved` (AdonisJS/Lucid parity):
			// atomic, rolled back on any failure (no orphan related row, no pivot to a
			// missing parent), with domain events flushed only after commit.
			const relatedPkProp =
				pivot.relatedKey ?? getPrimaryKey(relatedClass) ?? "id";
			const attachRows = (
				rows: BaseEntity[],
				trx: TransactionClient,
				fk: unknown,
				pivotFor?: (index: number) => Record<string, unknown>,
			): Promise<void> => {
				if (rows.length === 0) return Promise.resolve();
				const arg: Record<string, Record<string, unknown>> = {};
				rows.forEach((r, i) => {
					arg[String(r[relatedPkProp])] = pivotFor?.(i) ?? {};
				});
				return attach(arg, trx, fk);
			};
			// create/save accept per-row pivot attributes (Adonis Lucid `create(values,
			// pivotAttributes)` / `save(related, pivotAttributes)`) — written onto the
			// pivot row alongside the FK/otherKey, in the same transaction.
			const m2mOps = {
				create: (
					data: Record<string, unknown>,
					pivotAttributes?: Record<string, unknown>,
				): Promise<BaseEntity> =>
					withParentSaved(async (fk, rel, trx) => {
						const created = await rel.create(data);
						await attachRows([created], trx, fk, () => pivotAttributes ?? {});
						trx.after("commit", () => flushEvents([created]));
						return created;
					}),
				createMany: (
					rows: Array<Record<string, unknown>>,
					pivotAttributes?: Array<Record<string, unknown>>,
				): Promise<BaseEntity[]> =>
					withParentSaved(async (fk, rel, trx) => {
						const created = await rel.createMany(rows);
						await attachRows(
							created,
							trx,
							fk,
							(i) => pivotAttributes?.[i] ?? {},
						);
						// NO wrapper flush: rel.createMany now self-dispatches via
						// #inManagedTx (like saveMany). The pivot rows carry no events; a
						// second flush would double the related rows' events on a partial
						// sink failure.
						return created;
					}),
				save: (
					related: BaseEntity,
					pivotAttributes?: Record<string, unknown>,
				): Promise<void> =>
					withParentSaved(async (fk, rel, trx) => {
						await rel.save(related);
						await attachRows([related], trx, fk, () => pivotAttributes ?? {});
						trx.after("commit", () => flushEvents([related]));
					}),
				saveMany: (
					related: BaseEntity[],
					pivotAttributes?: Array<Record<string, unknown>>,
				): Promise<BaseEntity[]> =>
					withParentSaved(async (fk, rel, trx) => {
						const saved = await rel.saveMany(related);
						await attachRows(saved, trx, fk, (i) => pivotAttributes?.[i] ?? {});
						// NO wrapper flush: rel.saveMany self-dispatches via #inManagedTx
						// (all-or-nothing). A second flush would double on partial failure.
						return saved;
					}),
			};
			// attach/detach/sync operate DIRECTLY on the pivot using the parent key —
			// unlike create/save they never persist the parent (there's no related row
			// to hang the transaction on). Lucid requires a persisted parent WITH a key
			// here (every doc example starts from `findOrFail`); without the guard a
			// keyless/unsaved parent would write a pivot row with a null FK or target a
			// nonexistent parent. Same seam as delete/refresh: E_MODEL_NOT_PERSISTED on
			// an unsaved instance, E_MISSING_PRIMARY_KEY on a keyless projection.
			const guardParent = (op: string): void =>
				this.#assertPersistedRow(
					entity,
					readParentId(),
					`related('${relationName}').${op}`,
					// The pivot FK references `parentPk` (localKey ?? PK) — name THAT key
					// in a missing-key diagnostic, not always 'id'.
					parentPk,
				);
			const proxy: ManyToManyRelationProxy = {
				type: "manyToMany",
				...m2mOps,
				query: scopedQuery,
				// A query builder on the PIVOT table itself, scoped to this parent
				// (Adonis Lucid `pivotQuery`) — for reading/updating/deleting pivot
				// rows directly, beyond attach/detach/sync.
				pivotQuery: () => {
					guardParent("pivotQuery()");
					return new DatabaseQueryBuilder(this.#db, dialect, pivotTable).where(
						pivotFk,
						readParentId(),
					);
				},
				// async so the guard throw surfaces as a REJECTED promise — a method
				// typed `Promise<void>` must never throw synchronously.
				attach: async (ids) => {
					guardParent("attach()");
					return attach(ids);
				},
				detach: async (ids) => {
					guardParent("detach()");
					return detach(ids);
				},
				sync: async (target, additive) => {
					guardParent("sync()");
					return sync(target, additive);
				},
			};
			return proxy;
		}

		if (
			relation.type === "hasOneThrough" ||
			relation.type === "hasManyThrough"
		) {
			// READ-ONLY (Lucid parity, verified): a through relation exposes only
			// query()/preload. Every write is rejected — the old code fell through to
			// the hasMany default and wrote to the WRONG table with a bogus direct FK.
			// To persist, the caller must go through the intermediate model.
			const rejectWrite = async (op: string): Promise<never> => {
				throw new Error(
					`related('${relationName}').${op}() is not supported on ` +
						`@HasManyThrough/@HasOneThrough — through relations are READ-ONLY ` +
						`(Lucid parity); persist via the intermediate model.`,
				);
			};
			const proxy: HasManyThroughRelationProxy = {
				type: relation.type,
				query: scopedQuery,
				create: () => rejectWrite("create"),
				save: () => rejectWrite("save"),
				createMany: () => rejectWrite("createMany"),
				saveMany: () => rejectWrite("saveMany"),
			};
			return proxy;
		}

		// Default: hasOne / hasMany
		if (relation.type === "hasOne") {
			// @HasOne is a one-to-one relation — createMany/saveMany would violate
			// the invariant at the ORM level (and silently shadow a missing UNIQUE
			// constraint at the DB level). The typed proxy declares them as
			// `Promise<never>` so callers get a compile-time signal; at runtime
			// both throw a clear error.
			const reject = async (op: string): Promise<never> => {
				throw new Error(
					`related('${relationName}').${op}() is not supported on @HasOne — ` +
						`use .create() / .save() for a single related row.`,
				);
			};
			const proxy: HasOneRelationProxy = {
				type: "hasOne",
				create: hasOps.create,
				save: hasOps.save,
				firstOrCreate: hasOps.firstOrCreate,
				updateOrCreate: hasOps.updateOrCreate,
				createMany: () => reject("createMany"),
				saveMany: () => reject("saveMany"),
				query: scopedQuery,
			};
			return proxy;
		}
		const proxy: HasManyRelationProxy = {
			type: "hasMany",
			...hasOps,
			query: scopedQuery,
		};
		return proxy;
	}

	async fresh(entity: T): Promise<T> {
		const pk = entity[this.#primaryKey];
		this.#assertPersistedRow(entity, pk, "fresh()");
		const found = await this.find(pk as string | number);
		if (!found) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
		return found;
	}

	#entityToRow(entity: T): Record<string, unknown> {
		// `#columns` already includes the primary-key property: `@PrimaryKey()`
		// internally calls `@Column()` to register the PK as a regular column
		// (see decorators/entity.ts). The earlier trailing block re-emitted
		// the PK as a raw camelCase key, producing a double-write for non-`id`
		// PK names (`{ user_id: ..., userId: ... }` would land in the row dict).
		const row: Record<string, unknown> = {};
		for (const col of this.#columns) {
			const value = entity[col];
			if (value !== undefined) {
				row[this.#dbColumn(col)] = this.#applyPrepare(col, value, entity);
			}
		}
		return row;
	}

	#buildSetPairs(
		data: Partial<Record<string, unknown>>,
	): Array<[string, unknown]> {
		const pairs: Array<[string, unknown]> = [];
		for (const [key, value] of Object.entries(data)) {
			// Mirror `#plainToRowPairs` — skip undefined so updates can't bind it.
			if (value === undefined) continue;
			// `#applyPrepare` normalises the key (property / snake / columnName).
			pairs.push([this.#resolveColumn(key), this.#applyPrepare(key, value)]);
		}
		return pairs;
	}

	/**
	 * Build SET pairs for `increment`/`decrement` — each entry carries a
	 * `{ op, value }` payload that the Rust compiler turns into
	 * `SET col = col ± ?` instead of the standard `SET col = ?`.
	 */
	#buildIncrementPairs(
		columnOrMap: string | Record<string, number>,
		amount: number,
		op: "increment" | "decrement",
	): Array<[string, { op: "increment" | "decrement"; value: number }]> {
		if (typeof columnOrMap === "string") {
			return [[this.#resolveColumn(columnOrMap), { op, value: amount }]];
		}
		return Object.entries(columnOrMap).map(
			([col, delta]) =>
				[this.#resolveColumn(col), { op, value: delta }] as [
					string,
					{ op: "increment" | "decrement"; value: number },
				],
		);
	}
}

/**
 * Annotate an adapter callback failure with the property key that triggered
 * it. Without this, a `prepare`/`consume` throwing on row N silently surfaces
 * as "Invalid bind value" or similar, with no hint at WHICH column the
 * adapter rejected — the dev has to bisect across every adapter-tagged
 * property to find the culprit.
 */
export function wrapAdapterError(
	phase: "prepare" | "consume",
	propertyKey: string,
	err: unknown,
): Error {
	const message = err instanceof Error ? err.message : String(err);
	// `cause: err` preserves the original error (and its stack) per ES2022
	// Error Cause. The wrapped Error keeps its own `stack` pointing at the
	// wrap site so `console.error(wrapped)` shows the column-annotated
	// header; Node ≥16.9 walks the cause chain to print the underlying
	// throw's stack underneath.
	return new Error(`@Column.${phase} threw on '${propertyKey}': ${message}`, {
		cause: err,
	});
}

/**
 * Adapter callbacks must be synchronous — the bind layer cannot await before
 * handing values to the Rust DML compiler. Catching an `async` adapter here
 * gives the user a column-annotated error instead of an opaque "Invalid bind
 * value" downstream when the unawaited Promise hits the NAPI boundary.
 */
export function assertNotPromise(
	phase: "prepare" | "consume",
	propertyKey: string,
	value: unknown,
): void {
	if (
		value !== null &&
		typeof value === "object" &&
		"then" in value &&
		typeof Reflect.get(value, "then") === "function"
	) {
		throw new Error(
			`@Column.${phase} on '${propertyKey}' returned a Promise — adapters must be synchronous (the bind layer cannot await).`,
		);
	}
}
