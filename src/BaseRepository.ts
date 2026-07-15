/**
 * BaseRepository — Data Mapper ORM with typed CRUD, soft deletes, and domain events.
 *
 * @implements FR29, FR31, FR35
 */

import { randomUUID } from "node:crypto";
import { DateTime } from "@c9up/chronos";
import { dateTimeAtlasAdapter } from "@c9up/chronos/atlas";
import type { TransactionOptions } from "./adapters/NapiDbAdapter.js";
import type {
	BaseEntity,
	BelongsToRelationProxy,
	DomainEvent,
	HasManyRelationProxy,
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
	getEntityMetadata,
	getPrimaryKey,
	getPrimaryKeyGenerator,
	getRelationMetadata,
	hasSoftDeletes,
	type PrimaryKeyGenerator,
} from "./decorators/entity.js";
import { fireHooks } from "./decorators/hooks.js";
import { AtlasError, EntityNotFoundError } from "./errors.js";
import { ModelQuery, runWithAtlasInternalBypass } from "./ModelQuery.js";
import {
	type AtlasDialect,
	compileStatementNative,
	getAtlasDialect,
	registerColumnCast,
	registerTableCasts,
} from "./query/native.js";
import { type TransactionClient, transaction } from "./Transaction.js";
import { camelToSnake, snakeToCamel } from "./utils/casing.js";

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
	/** Run a write statement; returns rowsAffected. */
	execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>;
	/** Run a SELECT and return all rows. */
	query<T = Row>(sql: string, params?: unknown[]): Promise<T[]>;
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
	#columnPrepares: Map<string, (value: unknown) => unknown>;
	/**
	 * Per-property `consume` (DB → model) callbacks lifted directly from
	 * `@Column({ consume })` metadata. Keyed by camelCase `propertyKey`.
	 * Mirror of Adonis Lucid's `@column.consume`. Story 35.10.
	 */
	#columnConsumes: Map<string, (value: unknown) => unknown>;
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
		this.#columnPrepares = new Map<string, (value: unknown) => unknown>();
		this.#columnConsumes = new Map<string, (value: unknown) => unknown>();
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
				const relatedMeta = getEntityMetadata(related);
				if (cast && relatedMeta) {
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
		const quoted =
			this.#dialect === "mysql"
				? `\`${this.#tableName}\``
				: `"${this.#tableName}"`;
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
	 * Build an entity from a plain object and persist it. Fires `beforeSave` →
	 * `beforeCreate` → INSERT → `afterCreate` → `afterSave`.
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
			await fireHooks(this.#entityClass, "beforeSave", entity);
			await fireHooks(this.#entityClass, "beforeCreate", entity);
		}
		await this.#insert(entity);
		if (!quiet) {
			await fireHooks(this.#entityClass, "afterCreate", entity);
			await fireHooks(this.#entityClass, "afterSave", entity);
		}
		await this.#dispatchDomainEvents(entity);
		return entity;
	}

	/** {@link create} without firing lifecycle hooks (AdonisJS Lucid `createQuietly`). */
	createQuietly(data: Partial<Record<string, unknown>>): Promise<T> {
		return this.create(data, true);
	}

	/**
	 * Persist an entity. Insert if PK is missing or row doesn't exist, update
	 * otherwise. Fires `beforeSave` → (`beforeCreate` | `beforeUpdate`) → DB →
	 * (`afterCreate` | `afterUpdate`) → `afterSave`, then dispatches
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
		// Treat a present PK (including `0` and `''`) as a candidate update —
		// `pk && ...` would route legitimate zero / empty-string keys through
		// INSERT and double-write the row.
		const isUpdate = isProvidedPk(pk) && (await this.find(pk)) !== null;

		if (!quiet) await fireHooks(this.#entityClass, "beforeSave", entity);
		if (isUpdate) {
			await this.#runUpdateBranch(entity, quiet);
		} else {
			try {
				await this.#runInsertBranch(entity, quiet);
			} catch (err) {
				// Race recovery: the row didn't exist when we checked, but a
				// concurrent insert beat us to it. Only fall back when the PK
				// was explicitly provided (auto-generated PK can't collide on
				// a fresh insert — DB generates a unique one per call).
				if (isProvidedPk(pk) && isUniqueKeyViolation(err)) {
					await this.#runUpdateBranch(entity, quiet);
				} else {
					throw err;
				}
			}
		}
		if (!quiet) await fireHooks(this.#entityClass, "afterSave", entity);

		await this.#dispatchDomainEvents(entity);
	}

	/** {@link save} without firing lifecycle hooks (AdonisJS Lucid `saveQuietly`). */
	saveQuietly(entity: T): Promise<void> {
		return this.save(entity, true);
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

	async #runInsertBranch(entity: T, quiet = false): Promise<void> {
		if (!quiet) await fireHooks(this.#entityClass, "beforeCreate", entity);
		await this.#insert(entity);
		if (!quiet) await fireHooks(this.#entityClass, "afterCreate", entity);
	}

	async #runUpdateBranch(entity: T, quiet = false): Promise<void> {
		if (!quiet) await fireHooks(this.#entityClass, "beforeUpdate", entity);
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
		if (!quiet) {
			for (const e of entities) {
				await fireHooks(this.#entityClass, "beforeSave", e);
				await fireHooks(this.#entityClass, "beforeCreate", e);
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
					entities[i].setProp(prop, this.#applyConsume(prop, v));
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
		for (const e of entities) {
			await this.#dispatchDomainEvents(e);
		}
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
		// Split new vs already-persisted; for simplicity, persist new ones as a
		// batch and fall back to per-entity save for dirty ones.
		const fresh: T[] = [];
		const dirty: T[] = [];
		for (const e of entities) {
			if (Object.keys(e.$original ?? {}).length === 0) fresh.push(e);
			else dirty.push(e);
		}
		if (fresh.length > 0) {
			const rows = fresh.map((e) => {
				const r: Record<string, unknown> = {};
				for (const c of this.#columns) {
					const v = e[c];
					if (v !== undefined) r[c] = v;
				}
				return r;
			});
			const created = await this.createMany(rows);
			// Copy generated PKs back to the original instances.
			created.forEach((c, i) => {
				fresh[i].setProp(this.#primaryKey, c[this.#primaryKey]);
				fresh[i].markAsPersisted();
			});
		}
		for (const d of dirty) await this.save(d);
		return entities;
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
		return transaction(this.#db, async (trx) => {
			const repo = this.useTransaction(trx);
			const existing = await repo.#findBySearch(search, true);
			if (existing) return existing;
			return repo.create({ ...search, ...defaults });
		});
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
		return transaction(this.#db, async (trx) => {
			const repo = this.useTransaction(trx);
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
		});
	}

	/** Extract the search clause (the unique key column(s)) from a row. */
	#pickKeys(
		row: Record<string, unknown>,
		key: string | string[],
	): Record<string, unknown> {
		const keys = Array.isArray(key) ? key : [key];
		const search: Record<string, unknown> = {};
		for (const k of keys) search[k] = row[k];
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
		return transaction(this.#db, async (trx) => {
			const repo = this.useTransaction(trx);
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
		});
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
		return transaction(this.#db, async (trx) => {
			const repo = this.useTransaction(trx);
			const out: T[] = [];
			for (const row of rows) {
				const existing = await repo.#findBySearch(
					this.#pickKeys(row, key),
					true,
				);
				out.push(existing ?? (await repo.create(row)));
			}
			return out;
		});
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
	#applyPrepare(key: string, value: unknown): unknown {
		// Callers may pass a DB column name (e.g. updateWhere("starts_at", …) or a
		// `@Column({ columnName })` column) — prepare/dateColumns are keyed by the TS
		// property, so normalise via the reverse map first, else the adapter/date
		// conversion is silently skipped.
		const propertyKey = this.#columnByDbName.get(key) ?? key;
		const prepare = this.#columnPrepares.get(propertyKey);
		if (prepare) {
			let result: unknown;
			try {
				result = prepare(value);
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

	#applyConsume(propertyKey: string, value: unknown): unknown {
		const consume = this.#columnConsumes.get(propertyKey);
		if (consume) {
			let result: unknown;
			try {
				result = consume(value);
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
				entity.setProp(prop, this.#applyConsume(prop, v));
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
			entity.setProp(targetKey, this.#applyConsume(targetKey, value));
		}
		// Freeze the original snapshot — from now on, only columns changed AFTER
		// hydration are considered dirty by `entity.$dirty`.
		entity.markAsPersisted();
		entity.markAsFromDatabase();
		// Back-pointer so `entity.refresh()` / `entity.fresh()` can re-query.
		Object.defineProperty(entity, REPO_REF, {
			value: this,
			enumerable: false,
			configurable: true,
		});
		return entity;
	}

	/**
	 * Re-read the entity's row from the database and mutate the instance in place.
	 * Used by `entity.refresh()` — not normally called directly.
	 *
	 * @implements Story 32.6
	 */
	async refresh(entity: BaseEntity): Promise<void> {
		const pk = entity[this.#primaryKey];
		if (pk === undefined || pk === null) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
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
		if (pk === undefined || pk === null) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
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
		if (pk === undefined || pk === null) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
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
		if (pk === undefined || pk === null) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
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
		const relatedMeta = getEntityMetadata(relatedClass);
		if (!relatedMeta)
			throw new Error(
				`Entity metadata missing on related class ${relatedClass.name}`,
			);
		const relatedTable = relatedMeta.tableName;
		const parentPk =
			relation.localKey ?? getPrimaryKey(this.#entityClass) ?? "id";
		const parentIdValue = entity[parentPk];
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
		): Record<string, unknown> => ({
			...data,
			[fkCol]: parentIdValue,
			[fkProp]: parentIdValue,
		});

		// Shared "has" proxy methods (create/createMany/save/saveMany +
		// firstOrCreate/updateOrCreate scoped to this parent's FK).
		const hasOps = {
			async create(data: Record<string, unknown>) {
				return relatedRepo.create(injectFk(data));
			},
			async createMany(rows: Array<Record<string, unknown>>) {
				return relatedRepo.createMany(rows.map(injectFk));
			},
			// Scope the search to the parent's FK column so the lookup only sees
			// this parent's rows; inject the FK into the created/updated row. The
			// related repo's firstOrCreate/updateOrCreate are atomic (txn + lock).
			async firstOrCreate(
				search: Record<string, unknown>,
				defaults: Record<string, unknown> = {},
			) {
				return relatedRepo.firstOrCreate(
					{ ...search, [fkCol]: parentIdValue },
					injectFk(defaults),
				);
			},
			async updateOrCreate(
				search: Record<string, unknown>,
				values: Record<string, unknown>,
			) {
				return relatedRepo.updateOrCreate(
					{ ...search, [fkCol]: parentIdValue },
					injectFk(values),
				);
			},
			async save(related: BaseEntity) {
				related.setProp(fkCol, parentIdValue);
				related.setProp(fkProp, parentIdValue);
				await relatedRepo.save(related);
			},
			async saveMany(related: BaseEntity[]) {
				for (const r of related) {
					r.setProp(fkCol, parentIdValue);
					r.setProp(fkProp, parentIdValue);
				}
				return relatedRepo.saveMany(related);
			},
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
				const relatedPkProp = getPrimaryKey(relatedClass) ?? "id";
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
				// The bound `?` carries the parent PK type (often uuid). A raw `?`
				// can't be cast by the structured `casts` mechanism, so emit the
				// `::uuid` inline — `whereRaw` rewrites `?`→`$N`, yielding `$N::uuid`.
				// Postgres-only; sqlite/mysql coerce. Without it: `pivotFk = $N` is
				// `uuid = text`.
				const parentPkCast = this.#castTypes[this.#dbColumn(this.#primaryKey)];
				const ph =
					dialect === "postgres" && parentPkCast ? `?::${parentPkCast}` : "?";
				// EXISTS (SELECT 1 FROM pivot WHERE pivot.pivotFk = ? AND pivot.pivotOther = related.pk)
				// Framework-internal raw fragment (identifiers already validated by
				// the `quote` helper above) — bypass strict mode so this path still
				// works when the user enables `setAtlasStrictMode(true)` on their app.
				runWithAtlasInternalBypass(() => {
					q.whereRaw(
						`EXISTS (SELECT 1 FROM ${quote(pivot.pivotTable)} ` +
							`WHERE ${quote(pivot.pivotTable)}.${quote(pivotFk)} = ${ph} ` +
							`AND ${quote(pivot.pivotTable)}.${quote(pivotOther)} = ${quote(relatedTable)}.${quote(relatedPk)})`,
						[parentIdValue],
					);
				});
			} else if (relation.type === "belongsTo") {
				const ownerKey =
					relation.ownerKey ?? getPrimaryKey(relatedClass) ?? "id";
				q.where(ownerKey, entity[fkProp] ?? entity[fkCol]);
			} else {
				// hasOne / hasMany
				q.where(fkCol, parentIdValue);
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
			const proxy: BelongsToRelationProxy = {
				type: "belongsTo",
				...hasOps,
				query: scopedQuery,
				async associate(model: BaseEntity) {
					if (model === null || model === undefined) {
						throw new Error(
							`related('${relationName}').associate() rejects null/undefined — use dissociate() instead`,
						);
					}
					const ownerKey =
						relation.ownerKey ?? getPrimaryKey(relatedClass) ?? "id";
					const fkValue = model[ownerKey];
					entity.setProp(fkCol, fkValue);
					entity.setProp(fkProp, fkValue);
					await parentRepo.save(entity);
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
			const parentPkCast = this.#castTypes[this.#dbColumn(this.#primaryKey)];
			if (parentPkCast) pivotKeyCasts[pivotFk] = parentPkCast;
			const relatedPk = getPrimaryKey(relatedClass) ?? "id";
			const relatedPkDb =
				getColumnMetadata(relatedClass).find((c) => c.propertyKey === relatedPk)
					?.columnName ?? camelToSnake(relatedPk);
			const relatedPkCast = computeCastTypes(relatedClass)[relatedPkDb];
			if (relatedPkCast) pivotKeyCasts[pivotOther] = relatedPkCast;

			/**
			 * Resolve pivot timestamp column names from the decorator config.
			 *
			 * Three forms supported:
			 *   - `pivotTimestamps: true`           → { created_at, updated_at } default names
			 *   - `pivotTimestamps: { createdAt: false, updatedAt: 'updated_on' }` → opt-out / rename
			 *   - `pivotTimestamps: undefined`      → no timestamps written
			 *
			 * `false` opts a timestamp out; a string overrides the column name;
			 * `undefined` falls back to the default name.
			 */
			const resolveTimestamps = (): Record<string, unknown> => {
				if (!tsConfig) return {};
				const now = new Date().toISOString();
				let createdCol: string | null;
				let updatedCol: string | null;
				if (tsConfig === true) {
					createdCol = "created_at";
					updatedCol = "updated_at";
				} else {
					createdCol =
						tsConfig.createdAt === false
							? null
							: (tsConfig.createdAt ?? "created_at");
					updatedCol =
						tsConfig.updatedAt === false
							? null
							: (tsConfig.updatedAt ?? "updated_at");
				}
				const out: Record<string, unknown> = {};
				if (createdCol) out[createdCol] = now;
				if (updatedCol) out[updatedCol] = now;
				return out;
			};

			const normalizeAttach = (
				arg: Array<string | number> | Record<string, Record<string, unknown>>,
			): Array<{ id: string | number; extras: Record<string, unknown> }> => {
				if (Array.isArray(arg)) return arg.map((id) => ({ id, extras: {} }));
				return Object.entries(arg).map(([id, extras]) => ({ id, extras }));
			};

			// Current pivot rows — compiled through the Rust SELECT path so the
			// pivot identifiers go through `quote_identifier` (rejects anything
			// outside `[A-Za-z0-9_]`), rather than through the ad-hoc `quote`
			// helper that would blindly wrap a malicious metadata string.
			//
			// Now async — every site in `sync()` is in an async closure.
			const currentIds = async (): Promise<Array<string | number>> => {
				const selectSpec = {
					kind: "select",
					table: pivotTable,
					select: [pivotOther],
					wheres: [
						{
							column: pivotFk,
							operator: "=",
							value: parentIdValue,
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
				const rows = await db.query<Record<string, unknown>>(
					compiled.statements[0],
					compiled.params,
				);
				return rows.map((r) => r[pivotOther] as string | number);
			};

			// Delete via the Rust DELETE compiler so the pivot table + columns get
			// `quote_identifier` validation (rejects `"`, `;`, etc.) — safer than
			// the previous hand-built SQL with a dumb `"` wrapper.
			const detach = async (ids?: Array<string | number>): Promise<void> => {
				const wheres: Array<Record<string, unknown>> = [
					{ column: pivotFk, operator: "=", value: parentIdValue, type: "and" },
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
				await db.execute(compiled.statements[0], compiled.params);
			};

			const attach = async (
				ids: Array<string | number> | Record<string, Record<string, unknown>>,
			): Promise<void> => {
				const entries = normalizeAttach(ids);
				if (entries.length === 0) return;
				const ts = resolveTimestamps();
				// Normalize heterogeneous extras: compute the union of extra keys
				// across all entries and back-fill missing keys with `null`, so every
				// row in the multi-insert shares the same column set (required by the
				// Rust compiler's homogeneity check).
				const extraKeys = new Set<string>();
				for (const e of entries) {
					for (const k of Object.keys(e.extras)) extraKeys.add(k);
				}
				// Reject extras keys that collide with reserved pivot columns. Without
				// this guard, an extras entry named after the FK or a timestamp column
				// would emit a duplicate column in the INSERT row pair: the FK case
				// silently overrides `parentIdValue` (corrupting the join); the
				// timestamp case duplicates the column entirely (driver-dependent
				// failure or last-wins overwrite).
				for (const k of extraKeys) {
					if (k === pivotFk || k === pivotOther) {
						throw new Error(
							`Pivot extras key '${k}' collides with the ${k === pivotFk ? "foreignKey" : "otherKey"} column on '${pivotTable}'. Reserved keys MUST NOT appear in attach()/sync() extras.`,
						);
					}
					if (Object.hasOwn(ts, k)) {
						throw new Error(
							`Pivot extras key '${k}' collides with a pivotTimestamps column on '${pivotTable}'. Disable the timestamp in the relation options or rename your extra.`,
						);
					}
				}
				const rowPairs = entries.map((e) => {
					const pairs: Array<[string, unknown]> = [
						[pivotFk, parentIdValue],
						[pivotOther, e.id],
					];
					for (const k of extraKeys) {
						const raw = e.extras[k] ?? null;
						const prepare = pivotAdapters?.[k]?.prepare;
						if (!prepare) {
							pairs.push([k, raw]);
							continue;
						}
						let encoded: unknown;
						try {
							encoded = prepare(raw);
						} catch (err) {
							throw wrapAdapterError("prepare", k, err);
						}
						assertNotPromise("prepare", k, encoded);
						pairs.push([k, encoded]);
					}
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
				await db.execute(compiled.statements[0], compiled.params);
			};

			/**
			 * Diff the current pivot state against a target set and apply the
			 * minimum attach/detach to converge.
			 *
			 * **NOT ATOMIC.** `sync` reads the pivot, computes the diff, then
			 * writes — another process mutating the pivot between the read and
			 * the writes will cause divergence. Wrap the call in a transaction
			 * if you need strong consistency under concurrent writers.
			 *
			 * On SQLite this is typically fine because better-sqlite3 serializes
			 * writes per connection; on Postgres/MySQL use `useTransaction` first.
			 */
			const sync = async (
				target:
					| Array<string | number>
					| Record<string, Record<string, unknown>>,
				additive = false,
			): Promise<void> => {
				const current = new Set(await currentIds());
				const entries = normalizeAttach(target);
				const desired = new Set(entries.map((e) => e.id));
				const toAttach = entries.filter((e) => !current.has(e.id));
				const toDetach = additive
					? []
					: [...current].filter((id) => !desired.has(id));
				if (toDetach.length > 0) await detach(toDetach);
				if (toAttach.length > 0) {
					const attachArg: Record<string, Record<string, unknown>> = {};
					for (const e of toAttach) attachArg[String(e.id)] = e.extras;
					await attach(attachArg);
				}
			};

			// m2m create/save persist the related row THEN insert a pivot row —
			// NOT `hasOps.injectFk`, which would write a bogus `<parent>_id` column
			// onto the related table and never touch the pivot (silent corruption).
			const relatedPkProp = getPrimaryKey(relatedClass) ?? "id";
			const attachIds = (rows: BaseEntity[]): Promise<void> => {
				if (rows.length === 0) return Promise.resolve();
				const arg: Record<string, Record<string, unknown>> = {};
				for (const r of rows) arg[String(r[relatedPkProp])] = {};
				return attach(arg);
			};
			const m2mOps = {
				async create(data: Record<string, unknown>): Promise<BaseEntity> {
					const created = await relatedRepo.create(data);
					await attachIds([created]);
					return created;
				},
				async createMany(
					rows: Array<Record<string, unknown>>,
				): Promise<BaseEntity[]> {
					const created = await relatedRepo.createMany(rows);
					await attachIds(created);
					return created;
				},
				async save(related: BaseEntity): Promise<void> {
					await relatedRepo.save(related);
					await attachIds([related]);
				},
				async saveMany(related: BaseEntity[]): Promise<BaseEntity[]> {
					const saved = await relatedRepo.saveMany(related);
					await attachIds(saved);
					return saved;
				},
			};
			const proxy: ManyToManyRelationProxy = {
				type: "manyToMany",
				...m2mOps,
				query: scopedQuery,
				attach,
				detach,
				sync,
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
		if (pk === undefined || pk === null) {
			throw new EntityNotFoundError(this.#entityClass.name, {
				[this.#primaryKey]: pk,
			});
		}
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
				row[this.#dbColumn(col)] = this.#applyPrepare(col, value);
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
function wrapAdapterError(
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
function assertNotPromise(
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
