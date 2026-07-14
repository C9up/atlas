/**
 * Entity decorators — @Entity, @Column, @PrimaryKey, @BelongsTo, @HasMany, @computed
 *
 * @implements FR29, FR30, stories 32.3, 32.4
 */

import "reflect-metadata";
import {
	COLUMN_SERIALIZE_KEY,
	COMPUTED_KEY,
	type ColumnSerializeConfig,
} from "../metadata-keys.js";
import { getNamingStrategy } from "../naming/NamingStrategy.js";

const ENTITY_KEY = Symbol("atlas:entity");
const COLUMNS_KEY = Symbol("atlas:columns");
const PRIMARY_KEY = Symbol("atlas:primary");
const PRIMARY_KEY_GEN = Symbol("atlas:primary-gen");
const RELATIONS_KEY = Symbol("atlas:relations");

/** Auto-generation strategy for `@PrimaryKey({ generated: ... })`. */
export type PrimaryKeyGenerator = "uuid";

export interface PrimaryKeyOptions extends ColumnOptions {
	/**
	 * Auto-generate the primary-key value on INSERT when the entity has none
	 * (undefined). `'uuid'` produces an RFC-4122 v4 string via `crypto.randomUUID()`.
	 * Default: no auto-generation — the column relies on a DB default
	 * (`AUTOINCREMENT`, `SERIAL`, …) or the caller supplies a value.
	 */
	generated?: PrimaryKeyGenerator;
}

export interface EntityMetadata {
	tableName: string;
}

/**
 * Column adapter — the `{ prepare?, consume? }` callback pair shared between
 * `@Column({ prepare, consume })` (entity columns) and `pivotColumnAdapters`
 * (m2m pivot extras). Single source of truth for the adapter shape; both
 * `ColumnOptions` and `ColumnMetadata` extend this interface.
 */
export interface ColumnAdapter {
	/**
	 * Transform the model value before it is persisted (model → DB). Mirror of
	 * Adonis Lucid's `@column.prepare`.
	 *
	 * For entity columns, runs in `#entityToRow`, `#entityToRowPairs`,
	 * `#plainToRowPairs`, `#update`, and `#buildSetPairs`. For m2m pivot extras
	 * (`pivotColumnAdapters`), runs in the `attach()`/`sync()` row builder.
	 *
	 * **You MUST handle `null` and `undefined` yourself** — atlas calls the
	 * callback unconditionally for every value, including null/undefined, so a
	 * naive `(v: Decimal) => v.toString()` will throw on a null column. Return
	 * `null` for null/undefined inputs to preserve nullable semantics.
	 *
	 * For pivot extras specifically: when `attach()` is called with
	 * heterogeneous entries (the same extra key present on some entries and
	 * absent on others), the absent entries back-fill the missing key as
	 * `null` BEFORE `prepare` is called — your adapter must be null-safe even
	 * when no caller wrote null explicitly on that row.
	 *
	 * MUST stay synchronous; the bind layer cannot await before handing values
	 * to the Rust DML compiler. Returning a Promise throws.
	 */
	prepare?: (value: unknown) => unknown;
	/**
	 * Transform the raw DB value into the model attribute (DB → model). Mirror
	 * of Adonis Lucid's `@column.consume`. For entity columns, runs in
	 * `#hydrate`. For m2m pivot extras, runs in the `$extras.pivot_<col>`
	 * projection (currently dormant — see `pivotColumnAdapters` JSDoc).
	 *
	 * **You MUST handle `null` and `undefined` yourself** — atlas calls the
	 * callback unconditionally for every present row key, so a naive
	 * `(v: string) => v.trim()` will throw on a null column. Return `null` for
	 * null/undefined inputs to preserve nullable semantics.
	 *
	 * MUST stay synchronous (same constraint as `prepare`).
	 */
	consume?: (value: unknown) => unknown;
}

export interface ColumnMetadata extends ColumnAdapter {
	propertyKey: string;
	type?: string;
	nullable?: boolean;
	default?: unknown;
	serializeAs?: string | null;
	serialize?: (value: unknown) => unknown;
}

export interface ColumnOptions extends ColumnAdapter {
	type?: string;
	nullable?: boolean;
	default?: unknown;
	/** Rename this column at `toJSON` time. Use `null` to hide it entirely. */
	serializeAs?: string | null;
	/** Transform the value at `toJSON` time (e.g. mask a phone number, coerce a Date). */
	serialize?: (value: unknown) => unknown;
}

type Constructor = new (...args: unknown[]) => unknown;

export interface ManyToManyOptions {
	/** Pivot table name joining the two sides (e.g., 'users_roles'). */
	pivotTable: string;
	/** Foreign key in the pivot table pointing to THIS entity (default: `${thisTable}_id`). */
	foreignKey?: string;
	/** Foreign key in the pivot table pointing to the RELATED entity (default: `${relatedTable}_id`). */
	otherKey?: string;
	/** Pivot extra columns to project into `$extras.pivot_<col>` on loaded relations (Story 31.8). */
	pivotColumns?: string[];
	/** Auto-write `created_at`/`updated_at` on `attach`/`sync` (Story 31.8). */
	pivotTimestamps?:
		| boolean
		| { createdAt?: string | false; updatedAt?: string | false };
	/**
	 * Per-extra-column adapter map for typed pivot values. Mirrors
	 * `@Column({ prepare, consume })` for entity columns: `prepare` runs on
	 * every `attach()` / `sync()` write, before the value reaches the SQL bind
	 * layer; `consume` runs on every load that projects the extra into
	 * `$extras.pivot_<col>` (when the projection mechanism lands — currently
	 * the load-side hook is dormant).
	 *
	 * Keys are pivot-row column names as written in the SQL (e.g. `amount`);
	 * adapters are reused verbatim from `@c9up/atom/atlas` and friends so a
	 * caller can pass `{ amount: decimalAtlasAdapter }` without wrapping.
	 *
	 * Adapters that are NOT listed here keep current pass-through behaviour
	 * — backward compatible.
	 */
	pivotColumnAdapters?: Record<string, ColumnAdapter>;
}

/** Per-relation key/onQuery overrides (Story 31.3 + 31.4). */
export interface RelationOptions {
	/** Override the parent-side join column (default: parent PK). */
	localKey?: string;
	/** Override the child-side FK (default: `${parentSnake}_id` for hasOne/hasMany, `${relatedSnake}_id` on belongsTo parent row). */
	foreignKey?: string;
	/** `belongsTo` only — owner (target) side join column (default: related PK). */
	ownerKey?: string;
	/** Default constraint applied on every preload + lazy load (Story 31.4). */
	onQuery?: (q: unknown) => void;
	/** `toJSON()` key override — `null` hides the relation from serialization (Story 31.3). */
	serializeAs?: string | null;
}

/** Configuration for `@HasOneThrough` / `@HasManyThrough` — two-hop relations (Story 31.2). */
export interface ThroughOptions extends RelationOptions {
	/** FK on the *intermediate* table that points at the parent (default: `${parentSnake}_id`). */
	firstKey?: string;
	/** FK on the *related* table that points at the intermediate (default: `${intermediateSnake}_id`). */
	secondKey?: string;
	/** Parent-side local join column (default: parent PK). */
	localKey?: string;
	/** Intermediate-side local join column matched by `secondKey` (default: intermediate PK). */
	secondLocalKey?: string;
}

export interface RelationMetadata extends RelationOptions {
	propertyKey: string;
	type:
		| "belongsTo"
		| "hasOne"
		| "hasMany"
		| "hasOneThrough"
		| "hasManyThrough"
		| "manyToMany";
	target: () => Constructor;
	/** Intermediate ("through") model for two-hop relations — Story 31.2. */
	through?: () => Constructor;
	/** Extra through-specific keys. */
	firstKey?: string;
	secondKey?: string;
	secondLocalKey?: string;
	/** ManyToMany pivot configuration — required for type === 'manyToMany'. */
	pivot?: ManyToManyOptions;
}

/** @Entity('table_name') — marks a class as a database entity. */
export function Entity(tableName: string): ClassDecorator {
	return (target) => {
		Reflect.defineMetadata(ENTITY_KEY, { tableName }, target);
	};
}

/** @Column() — marks a property as a database column. */
export function Column(options?: ColumnOptions): PropertyDecorator {
	return (target, propertyKey) => {
		const columns: ColumnMetadata[] =
			Reflect.getOwnMetadata(COLUMNS_KEY, target.constructor) ?? [];
		const key = String(propertyKey);
		// Deduplicate — @PrimaryKey also calls Column()
		if (!columns.some((c) => c.propertyKey === key)) {
			columns.push({
				propertyKey: key,
				type: options?.type,
				nullable: options?.nullable,
				default: options?.default,
				serializeAs: options?.serializeAs,
				serialize: options?.serialize,
				prepare: options?.prepare,
				consume: options?.consume,
			});
			Reflect.defineMetadata(COLUMNS_KEY, columns, target.constructor);
		}

		// Register serialize overrides on a separate map read by BaseEntity.toJSON.
		// Stored per-class so subclasses can override a parent's serializeAs.
		if (
			options?.serializeAs !== undefined ||
			options?.serialize !== undefined
		) {
			const serializeMap: Record<string, ColumnSerializeConfig> =
				Reflect.getOwnMetadata(COLUMN_SERIALIZE_KEY, target.constructor) ?? {};
			serializeMap[key] = {
				serializeAs: options.serializeAs,
				serialize: options.serialize,
			};
			Reflect.defineMetadata(
				COLUMN_SERIALIZE_KEY,
				serializeMap,
				target.constructor,
			);
		}
	};
}

/**
 * @computed() — marks a getter as a computed column that shows up in `toJSON()`.
 * The getter is NEVER read or written during persistence (it's derived from other
 * real columns). Lucid-compatible alias.
 *
 * @implements Story 32.3
 */
export function computed(): MethodDecorator {
	return (target, propertyKey) => {
		const list: string[] =
			Reflect.getOwnMetadata(COMPUTED_KEY, target.constructor) ?? [];
		const key = String(propertyKey);
		if (!list.includes(key)) {
			list.push(key);
			Reflect.defineMetadata(COMPUTED_KEY, list, target.constructor);
		}
	};
}

// ─── Date / DateTime column sub-decorators (story 32.8) ──────────

export interface DateTimeColumnOptions extends ColumnOptions {
	/** Set the column to `new Date()` on INSERT (like `created_at`). */
	autoCreate?: boolean;
	/** Set the column to `new Date()` on every UPDATE (like `updated_at`). */
	autoUpdate?: boolean;
}

/** Symbol metadata key for per-column date-column config. Read by BaseRepository. */
export const DATE_COLUMNS_KEY = Symbol.for("atlas:dateColumns");

export interface DateColumnConfig {
	/** `true` means the column holds date-only values (no time). */
	dateOnly: boolean;
	autoCreate?: boolean;
	autoUpdate?: boolean;
}

function registerDateColumn(
	target: object,
	propertyKey: string | symbol,
	config: DateColumnConfig,
): void {
	const ctor = (target as { constructor: object }).constructor;
	const map: Record<string, DateColumnConfig> =
		Reflect.getOwnMetadata(DATE_COLUMNS_KEY, ctor) ?? {};
	map[String(propertyKey)] = config;
	Reflect.defineMetadata(DATE_COLUMNS_KEY, map, ctor);
}

/**
 * `@column.date()` — marks a property as a date-only column (YYYY-MM-DD).
 * The raw DB value is hydrated to a JS Date; nulls pass through.
 */
function columnDate(options?: ColumnOptions): PropertyDecorator {
	return (target, propertyKey) => {
		Column(options)(target, propertyKey);
		registerDateColumn(target, propertyKey, { dateOnly: true });
	};
}

/**
 * `@column.dateTime({ autoCreate, autoUpdate })` — marks a property as a
 * timestamp column. Hydrated to JS Date on read.
 *
 * `autoCreate`: BaseRepository sets it to `new Date()` on INSERT.
 * `autoUpdate`: BaseRepository sets it to `new Date()` on every UPDATE.
 *
 * **Time zones.** atlas writes timestamps as UTC (`Date.toISOString()`) and
 * reads them back as UTC (the driver emits a `Z`-suffixed ISO string), so a
 * value WRITTEN THROUGH atlas round-trips correctly regardless of the server's
 * `TZ` — including exact, to-the-second comparisons.
 *
 * The caveat is a Postgres `timestamp WITHOUT time zone` column populated
 * OUTSIDE atlas — a DB-side `DEFAULT now()` / `CURRENT_TIMESTAMP`, raw SQL, or a
 * seed — which stores the server's LOCAL wall-clock; atlas then reads it as UTC,
 * so on a non-UTC host the instant drifts by the server offset. For such columns
 * (DB-side defaults, external writers, or cross-source comparisons) use
 * `timestamptz` — it normalises every writer to UTC. Prefer `timestamptz` for
 * any timestamp you compare exactly.
 */
function columnDateTime(options?: DateTimeColumnOptions): PropertyDecorator {
	return (target, propertyKey) => {
		Column(options)(target, propertyKey);
		registerDateColumn(target, propertyKey, {
			dateOnly: false,
			autoCreate: options?.autoCreate,
			autoUpdate: options?.autoUpdate,
		});
	};
}

/** Namespace access so users write `@column.date()` / `@column.dateTime()`. */
const columnWithSubs = Column as typeof Column & {
	date: typeof columnDate;
	dateTime: typeof columnDateTime;
};
columnWithSubs.date = columnDate;
columnWithSubs.dateTime = columnDateTime;

/**
 * `Column` exposed with its sub-decorators (`Column.date()`, `Column.dateTime()`).
 * Alias exports so TS users can `import { column } from '@c9up/atlas'` for a
 * Lucid-style lowercase naming when they prefer.
 */
export const column = Column as typeof Column & {
	date: typeof columnDate;
	dateTime: typeof columnDateTime;
};

/** Read the date-column configuration map for an entity class (walks prototype chain). */
export function getDateColumnConfig(
	entityClass: object,
): Record<string, DateColumnConfig> {
	const merged: Record<string, DateColumnConfig> = {};
	let current: object | null = entityClass;
	while (current && current !== Function.prototype) {
		const map = Reflect.getOwnMetadata(DATE_COLUMNS_KEY, current) as
			| Record<string, DateColumnConfig>
			| undefined;
		if (map) Object.assign(merged, map);
		current = Object.getPrototypeOf(current);
	}
	return merged;
}

/** @PrimaryKey() — marks a property as the primary key. */
export function PrimaryKey(options?: PrimaryKeyOptions): PropertyDecorator {
	return (target, propertyKey) => {
		Reflect.defineMetadata(
			PRIMARY_KEY,
			String(propertyKey),
			target.constructor,
		);
		if (options?.generated) {
			Reflect.defineMetadata(
				PRIMARY_KEY_GEN,
				options.generated,
				target.constructor,
			);
		}
		// Register as a column, PROPAGATING the options so a typed PK records its
		// SQL type — needed for the `::uuid` cast in WHERE/INSERT (otherwise a
		// uuid PK compared to a bound string fails `operator does not exist:
		// uuid = text`). A uuid generator implies a uuid column type unless the
		// caller set one explicitly.
		const columnOptions: ColumnOptions = { ...options };
		if (columnOptions.type === undefined && options?.generated === "uuid") {
			columnOptions.type = "uuid";
		}
		Column(columnOptions)(target, propertyKey);
	};
}

/**
 * Read the PK auto-generation strategy declared via `@PrimaryKey({ generated })`.
 * Returns `undefined` when the entity's PK has no generator — caller-supplied
 * or DB-defaulted.
 */
export function getPrimaryKeyGenerator(
	entityClass: object,
): PrimaryKeyGenerator | undefined {
	let current: object | null = entityClass;
	while (current && current !== Function.prototype) {
		const gen = Reflect.getOwnMetadata(PRIMARY_KEY_GEN, current) as
			| PrimaryKeyGenerator
			| undefined;
		if (gen) return gen;
		current = Object.getPrototypeOf(current);
	}
	return undefined;
}

/** @BelongsTo(() => Related, { foreignKey, ownerKey, onQuery, serializeAs }) */
export function BelongsTo(
	target: () => Constructor,
	options: RelationOptions = {},
): PropertyDecorator {
	return (proto, propertyKey) => {
		addRelation(proto.constructor, {
			propertyKey: String(propertyKey),
			type: "belongsTo",
			target,
			...options,
		});
	};
}

/** @HasOne(() => Related, { localKey, foreignKey, onQuery, serializeAs }) */
export function HasOne(
	target: () => Constructor,
	options: RelationOptions = {},
): PropertyDecorator {
	return (proto, propertyKey) => {
		addRelation(proto.constructor, {
			propertyKey: String(propertyKey),
			type: "hasOne",
			target,
			...options,
		});
	};
}

/** @HasMany(() => Related, { localKey, foreignKey, onQuery, serializeAs }) */
export function HasMany(
	target: () => Constructor,
	options: RelationOptions = {},
): PropertyDecorator {
	return (proto, propertyKey) => {
		addRelation(proto.constructor, {
			propertyKey: String(propertyKey),
			type: "hasMany",
			target,
			...options,
		});
	};
}

/** @HasOneThrough(() => Related, () => Through, { firstKey, secondKey, localKey, secondLocalKey, onQuery }) */
export function HasOneThrough(
	target: () => Constructor,
	through: () => Constructor,
	options: ThroughOptions = {},
): PropertyDecorator {
	return (proto, propertyKey) => {
		addRelation(proto.constructor, {
			propertyKey: String(propertyKey),
			type: "hasOneThrough",
			target,
			through,
			...options,
		});
	};
}

/** @HasManyThrough(() => Related, () => Through, { firstKey, secondKey, localKey, secondLocalKey, onQuery }) */
export function HasManyThrough(
	target: () => Constructor,
	through: () => Constructor,
	options: ThroughOptions = {},
): PropertyDecorator {
	return (proto, propertyKey) => {
		addRelation(proto.constructor, {
			propertyKey: String(propertyKey),
			type: "hasManyThrough",
			target,
			through,
			...options,
		});
	};
}

/** @ManyToMany(() => Related, { pivotTable: 'users_roles' }) */
export function ManyToMany(
	target: () => Constructor,
	options: ManyToManyOptions & RelationOptions,
): PropertyDecorator {
	return (proto, propertyKey) => {
		addRelation(proto.constructor, {
			propertyKey: String(propertyKey),
			type: "manyToMany",
			target,
			pivot: options,
			onQuery: options.onQuery,
			serializeAs: options.serializeAs,
		});
	};
}

function addRelation(target: object, relation: RelationMetadata): void {
	const relations: RelationMetadata[] =
		Reflect.getOwnMetadata(RELATIONS_KEY, target) ?? [];
	relations.push(relation);
	Reflect.defineMetadata(RELATIONS_KEY, relations, target);
}

/** Get entity metadata for a class. */
export function getEntityMetadata(
	target: Constructor,
): EntityMetadata | undefined {
	return Reflect.getMetadata(ENTITY_KEY, target);
}

/**
 * Return the class's `@Entity` metadata, SYNTHESIZING it when the decorator is
 * absent: the table name is inferred from the class name via the naming strategy
 * (or an explicit `static table`). AdonisJS Lucid parity — a model needs no
 * explicit `@Entity('table')`. Used by both `BaseModel` and the `BaseRepository`
 * constructor so the Data-Mapper and Active-Record paths share one convention.
 */
export function ensureEntityMetadata(target: Constructor): EntityMetadata {
	const existing = getEntityMetadata(target);
	if (existing) return existing;
	const staticTable = (target as { table?: string }).table;
	const table = staticTable ?? getNamingStrategy(target).tableName(target.name);
	Entity(table)(target);
	return getEntityMetadata(target) ?? { tableName: table };
}

/** Get column metadata for a class (returns a copy). */
export function getColumnMetadata(target: Constructor): ColumnMetadata[] {
	return [...(Reflect.getMetadata(COLUMNS_KEY, target) ?? [])];
}

/** Get primary key property name. A `static primaryKey` (AdonisJS Lucid) wins over the `@PrimaryKey()` decorator. */
export function getPrimaryKey(target: Constructor): string | undefined {
	const staticPk = (target as { primaryKey?: string }).primaryKey;
	if (staticPk) return staticPk;
	return Reflect.getMetadata(PRIMARY_KEY, target);
}

/** Get relation metadata for a class (returns a copy). */
export function getRelationMetadata(target: Constructor): RelationMetadata[] {
	return [...(Reflect.getMetadata(RELATIONS_KEY, target) ?? [])];
}

// ─── Soft Deletes ────────────────────────────────────────────

const SOFT_DELETE_KEY = Symbol("atlas:softDeletes");

/** @SoftDeletes() — marks entity as soft-deletable via deleted_at column. */
export function SoftDeletes(): ClassDecorator {
	return (target) => {
		Reflect.defineMetadata(SOFT_DELETE_KEY, true, target);
		// Auto-add deleted_at as a column
		const columns: ColumnMetadata[] =
			Reflect.getOwnMetadata(COLUMNS_KEY, target) ?? [];
		if (!columns.some((c) => c.propertyKey === "deletedAt")) {
			columns.push({
				propertyKey: "deletedAt",
				type: "timestamp",
				nullable: true,
			});
			Reflect.defineMetadata(COLUMNS_KEY, columns, target);
		}
	};
}

/** Check if an entity class uses soft deletes. */
export function hasSoftDeletes(target: Constructor): boolean {
	return Reflect.getMetadata(SOFT_DELETE_KEY, target) === true;
}
