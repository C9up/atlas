/**
 * BaseEntity — base class for all Atlas entities.
 *
 * Provides:
 * - Domain event accumulation (flushed post-commit through the event bus)
 * - `$extras` bag for ad-hoc / computed columns (32.5)
 * - `$original` snapshot + `$dirty` diff tracking (32.2)
 * - Serialization layer hooks (`hidden` / `visible` / `@column` serializeAs) (32.4)
 * - Computed-property collection (32.3)
 *
 * @implements FR29, FR35, stories 32.1 through 32.5
 */

import {
	getColumnMetadata,
	getDateColumnConfig,
	getPrimaryKey,
	getRelationMetadata,
} from "./decorators/entity.js";
import { AtlasError, MassAssignmentError } from "./errors.js";
import {
	COLUMN_SERIALIZE_KEY,
	COMPUTED_KEY,
	type ColumnSerializeConfig,
} from "./metadata-keys.js";

export interface DomainEvent {
	name: string;
	data: Record<string, unknown>;
}

// COMPUTED_KEY / COLUMN_SERIALIZE_KEY / ColumnSerializeConfig moved to
// ./metadata-keys.js to break the BaseEntity ↔ entity-decorator runtime cycle
// (fallow 2026-06-14). Re-exported here for backward compatibility.
export { COLUMN_SERIALIZE_KEY, COMPUTED_KEY };

/** Symbol property key used by entities to back-reference their hydrating repo. */
export const REPO_REF = Symbol.for("atlas:repoRef");

/** Minimal repo-back-reference interface used by `entity.refresh()` / `entity.fresh()` / `entity.loadCount()`. */
export interface EntityRepoRef {
	refresh(entity: BaseEntity): Promise<void>;
	fresh(entity: BaseEntity): Promise<BaseEntity>;
	loadCount(
		entity: BaseEntity,
		relationName: string,
		alias?: string,
	): Promise<void>;
	loadAggregate(
		entity: BaseEntity,
		relationName: string,
		build: (q: unknown) => void,
	): Promise<void>;
	/** Lazy-load a relation onto `entity` — Story 31.10. */
	loadRelation(
		entity: BaseEntity,
		relationName: string,
		callback?: (q: unknown) => void,
	): Promise<void>;
	/** Build a related-entity proxy for fluent create/save + pivot ops — Stories 31.5–31.9. */
	relatedProxy(entity: BaseEntity, relationName: string): RelationProxy;
}

// ─── Relation proxy discriminated union ─────────────────────────────────────
// Stories 31.5–31.9. Each relation type returns a proxy with a `type`
// discriminator so `user.related('skills').type === 'manyToMany' && …` narrows
// to the m2m-specific methods and TS catches misuse at compile time.

interface BaseRelationProxy {
	create(data: Record<string, unknown>): Promise<BaseEntity>;
	save(related: BaseEntity): Promise<void>;
	/** Scoped query builder over the related table (Story 31.9). */
	query(): unknown;
}

interface BulkRelationProxy extends BaseRelationProxy {
	createMany(rows: Array<Record<string, unknown>>): Promise<BaseEntity[]>;
	saveMany(related: BaseEntity[]): Promise<BaseEntity[]>;
}

/** Relation upsert helpers scoped to the parent's FK (AdonisJS hasOne/hasMany). */
interface RelationUpsertProxy {
	firstOrCreate(
		search: Record<string, unknown>,
		defaults?: Record<string, unknown>,
	): Promise<BaseEntity>;
	updateOrCreate(
		search: Record<string, unknown>,
		values: Record<string, unknown>,
	): Promise<BaseEntity>;
}

/** `@HasOne` — single related row. `createMany`/`saveMany` are intentionally absent. */
export interface HasOneRelationProxy
	extends BaseRelationProxy,
		RelationUpsertProxy {
	readonly type: "hasOne";
	/** Throws with a clear "not supported on @HasOne" — exposed as a typed no-op for symmetry. */
	createMany(rows: Array<Record<string, unknown>>): Promise<never>;
	saveMany(related: BaseEntity[]): Promise<never>;
}

/** `@HasMany` — zero or more related rows with bulk write support. */
export interface HasManyRelationProxy
	extends BulkRelationProxy,
		RelationUpsertProxy {
	readonly type: "hasMany";
}

/**
 * `@BelongsTo` — the FK lives on THIS model, so the only writes are `associate`
 * (link an owner) / `dissociate` (clear it). create/save/createMany/saveMany are
 * NOT valid here (they'd inject the FK into the owner table and save this model
 * before it has an owner): they throw at runtime and are typed `Promise<never>`
 * so a caller who narrows to belongsTo gets a compile-time signal too — same
 * pattern as `@HasOne`'s bulk methods. AdonisJS Lucid's belongsTo client exposes
 * only associate/dissociate.
 */
export interface BelongsToRelationProxy extends BulkRelationProxy {
	readonly type: "belongsTo";
	create(data: Record<string, unknown>): Promise<never>;
	save(related: BaseEntity): Promise<never>;
	createMany(rows: Array<Record<string, unknown>>): Promise<never>;
	saveMany(related: BaseEntity[]): Promise<never>;
	/** Set `parent.<fk> = model.<ownerKey>` and save the parent. Rejects null/undefined. */
	associate(model: BaseEntity): Promise<void>;
	/** Clear the FK and save the parent. */
	dissociate(): Promise<void>;
}

/**
 * `@ManyToMany` — the pivot write API.
 *
 * Not the *full* Lucid surface, despite what this used to say. Still missing:
 * `pivotAttributes`/`performSync` on `save`/`saveMany`/`create`/`createMany`
 * (they attach with empty extras), a public `updatePivot` (it exists but only
 * `sync` calls it), `pivotQuery`, and an overridable `relatedKey` (the related
 * primary key is assumed).
 */
export interface ManyToManyRelationProxy extends BulkRelationProxy {
	readonly type: "manyToMany";
	/** Insert pivot rows. Accepts `id[]` or `{ id: extras }`. */
	attach(
		ids: Array<string | number> | Record<string, Record<string, unknown>>,
	): Promise<void>;
	/** Delete pivot rows. No args = delete all for this parent. */
	detach(ids?: Array<string | number>): Promise<void>;
	/**
	 * Diff-compute the target set. `additive=false` (default) removes orphans;
	 * `additive=true` only inserts, never deletes.
	 */
	sync(
		target: Array<string | number> | Record<string, Record<string, unknown>>,
		additive?: boolean,
	): Promise<void>;
}

/**
 * `@HasOneThrough` / `@HasManyThrough` — READ-ONLY two-hop relations. Lucid does
 * NOT expose persistence on a through relation (verified against the Lucid docs):
 * you persist via the intermediate model. `query()` traverses the through table;
 * create/save/createMany/saveMany throw at runtime and are typed `Promise<never>`
 * so a caller who narrows to a through relation gets a compile-time signal too.
 *
 * `@HasManyThrough` is Lucid parity. `@HasOneThrough` is an atlas addition —
 * Lucid has no such relation (checked against adonisjs/lucid `develop`:
 * `src/orm/relations/` holds belongs_to, has_many, has_many_through, has_one and
 * many_to_many, and no hasOneThrough appears in its types). It is the same
 * two-hop traversal returning a single row instead of an array.
 */
export interface HasManyThroughRelationProxy extends BulkRelationProxy {
	readonly type: "hasOneThrough" | "hasManyThrough";
	create(data: Record<string, unknown>): Promise<never>;
	save(related: BaseEntity): Promise<never>;
	createMany(rows: Array<Record<string, unknown>>): Promise<never>;
	saveMany(related: BaseEntity[]): Promise<never>;
}

export type RelationProxy =
	| HasOneRelationProxy
	| HasManyRelationProxy
	| BelongsToRelationProxy
	| ManyToManyRelationProxy
	| HasManyThroughRelationProxy;

export type { ColumnSerializeConfig };

/**
 * Internal reserved keys on BaseEntity that must never be treated as database
 * columns or serialized as data. Used by dirty tracking and by `toJSON`.
 */
const INTERNAL_KEYS = new Set<string>(["$extras", "$original", "$sideloaded"]);

/**
 * Structural (cross-realm-safe) check for a date value exposing `toISO()` — a
 * Chronos `DateTime` and any compatible instance from a duplicated package copy.
 * Used by dirty-tracking to compare date columns by instant, not by reference.
 */
function hasToISO(v: unknown): v is { toISO(): string } {
	return (
		typeof v === "object" &&
		v !== null &&
		"toISO" in v &&
		typeof v.toISO === "function"
	);
}

export class BaseEntity {
	/** Index signature — entities have dynamic column properties set by hydrate/create. */
	[key: string]: unknown;

	/** Accumulated domain events — dispatched on the event bus after DB commit. */
	#domainEvents: DomainEvent[] = [];

	/**
	 * `$extras` — bag for ad-hoc/computed values that are NOT declared as `@Column`.
	 * Used by `withCount`, pivot extras, and aggregate loaders. Kept separate from
	 * real columns so persistence (`#entityToRow`) never tries to write them back.
	 *
	 * @implements Story 32.5
	 */
	$extras: Record<string, unknown> = {};

	/**
	 * Sideloaded data — arbitrary context attached to the instance (AdonisJS Lucid
	 * `$sideloaded`), e.g. the current tenant/user threaded through from a query.
	 * Never a column, never dirty-tracked, never serialized. Set it manually;
	 * query-level `.sideload()` auto-propagation to hydrated results is not wired.
	 */
	$sideloaded: Record<string, unknown> = {};

	/**
	 * Snapshot of the column values at the moment this entity was hydrated from
	 * the database. Used by dirty tracking (`isDirty`, `$dirty`). Populated by
	 * `BaseRepository.#hydrate` via `markAsPersisted` below; empty for entities
	 * built in memory with `new MyEntity()`.
	 *
	 * @implements Story 32.2
	 */
	$original: Record<string, unknown> = {};

	// — Lifecycle state (AdonisJS Lucid parity). Kept in `#`-private fields so they
	//   never leak into `Object.keys(this)` / `$dirty` / `$original` / `toJSON`.
	#persisted = false;
	#deleted = false;
	#local = true;
	#forceUpdate = false;

	/**
	 * Force the next `save()` to run an UPDATE even when nothing is dirty — e.g. to
	 * fire DB triggers or re-persist the current state (AdonisJS Lucid
	 * `enableForceUpdate`). The flag is consumed by that save. Chainable.
	 */
	enableForceUpdate(): this {
		this.#forceUpdate = true;
		return this;
	}

	/** @internal Read-and-clear the force-update flag — called by `BaseRepository`. */
	$consumeForceUpdate(): boolean {
		const forced = this.#forceUpdate;
		this.#forceUpdate = false;
		return forced;
	}

	/**
	 * `true` once the row exists in the database — after `save()`/`create()`
	 * inserts it or a fetch hydrates it. AdonisJS Lucid `$isPersisted`.
	 */
	get $isPersisted(): boolean {
		return this.#persisted;
	}

	/** Inverse of {@link $isPersisted} — a never-persisted instance. Lucid `$isNew`. */
	get $isNew(): boolean {
		return !this.#persisted;
	}

	/**
	 * `true` when the instance originated in memory (`new Model()` / `create()`),
	 * `false` when it was fetched from the database. Lucid `$isLocal`.
	 */
	get $isLocal(): boolean {
		return this.#local;
	}

	/**
	 * `true` once `delete()` has removed the row; the instance must not be saved
	 * again. Lucid `$isDeleted`.
	 */
	get $isDeleted(): boolean {
		return this.#deleted;
	}

	/** The primary-key column's current value. Lucid `$primaryKeyValue`. */
	get $primaryKeyValue(): unknown {
		const pk = getPrimaryKey(this.constructor as new () => BaseEntity);
		return pk === undefined ? undefined : this[pk];
	}

	/** @internal Repository marks the instance deleted after DELETE. */
	markAsDeleted(): void {
		this.#deleted = true;
	}

	/** @internal Repository flags a DB-originated instance (`$isLocal = false`). */
	markAsFromDatabase(): void {
		this.#local = false;
	}

	/**
	 * @internal Revert a fresh INSERT that was rolled back — the row never
	 * persisted, so the instance must report `$isNew` again. Named safety
	 * deviation from Lucid (which keeps `$isPersisted` after rollback): without
	 * this, a later `parent.related('x').create(...)` reads `$isPersisted === true`,
	 * skips re-saving the parent, and writes a child with a foreign key pointing at
	 * a phantom row. Only used for instances that were provably not persisted
	 * before the failed batch.
	 */
	markAsNotPersisted(): void {
		this.#persisted = false;
		this.$original = {};
	}

	/** Set a property dynamically (used by hydrate/create). */
	setProp(key: string, value: unknown): void {
		this[key] = value;
	}

	/** Set an `$extras` value (used by `withCount`, pivot extras, aggregate loaders). */
	setExtra(key: string, value: unknown): void {
		this.$extras[key] = value;
	}

	/** Get an `$extras` value with optional default. */
	getExtra<T = unknown>(key: string, defaultValue?: T): T | undefined {
		return (this.$extras[key] as T | undefined) ?? defaultValue;
	}

	/**
	 * Freeze the current column values as the "persisted" snapshot. Called by
	 * `BaseRepository.#hydrate` after a SELECT and by `save()` after INSERT/UPDATE
	 * succeeds. From now on, `isDirty()` compares against this snapshot.
	 */
	/**
	 * Freeze the current column values as the persisted snapshot. Atlas uses
	 * **reference-based dirty tracking**: `$original` holds the SAME reference
	 * the hydrator produced, not a deep clone. A column is dirty iff
	 * `Object.is(current, original) === false`.
	 *
	 * **Contract (important)**: to mark an object/array column dirty, the user
	 * MUST reassign it — in-place mutation is undetectable by design:
	 *
	 *     entity.settings = { ...entity.settings, theme: 'dark' }  // ✅ dirty
	 *     entity.settings.theme = 'dark'                           // ❌ NOT detected
	 *
	 * This matches Lucid's contract and gives us O(1) dirty checks + zero
	 * allocations on hydrate. The alternative (deep-equal with cloned snapshot)
	 * was correct but allocated a full copy of every column on every load and
	 * traversed nested JSON on every `save()` — unacceptable for hot paths.
	 *
	 * Rollback can only restore reassigned columns; in-place mutations are
	 * unrecoverable because the snapshot is the same reference as the current
	 * value. Use immutable update patterns if you rely on rollback.
	 *
	 * @implements Story 32.2
	 */
	markAsPersisted(): void {
		const snapshot: Record<string, unknown> = {};
		for (const key of Object.keys(this)) {
			if (INTERNAL_KEYS.has(key)) continue;
			snapshot[key] = this[key];
		}
		this.$original = snapshot;
		this.#persisted = true;
	}

	/**
	 * Compute the set of columns whose current value differs from `$original`.
	 * Called on demand by `save()` (to emit UPDATEs that only touch dirty cols)
	 * and by lifecycle hooks (e.g. `beforeSave` only rehashes password if dirty).
	 *
	 * @implements Story 32.2
	 */
	get $dirty(): Record<string, unknown> {
		const diff: Record<string, unknown> = {};
		for (const key of Object.keys(this)) {
			if (INTERNAL_KEYS.has(key)) continue;
			if (!this.#columnEqualsOriginal(key)) {
				diff[key] = this[key];
			}
		}
		return diff;
	}

	/**
	 * Reference-based dirty comparison for a single column — O(1), no allocation.
	 * `Object.is` handles NaN correctly and treats same-reference objects as
	 * equal (the core of the tracking contract — see `markAsPersisted` doc).
	 *
	 * The only structural exception is `Date`: two Date instances representing
	 * the same instant are compared by `getTime()` so hydration through a driver
	 * that rebuilds Date objects from ISO strings doesn't flag spurious dirty.
	 */
	#columnEqualsOriginal(key: string): boolean {
		const current = this[key];
		const original = this.$original[key];
		if (current instanceof Date && original instanceof Date) {
			return current.getTime() === original.getTime();
		}
		// Chronos DateTime (or any `toISO()`-bearing value): compare by instant, not
		// reference, so re-wrapping the same moment (x = DateTime.from(x), toUTC(), a
		// driver rebuilding it) doesn't spuriously flag the column dirty.
		if (hasToISO(current) && hasToISO(original)) {
			return current.toISO() === original.toISO();
		}
		return Object.is(current, original);
	}

	/**
	 * Check whether a specific column is dirty, or whether any column is dirty
	 * when called without arguments.
	 */
	isDirty(field?: string): boolean {
		if (field === undefined) return Object.keys(this.$dirty).length > 0;
		return !this.#columnEqualsOriginal(field);
	}

	/**
	 * `true` when the instance has unsaved changes since it was hydrated/persisted
	 * (AdonisJS Lucid `$isDirty` getter). Equivalent to `isDirty()` with no args.
	 */
	get $isDirty(): boolean {
		return Object.keys(this.$dirty).length > 0;
	}

	/**
	 * Revert all dirty columns back to their `$original` values.
	 *
	 * Because dirty tracking is reference-based, `rollback` only restores
	 * reassigned columns to their persisted reference. In-place mutations on
	 * object/array columns are NOT recoverable — the snapshot holds the same
	 * reference the user mutated. If you rely on rollback, use immutable
	 * update patterns (`entity.field = { ...entity.field, x: y }`).
	 */
	rollback(): void {
		for (const key of Object.keys(this.$dirty)) {
			this[key] = this.$original[key];
		}
	}

	/**
	 * Repository back-pointer set by `BaseRepository.#hydrate` so instances can
	 * self-refresh / lazy-load. Not serialized (symbol key).
	 *
	 * @implements Story 32.6
	 */
	[REPO_REF]?: EntityRepoRef;

	/**
	 * Re-read this entity's row from the database and mutate THIS instance
	 * with the latest values. Throws if the row no longer exists.
	 */
	async refresh(): Promise<this> {
		const repo = this[REPO_REF];
		if (!repo)
			throw new Error(
				"refresh() requires the entity to be hydrated by a BaseRepository",
			);
		await repo.refresh(this);
		return this;
	}

	/**
	 * Re-read this entity's row from the database and return a NEW instance.
	 * `this` is NOT mutated. The returned object has the same runtime class
	 * because the repository that produced it is the same one we back-reference.
	 */
	async fresh(): Promise<BaseEntity> {
		const repo = this[REPO_REF];
		if (!repo)
			throw new Error(
				"fresh() requires the entity to be hydrated by a BaseRepository",
			);
		return repo.fresh(this);
	}

	/**
	 * Lazy-load a relation count into `this.$extras[alias ?? `${relationName}_count`]`.
	 * Issues a single `SELECT COUNT(*) FROM related WHERE <fk> = ?` for this entity.
	 *
	 * @implements Story 29.2
	 */
	async loadCount(relationName: string, alias?: string): Promise<this> {
		const repo = this[REPO_REF];
		if (!repo)
			throw new Error(
				"loadCount() requires the entity to be hydrated by a BaseRepository",
			);
		await repo.loadCount(this, relationName, alias);
		return this;
	}

	/**
	 * Lazy-load a relation aggregate. The builder callback must set the aggregate
	 * via `.sum('col')` / `.avg(...)` / `.min(...)` / `.max(...)` / `.count()` and
	 * an alias via `.as('name')`. The result lands on `this.$extras[alias]`.
	 *
	 *     await user.loadAggregate('posts', q => q.sum('views').as('total_views'))
	 *
	 * @implements Story 29.2
	 */
	async loadAggregate(
		relationName: string,
		build: (q: unknown) => void,
	): Promise<this> {
		const repo = this[REPO_REF];
		if (!repo)
			throw new Error(
				"loadAggregate() requires the entity to be hydrated by a BaseRepository",
			);
		await repo.loadAggregate(this, relationName, build);
		return this;
	}

	/**
	 * Lazy-load a relation onto this entity after it was initially fetched.
	 *
	 * @implements Story 31.10
	 */
	async load(
		relationName: string,
		callback?: (q: unknown) => void,
	): Promise<this> {
		const repo = this[REPO_REF];
		if (!repo)
			throw new Error(
				"load() requires the entity to be hydrated by a BaseRepository",
			);
		await repo.loadRelation(this, relationName, callback);
		return this;
	}

	/**
	 * Like {@link load} but a no-op when the relation is already populated on this
	 * instance (AdonisJS Lucid `loadOnce`). Chainable.
	 */
	async loadOnce(
		relationName: string,
		callback?: (q: unknown) => void,
	): Promise<this> {
		if (this[relationName] !== undefined) return this;
		return this.load(relationName, callback);
	}

	/**
	 * Return a relation proxy bound to this instance. The proxy exposes
	 * `create` / `createMany` / `save` / `saveMany` that auto-set the FK.
	 *
	 * @implements Story 31.5
	 */
	related(relationName: string): ReturnType<EntityRepoRef["relatedProxy"]> {
		const repo = this[REPO_REF];
		if (!repo)
			throw new Error(
				"related() requires the entity to be hydrated by a BaseRepository",
			);
		return repo.relatedProxy(this, relationName);
	}

	/**
	 * Mass-assign columns from a plain payload. Only columns that are in the
	 * class's `static fillable` allowlist (or absent from `static guarded` when
	 * no fillable is declared) are assigned. Columns not in the payload are
	 * reset to undefined so the entity reflects exactly what was filled.
	 *
	 * @implements Story 30.7
	 */
	fill(payload: Record<string, unknown>, allowExtraProperties = false): this {
		const ctor = this.constructor as typeof BaseEntity & {
			fillable?: string[];
			guarded?: string[];
		};
		if (ctor.fillable && ctor.guarded) {
			throw new Error(
				`${ctor.name}: cannot declare both 'fillable' and 'guarded'`,
			);
		}
		const known = this.#knownColumnKeys();
		const allowed = (key: string): boolean => {
			if (ctor.fillable) return ctor.fillable.includes(key);
			if (ctor.guarded) return !ctor.guarded.includes(key);
			return true;
		};
		// Reset fillable fields that are absent from the payload. To keep dirty
		// tracking honest, we restore the persisted `$original` reference (so the
		// column reads as clean) for hydrated entities, and `delete` the property
		// entirely for freshly-constructed ones (so `Object.keys(this)` doesn't
		// list a phantom undefined column).
		if (ctor.fillable) {
			const hasOriginal = Object.keys(this.$original).length > 0;
			for (const f of ctor.fillable) {
				if (!(f in payload)) {
					if (hasOriginal && f in this.$original) {
						this[f] = this.$original[f];
					} else {
						delete this[f];
					}
				}
			}
		}
		for (const [k, v] of Object.entries(payload)) {
			// Mass-assignment (fillable/guarded) is the more specific gate — it wins.
			if (!allowed(k)) throw new MassAssignmentError(ctor.name, k);
			// Otherwise reject keys that aren't declared columns at all (Lucid
			// strict), unless the caller opts into dropping extras.
			if (!known.has(k)) {
				if (allowExtraProperties) continue;
				throw new AtlasError(
					"E_EXTRA_PROPERTIES",
					`Cannot fill '${k}' on ${ctor.name}: it is not a declared column.`,
					{
						hint: "Declare it with @Column, or pass allowExtraProperties=true to ignore extra keys.",
					},
				);
			}
			this[k] = v;
		}
		return this;
	}

	/**
	 * The set of keys `fill`/`merge` treat as declared attributes: `@Column`
	 * property keys, the primary key, and any names the user listed in
	 * `static fillable` / `static guarded` (which reference real columns).
	 * Anything outside this set is an "extra property" rejected unless
	 * `allowExtraProperties` is passed.
	 */
	#knownColumnKeys(): Set<string> {
		const ctor = this.constructor as typeof BaseEntity & {
			fillable?: string[];
			guarded?: string[];
		};
		const keys = new Set(getColumnMetadata(ctor).map((c) => c.propertyKey));
		const pk = getPrimaryKey(ctor);
		if (pk) keys.add(pk);
		for (const k of ctor.fillable ?? []) keys.add(k);
		for (const k of ctor.guarded ?? []) keys.add(k);
		return keys;
	}

	/**
	 * Throw `MassAssignmentError` if `key` is blocked by the class's static
	 * `fillable` allowlist / `guarded` denylist — the SAME rule `fill()`/`merge()`
	 * enforce. Repositories call this in `create`/`createMany`/`updateOrCreate` so
	 * those paths cannot bypass mass-assignment protection (a `guarded` column
	 * like `role`/`isAdmin` must not be settable from a plain payload).
	 */
	assertMassAssignable(key: string): void {
		const ctor = this.constructor as typeof BaseEntity & {
			fillable?: string[];
			guarded?: string[];
		};
		if (ctor.fillable && ctor.guarded) {
			throw new Error(
				`${ctor.name}: cannot declare both 'fillable' and 'guarded'`,
			);
		}
		const allowed = ctor.fillable
			? ctor.fillable.includes(key)
			: ctor.guarded
				? !ctor.guarded.includes(key)
				: true;
		if (!allowed) throw new MassAssignmentError(ctor.name, key);
	}

	/**
	 * Patch the entity with a payload, only touching the provided keys. Same
	 * allowlist/blocklist rules as `fill` but preserves fields not present in
	 * the payload.
	 */
	merge(payload: Record<string, unknown>, allowExtraProperties = false): this {
		const ctor = this.constructor as typeof BaseEntity & {
			fillable?: string[];
			guarded?: string[];
		};
		if (ctor.fillable && ctor.guarded) {
			throw new Error(
				`${ctor.name}: cannot declare both 'fillable' and 'guarded'`,
			);
		}
		const known = this.#knownColumnKeys();
		const allowed = (key: string): boolean => {
			if (ctor.fillable) return ctor.fillable.includes(key);
			if (ctor.guarded) return !ctor.guarded.includes(key);
			return true;
		};
		for (const [k, v] of Object.entries(payload)) {
			if (!allowed(k)) {
				throw new MassAssignmentError(ctor.name, k);
			}
			if (!known.has(k)) {
				if (allowExtraProperties) continue;
				throw new AtlasError(
					"E_EXTRA_PROPERTIES",
					`Cannot merge '${k}' on ${ctor.name}: it is not a declared column.`,
					{
						hint: "Declare it with @Column, or pass allowExtraProperties=true to ignore extra keys.",
					},
				);
			}
			this[k] = v;
		}
		return this;
	}

	/** Add a domain event to be dispatched after save. */
	addDomainEvent(name: string, data: Record<string, unknown>): void {
		this.#domainEvents.push({ name, data });
	}

	/** Get accumulated domain events (non-destructive read). */
	getDomainEvents(): readonly DomainEvent[] {
		return [...this.#domainEvents];
	}

	/** Clear accumulated domain events. */
	clearDomainEvents(): void {
		this.#domainEvents = [];
	}

	/**
	 * Number of queued domain events — snapshot this BEFORE a transactional write
	 * so a rollback can drop only the events that write added (see
	 * {@link restoreDomainEventsTo}), preserving any the caller queued earlier.
	 */
	domainEventCount(): number {
		return this.#domainEvents.length;
	}

	/**
	 * Truncate the queued domain events back to a floor captured before a
	 * transactional write. On rollback this drops the tx-added events while
	 * KEEPING pre-existing ones (which describe work outside the rolled-back
	 * transaction). Events are append-only (`push`), so the first `n` are the
	 * pre-existing ones. A floor past the current length is a no-op.
	 */
	restoreDomainEventsTo(n: number): void {
		if (n < this.#domainEvents.length)
			this.#domainEvents.length = Math.max(0, n);
	}

	/** Get and clear accumulated domain events atomically. */
	flushDomainEvents(): DomainEvent[] {
		const events = [...this.#domainEvents];
		this.#domainEvents = [];
		return events;
	}

	/** Check if entity has pending domain events. */
	hasDomainEvents(): boolean {
		return this.#domainEvents.length > 0;
	}

	/**
	 * Serialize to JSON — honors class-level `hidden`/`visible` allowlists,
	 * per-column `serializeAs` / `serialize` overrides, and `@computed` getters.
	 * `$extras` is merged on top so callers see `withCount` / pivot extras next
	 * to regular columns. `#private` fields are excluded automatically by ES.
	 *
	 * @implements Story 32.4
	 */
	toJSON(): Record<string, unknown> {
		const ctor = this.constructor as typeof BaseEntity & {
			serializeExtras?:
				| boolean
				| ((extras: Record<string, unknown>) => Record<string, unknown>);
		};
		const result: Record<string, unknown> = {
			...this.serializeAttributes(),
			...this.serializeRelations(),
			...this.serializeComputed(),
		};

		// $extras (aggregates / pivot values) are serialized only when the model
		// opts in via `static serializeExtras = true` — AdonisJS Lucid parity
		// (default OFF), so internal aggregates never leak into API JSON by default.
		if (!ctor.serializeExtras) return result;
		const extras =
			typeof ctor.serializeExtras === "function"
				? ctor.serializeExtras(this.$extras)
				: this.$extras;
		return { ...result, ...extras };
	}

	/**
	 * Effective hidden/visible sets — class-level `static hidden`/`static visible`
	 * allowlists layered with per-instance `makeHidden`/`makeVisible` overrides.
	 */
	#visibility(): { hidden: Set<string>; visible: Set<string> | null } {
		const ctor = this.constructor as typeof BaseEntity & {
			hidden?: readonly string[];
			visible?: readonly string[];
		};
		const hidden = new Set(ctor.hidden ?? []);
		// makeHidden adds, makeVisible force-shows.
		for (const f of this.#hiddenOverride ?? []) hidden.add(f);
		for (const f of this.#visibleOverride ?? []) hidden.delete(f);
		const visible =
			ctor.visible && ctor.visible.length > 0 ? new Set(ctor.visible) : null;
		return { hidden, visible };
	}

	#isVisible(
		key: string,
		hidden: Set<string>,
		visible: Set<string> | null,
	): boolean {
		if (visible && !visible.has(key) && !this.#visibleOverride?.has(key))
			return false;
		return !hidden.has(key);
	}

	/**
	 * Serialize the regular `@column` attributes only — respecting hidden/visible
	 * allowlists and per-column `serializeAs`/`serialize` overrides. Override this
	 * to customize attribute serialization (AdonisJS Lucid `serializeAttributes`).
	 */
	protected serializeAttributes(): Record<string, unknown> {
		const ctor = this.constructor as typeof BaseEntity;
		const serializeConfig = getColumnSerializeConfig(ctor);
		const relKeys = new Set(
			getRelationMetadata(ctor).map((r) => r.propertyKey),
		);
		// Only DECLARED @column.date/dateTime columns get ISO'd — a business value
		// object that happens to expose toISO() on a non-date column is left intact.
		const dateCols = getDateColumnConfig(ctor);
		const { hidden, visible } = this.#visibility();
		const result: Record<string, unknown> = {};
		for (const key of Object.keys(this)) {
			if (INTERNAL_KEYS.has(key)) continue;
			if (relKeys.has(key)) continue; // handled by serializeRelations
			if (!this.#isVisible(key, hidden, visible)) continue;
			const cfg = serializeConfig[key];
			if (cfg?.serializeAs === null) continue; // explicit hide
			const outKey = cfg?.serializeAs ?? key;
			const rawValue = this[key];
			// A @column.dateTime value is a Chronos DateTime; serialize it to an ISO
			// string (AdonisJS Lucid serializes date columns to ISO), unless an
			// explicit @Column({ serialize }) override takes over.
			result[outKey] = cfg?.serialize
				? cfg.serialize(rawValue)
				: dateCols[key] && hasToISO(rawValue)
					? rawValue.toISO()
					: rawValue;
		}
		return result;
	}

	/**
	 * Serialize preloaded relations only — honouring each relation's `serializeAs`
	 * (rename, or `null` to hide). Nested entities serialize via their own
	 * `toJSON`. Override to customize (AdonisJS Lucid `serializeRelations`).
	 */
	protected serializeRelations(): Record<string, unknown> {
		const ctor = this.constructor as typeof BaseEntity;
		const relByKey = new Map(
			getRelationMetadata(ctor).map((r) => [r.propertyKey, r]),
		);
		const { hidden, visible } = this.#visibility();
		const result: Record<string, unknown> = {};
		for (const key of Object.keys(this)) {
			if (!this.#isVisible(key, hidden, visible)) continue;
			const rel = relByKey.get(key);
			if (!rel) continue;
			if (rel.serializeAs === null) continue;
			result[rel.serializeAs ?? key] = this[key];
		}
		return result;
	}

	/**
	 * Serialize `@computed` getters only. Override to customize
	 * (AdonisJS Lucid `serializeComputed`).
	 */
	protected serializeComputed(): Record<string, unknown> {
		const ctor = this.constructor as typeof BaseEntity;
		const { hidden, visible } = this.#visibility();
		const result: Record<string, unknown> = {};
		for (const prop of getComputedProperties(ctor)) {
			if (!this.#isVisible(prop, hidden, visible)) continue;
			result[prop] = this[prop];
		}
		return result;
	}

	// Per-instance serialization visibility.
	//
	// NOT Lucid parity, despite what these used to claim: `makeHidden` /
	// `makeVisible` and the `static hidden` / `static visible` allowlists do not
	// exist in Lucid (checked against adonisjs/lucid `develop` — LucidRow and
	// LucidModel declare neither). Lucid hides a column with
	// `@column({ serializeAs: null })`, which atlas also supports. This is an
	// atlas addition of Eloquent lineage, kept because per-instance visibility
	// is genuinely useful; it is a named deviation, not a Lucid feature.
	#hiddenOverride?: Set<string>;
	#visibleOverride?: Set<string>;

	/**
	 * Hide these fields when serializing THIS instance, on top of the class-level
	 * `static hidden`. Chainable.
	 *
	 * An atlas addition, not Lucid — see the `#hiddenOverride` note. Lucid's
	 * equivalent is the static `@column({ serializeAs: null })`, which atlas
	 * supports too; this is the per-instance form Lucid has no answer for.
	 */
	makeHidden(...fields: string[]): this {
		this.#hiddenOverride ??= new Set();
		for (const f of fields) this.#hiddenOverride.add(f);
		return this;
	}

	/**
	 * Force these fields visible when serializing THIS instance, overriding the
	 * class-level `static hidden`/`visible`. Chainable.
	 *
	 * An atlas addition, not Lucid — see {@link makeHidden}.
	 */
	makeVisible(...fields: string[]): this {
		this.#visibleOverride ??= new Set();
		for (const f of fields) this.#visibleOverride.add(f);
		return this;
	}

	/**
	 * Plain object of the raw columns + preloaded relations + `$extras`, WITHOUT
	 * any serialization transform (no `hidden`/`visible`, no `serializeAs`, no
	 * per-column `serialize`) — AdonisJS Lucid `toObject()`.
	 */
	toObject(): Record<string, unknown> {
		const out: Record<string, unknown> = {};
		for (const key of Object.keys(this)) {
			if (INTERNAL_KEYS.has(key)) continue;
			out[key] = this[key];
		}
		return { ...out, ...this.$extras };
	}

	/**
	 * Pick / limit the fields returned by `toJSON()` for a single call.
	 *
	 *     entity.serialize({ fields: ['id', 'title'] })
	 */
	serialize(options?: { fields?: readonly string[] }): Record<string, unknown> {
		const full = this.toJSON();
		if (!options?.fields) return full;
		const picked: Record<string, unknown> = {};
		for (const key of options.fields) {
			if (key in full) picked[key] = full[key];
		}
		return picked;
	}
}

// ─── Computed / serialize metadata accessors ────────────────────

/** Collect the names of all `@computed` getters declared on the prototype chain. */
function getComputedProperties(ctor: unknown): string[] {
	const names = new Set<string>();
	let current: object | null =
		typeof ctor === "object" || typeof ctor === "function"
			? (ctor as object | null)
			: null;
	while (current && current !== Function.prototype) {
		const list = Reflect.getOwnMetadata?.(COMPUTED_KEY, current) as
			| string[]
			| undefined;
		if (list) for (const n of list) names.add(n);
		current = Object.getPrototypeOf(current);
	}
	return [...names];
}

/** Collect the serialize config for every column declared on the prototype chain. */
function getColumnSerializeConfig(
	ctor: unknown,
): Record<string, ColumnSerializeConfig> {
	const config: Record<string, ColumnSerializeConfig> = {};
	let current: object | null =
		typeof ctor === "object" || typeof ctor === "function"
			? (ctor as object | null)
			: null;
	while (current && current !== Function.prototype) {
		const map = Reflect.getOwnMetadata?.(COLUMN_SERIALIZE_KEY, current) as
			| Record<string, ColumnSerializeConfig>
			| undefined;
		if (map) Object.assign(config, map);
		current = Object.getPrototypeOf(current);
	}
	return config;
}

// `equalsDeep` was intentionally removed with the move to reference-based
// dirty tracking (Story 32.2 perf revision). The single comparison path is
// `Object.is` in `#columnEqualsOriginal`, with `Date` as the only structural
// exception. If you find yourself wanting a deep-equal here, reach for an
// immutable update pattern at the call site instead — the framework does not
// traverse object columns at save time by design.
