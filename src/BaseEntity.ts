/**
 * BaseEntity — base class for all Atlas entities.
 *
 * Provides:
 * - Domain event accumulation (flushed post-commit through Pulsar)
 * - `$extras` bag for ad-hoc / computed columns (32.5)
 * - `$original` snapshot + `$dirty` diff tracking (32.2)
 * - Serialization layer hooks (`hidden` / `visible` / `@column` serializeAs) (32.4)
 * - Computed-property collection (32.3)
 *
 * @implements FR29, FR35, stories 32.1 through 32.5
 */

import { MassAssignmentError } from "./errors.js";

export interface DomainEvent {
	name: string;
	data: Record<string, unknown>;
}

/** Symbol metadata key for the computed-property registry on an entity class. */
export const COMPUTED_KEY = Symbol.for("atlas:computed");

/** Symbol metadata key for the serialize-as / serializer overrides on columns. */
export const COLUMN_SERIALIZE_KEY = Symbol.for("atlas:columnSerialize");

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

/** `@HasOne` — single related row. `createMany`/`saveMany` are intentionally absent. */
export interface HasOneRelationProxy extends BaseRelationProxy {
	readonly type: "hasOne";
	/** Throws with a clear "not supported on @HasOne" — exposed as a typed no-op for symmetry. */
	createMany(rows: Array<Record<string, unknown>>): Promise<never>;
	saveMany(related: BaseEntity[]): Promise<never>;
}

/** `@HasMany` — zero or more related rows with bulk write support. */
export interface HasManyRelationProxy extends BulkRelationProxy {
	readonly type: "hasMany";
}

/** `@BelongsTo` — set/clear the FK via `associate`/`dissociate`. */
export interface BelongsToRelationProxy extends BulkRelationProxy {
	readonly type: "belongsTo";
	/** Set `parent.<fk> = model.<ownerKey>` and save the parent. Rejects null/undefined. */
	associate(model: BaseEntity): Promise<void>;
	/** Clear the FK and save the parent. */
	dissociate(): Promise<void>;
}

/** `@ManyToMany` — full Lucid pivot API. */
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

export type RelationProxy =
	| HasOneRelationProxy
	| HasManyRelationProxy
	| BelongsToRelationProxy
	| ManyToManyRelationProxy;

/** Per-column serialization config (populated by @Column options). */
export interface ColumnSerializeConfig {
	/** Rename this column at toJSON time (e.g. `password` → `passwordHash`). Null = hidden. */
	serializeAs?: string | null;
	/** Transform function applied to the value at toJSON time. */
	serialize?: (value: unknown) => unknown;
}

/**
 * Internal reserved keys on BaseEntity that must never be treated as database
 * columns or serialized as data. Used by dirty tracking and by `toJSON`.
 */
const INTERNAL_KEYS = new Set<string>(["$extras", "$original"]);

export class BaseEntity {
	/** Index signature — entities have dynamic column properties set by hydrate/create. */
	[key: string]: unknown;

	/** Accumulated domain events — dispatched on Pulsar after DB commit. */
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
	 * Snapshot of the column values at the moment this entity was hydrated from
	 * the database. Used by dirty tracking (`isDirty`, `$dirty`). Populated by
	 * `BaseRepository.#hydrate` via `markAsPersisted` below; empty for entities
	 * built in memory with `new MyEntity()`.
	 *
	 * @implements Story 32.2
	 */
	$original: Record<string, unknown> = {};

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
	fill(payload: Record<string, unknown>): this {
		const ctor = this.constructor as typeof BaseEntity & {
			fillable?: string[];
			guarded?: string[];
		};
		if (ctor.fillable && ctor.guarded) {
			throw new Error(
				`${ctor.name}: cannot declare both 'fillable' and 'guarded'`,
			);
		}
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
			if (!allowed(k)) throw new MassAssignmentError(ctor.name, k);
			this[k] = v;
		}
		return this;
	}

	/**
	 * Patch the entity with a payload, only touching the provided keys. Same
	 * allowlist/blocklist rules as `fill` but preserves fields not present in
	 * the payload.
	 */
	merge(payload: Record<string, unknown>): this {
		const ctor = this.constructor as typeof BaseEntity & {
			fillable?: string[];
			guarded?: string[];
		};
		if (ctor.fillable && ctor.guarded) {
			throw new Error(
				`${ctor.name}: cannot declare both 'fillable' and 'guarded'`,
			);
		}
		const allowed = (key: string): boolean => {
			if (ctor.fillable) return ctor.fillable.includes(key);
			if (ctor.guarded) return !ctor.guarded.includes(key);
			return true;
		};
		for (const [k, v] of Object.entries(payload)) {
			if (!allowed(k)) {
				throw new MassAssignmentError(ctor.name, k);
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
			hidden?: readonly string[];
			visible?: readonly string[];
		};
		const hidden = new Set(ctor.hidden ?? []);
		const visible =
			ctor.visible && ctor.visible.length > 0 ? new Set(ctor.visible) : null;

		const serializeConfig = getColumnSerializeConfig(ctor);
		const result: Record<string, unknown> = {};

		// Regular columns (respecting hidden/visible + serialize overrides)
		for (const key of Object.keys(this)) {
			if (INTERNAL_KEYS.has(key)) continue;
			if (visible && !visible.has(key)) continue;
			if (hidden.has(key)) continue;

			const cfg = serializeConfig[key];
			if (cfg?.serializeAs === null) continue; // explicit hide

			const outKey = cfg?.serializeAs ?? key;
			const rawValue = this[key];
			result[outKey] = cfg?.serialize ? cfg.serialize(rawValue) : rawValue;
		}

		// Computed getters (@computed on the prototype)
		const computed = getComputedProperties(ctor);
		for (const prop of computed) {
			if (visible && !visible.has(prop)) continue;
			if (hidden.has(prop)) continue;
			result[prop] = (this as Record<string, unknown>)[prop];
		}

		// $extras merged last — aggregates and pivot values show up alongside columns
		return { ...result, ...this.$extras };
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
