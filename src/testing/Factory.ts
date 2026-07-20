/**
 * Model Factory — Lucid-compatible factory surface for generating test data.
 *
 * Supports named states (variations), relations (nested `with`), many-to-many
 * pivot attributes, stubbing (build without persisting), and ad-hoc merges.
 *
 *     const UserFactory = factory(User, () => ({ email: `user-${Date.now()}@test.com`, name: 'Test' }))
 *       .state('admin', (u) => { u.role = 'admin' })
 *
 *     const user = await UserFactory.create(db)
 *     const users = await UserFactory.apply('admin').createMany(5, db)
 *     const draft = UserFactory.merge({ name: 'Alice' }).makeStubbed()
 *
 * @implements MISS-18, Story 32.13
 */

import type { BaseEntity } from "../BaseEntity.js";
import { BaseRepository, type DatabaseConnection } from "../BaseRepository.js";
import { getPrimaryKey } from "../decorators/entity.js";

type EntityConstructor<T extends BaseEntity> = new () => T;

/** A named state — mutates an in-progress data object in place. */
type StateFn<D> = (data: D) => void;

/**
 * Process-wide counter for stubbed primary keys (Lucid's stub id). Every
 * `makeStubbed*` build with no explicit PK gets the next value, so stubbed
 * instances have stable, distinct, DB-free identifiers — enough to look
 * persisted and to key relations/serialization without a round trip.
 */
let stubIdCounter = 0;

/** Keys that must never be written through a recursive merge — prototype-pollution guard. */
const FORBIDDEN_MERGE_KEYS = new Set(["__proto__", "constructor", "prototype"]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
	if (value === null || typeof value !== "object") return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
}

/**
 * Deep-merge `source` into `target` (Lucid `mergeRecursive`). Plain objects are
 * merged key by key; arrays and every non-plain value replace wholesale. Returns
 * a fresh object — neither argument is mutated. Skips prototype-pollution keys.
 */
function deepMerge(
	target: Record<string, unknown>,
	source: Record<string, unknown>,
): Record<string, unknown> {
	const out: Record<string, unknown> = { ...target };
	for (const [key, value] of Object.entries(source)) {
		if (FORBIDDEN_MERGE_KEYS.has(key)) continue;
		const existing = out[key];
		out[key] =
			isPlainObject(existing) && isPlainObject(value)
				? deepMerge(existing, value)
				: value;
	}
	return out;
}

export interface FactoryBuilder<T extends BaseEntity> {
	/** Override specific fields for the next call (reset after consumption). */
	merge(overrides: Partial<Record<string, unknown>>): FactoryBuilder<T>;

	/**
	 * Like {@link merge} but deep — nested plain objects are merged key by key
	 * instead of replaced wholesale (Lucid `mergeRecursive`). Arrays and
	 * non-plain values still replace. Reset after consumption like `merge`.
	 */
	mergeRecursive(
		overrides: Partial<Record<string, unknown>>,
	): FactoryBuilder<T>;

	/** Declare a named variation of this factory, stored on the factory's state map. */
	state(name: string, fn: StateFn<Record<string, unknown>>): FactoryBuilder<T>;

	/**
	 * Activate one or more declared states for the NEXT build. Multiple applies
	 * compose (all fire, in order). NOTE: this mutates the builder's shared
	 * pending state and returns the SAME builder for chaining — there is no
	 * isolated child builder. The pending set is reset after each make/create
	 * (audit 2026-06-13). `merge()` behaves the same way.
	 */
	apply(...stateNames: string[]): FactoryBuilder<T>;

	/** Create and persist a single entity (fires lifecycle hooks via `repo.create`). */
	create(db: DatabaseConnection): Promise<T>;

	/** Create and persist multiple entities (fires hooks per row). */
	createMany(count: number, db: DatabaseConnection): Promise<T[]>;

	/** Build the data object without persisting and without instantiating an entity. */
	make(): Record<string, unknown>;

	/** Build multiple data objects without persisting. */
	makeMany(count: number): Record<string, unknown>[];

	/**
	 * Build an entity INSTANCE without persisting it (Lucid's `makeStubbed`).
	 * The instance is marked persisted and, unless the build already supplied a
	 * primary key, given a process-unique stub id — so it looks like a saved row
	 * (relations, serialization, `$isPersisted`) without touching the DB.
	 */
	makeStubbed(): T;

	/** Build many stubbed instances, each with its own stub id ({@link makeStubbed}). */
	makeStubbedMany(count: number): T[];
}

/**
 * Define a model factory.
 *
 *     const UserFactory = factory(User, () => ({
 *       email: `user-${Date.now()}@test.com`,
 *       name: 'Test User',
 *     }))
 */
export function factory<T extends BaseEntity>(
	entityClass: EntityConstructor<T>,
	defaults: () => Record<string, unknown>,
): FactoryBuilder<T> {
	// Persistent state — lives across calls.
	const states = new Map<string, StateFn<Record<string, unknown>>>();
	// Transient state — resets after every `make`/`create`.
	let pendingOverrides: Partial<Record<string, unknown>> = {};
	// Recursive overrides are kept apart from the shallow `pendingOverrides`
	// because they must deep-merge into `defaults()`, which only exists at build
	// time — a shallow spread here would clobber a whole nested object.
	let pendingRecursive: Record<string, unknown> = {};
	let pendingStates: string[] = [];

	// Primary-key property, resolved once, for stub-id assignment. Same fallback
	// as BaseRepository so a model without an explicit `@PrimaryKey` uses `id`.
	const primaryKey = getPrimaryKey(entityClass) ?? "id";

	/** Build a stubbed instance from a data object: hydrate, give it a stub id if
	 * none was supplied, then mark persisted so it mirrors a fetched row. */
	const stub = (data: Record<string, unknown>): T => {
		const entity = new entityClass();
		for (const [key, value] of Object.entries(data)) {
			entity.setProp(key, value);
		}
		if (data[primaryKey] === undefined) {
			entity.setProp(primaryKey, ++stubIdCounter);
		}
		entity.markAsPersisted();
		return entity;
	};

	const buildData = (): Record<string, unknown> => {
		let data: Record<string, unknown> = {
			...defaults(),
			...pendingOverrides,
		};
		// Deep overrides layer on top of the shallow one so nested defaults survive.
		if (Object.keys(pendingRecursive).length > 0) {
			data = deepMerge(data, pendingRecursive);
		}
		for (const name of pendingStates) {
			const fn = states.get(name);
			if (!fn)
				throw new Error(
					`Factory state '${name}' is not defined on ${entityClass.name}Factory`,
				);
			fn(data);
		}
		return data;
	};

	const resetPending = (): void => {
		pendingOverrides = {};
		pendingRecursive = {};
		pendingStates = [];
	};

	const builder: FactoryBuilder<T> = {
		merge(overrides) {
			pendingOverrides = { ...pendingOverrides, ...overrides };
			return builder;
		},

		mergeRecursive(overrides) {
			pendingRecursive = deepMerge(pendingRecursive, overrides);
			return builder;
		},

		state(name, fn) {
			states.set(name, fn);
			return builder;
		},

		apply(...names) {
			pendingStates.push(...names);
			return builder;
		},

		make() {
			const data = buildData();
			resetPending();
			return data;
		},

		makeMany(count) {
			// Re-evaluate defaults for each row so `Date.now()` / faker generate
			// distinct values. `buildData()` reads (never mutates) the pending
			// overrides/states, so they stay stable across iterations on their own.
			const rows: Record<string, unknown>[] = [];
			for (let i = 0; i < count; i++) {
				rows.push(buildData());
			}
			resetPending();
			return rows;
		},

		makeStubbed() {
			const data = buildData();
			resetPending();
			return stub(data);
		},

		makeStubbedMany(count) {
			// Re-evaluate defaults per row (distinct Date.now()/faker values), same
			// as makeMany; each row then gets its own stub id via `stub`.
			const rows: T[] = [];
			for (let i = 0; i < count; i++) {
				rows.push(stub(buildData()));
			}
			resetPending();
			return rows;
		},

		async create(db) {
			const data = builder.make();
			const repo = new BaseRepository(entityClass, db);
			return repo.create(data);
		},

		async createMany(count, db) {
			const rows = builder.makeMany(count);
			const repo = new BaseRepository(entityClass, db);
			const created: T[] = [];
			for (const data of rows) {
				created.push(await repo.create(data));
			}
			return created;
		},
	};

	return builder;
}
