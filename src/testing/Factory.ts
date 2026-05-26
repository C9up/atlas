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

type EntityConstructor<T extends BaseEntity> = new () => T;

/** A named state — mutates an in-progress data object in place. */
type StateFn<D> = (data: D) => void;

export interface FactoryBuilder<T extends BaseEntity> {
	/** Override specific fields for the next call (reset after consumption). */
	merge(overrides: Partial<Record<string, unknown>>): FactoryBuilder<T>;

	/**
	 * Declare a named variation of this factory. States are stored on the
	 * factory itself and don't mutate the caller — `apply()` returns a child
	 * builder with the state active.
	 */
	state(name: string, fn: StateFn<Record<string, unknown>>): FactoryBuilder<T>;

	/**
	 * Activate one or more declared states for the next call. Multiple applies
	 * compose (all applied states fire, in order).
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
	 * Useful when you need a `new User()` object but want to avoid the DB.
	 */
	makeStubbed(): T;
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
	let pendingStates: string[] = [];

	const buildData = (): Record<string, unknown> => {
		const data: Record<string, unknown> = {
			...defaults(),
			...pendingOverrides,
		};
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
		pendingStates = [];
	};

	const builder: FactoryBuilder<T> = {
		merge(overrides) {
			pendingOverrides = { ...pendingOverrides, ...overrides };
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
			// Re-evaluate defaults for each row so `Date.now()` / faker generate distinct values.
			const rows: Record<string, unknown>[] = [];
			const capturedOverrides = pendingOverrides;
			const capturedStates = pendingStates;
			for (let i = 0; i < count; i++) {
				pendingOverrides = capturedOverrides;
				pendingStates = capturedStates;
				rows.push(buildData());
			}
			resetPending();
			return rows;
		},

		makeStubbed() {
			const data = buildData();
			resetPending();
			const entity = new entityClass();
			for (const [key, value] of Object.entries(data)) {
				entity.setProp(key, value);
			}
			return entity;
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
