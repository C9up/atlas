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

import { type Faker, faker } from "@faker-js/faker";
import type { BaseEntity } from "../BaseEntity.js";
import { BaseRepository, type DatabaseConnection } from "../BaseRepository.js";
import { getPrimaryKey, getRelationMetadata } from "../decorators/entity.js";
import { getConnection } from "../services/db.js";

type EntityConstructor<T extends BaseEntity> = new () => T;

/** Resolves the factory that builds a related model (Lucid `.relation`). */
type RelationResolver = () => FactoryBuilder<BaseEntity>;

/** A queued `.with()` request, applied on the next create()/createMany(). */
interface WithRequest {
	name: string;
	count: number;
	callback?: (factory: FactoryBuilder<BaseEntity>) => void;
}

/**
 * Context handed to the `define` callback (Adonis Lucid parity). Currently just
 * a Faker instance for generating fake data. A callback that takes no argument
 * keeps working — it simply ignores the context.
 */
export interface FactoryContext {
	faker: Faker;
}

/** Attributes callback — receives {@link FactoryContext}, returns the row shape. */
type DefaultsFn = (ctx: FactoryContext) => Record<string, unknown>;

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

	/**
	 * Bind a connection (e.g. a transaction) used by subsequent
	 * `create`/`createMany` calls that pass no explicit `db` (Adonis Lucid
	 * `.client`). The binding persists until changed. Ideal for test isolation:
	 * `factory.client(trx).create()`.
	 */
	client(connection: DatabaseConnection): FactoryBuilder<T>;

	/**
	 * Like {@link client} but resolves a connection registered under `name` via
	 * atlas's connection registry (Adonis Lucid `.connection`). Throws if no
	 * connection is registered under that name.
	 */
	connection(name: string): FactoryBuilder<T>;

	/**
	 * Create and persist a single entity (fires lifecycle hooks via
	 * `repo.create`). `db` is optional when a connection was bound with
	 * {@link client} / {@link connection}.
	 */
	create(db?: DatabaseConnection): Promise<T>;

	/** Create and persist multiple entities (fires hooks per row). */
	createMany(count: number, db?: DatabaseConnection): Promise<T[]>;

	/**
	 * Build an entity INSTANCE without persisting it (Adonis Lucid `make`). The
	 * instance has NO primary key and `$isPersisted === false` — use it to
	 * exercise model logic (computed props, validation) with no DB round trip.
	 * For an instance that looks persisted (stub id, `$isPersisted === true`),
	 * use {@link makeStubbed}.
	 */
	make(): T;

	/** Build many un-persisted instances (Adonis Lucid `makeMany`). */
	makeMany(count: number): T[];

	/**
	 * Build an entity INSTANCE without persisting it (Lucid's `makeStubbed`).
	 * The instance is marked persisted and, unless the build already supplied a
	 * primary key, given a process-unique stub id — so it looks like a saved row
	 * (relations, serialization, `$isPersisted`) without touching the DB.
	 */
	makeStubbed(): T;

	/** Build many stubbed instances, each with its own stub id ({@link makeStubbed}). */
	makeStubbedMany(count: number): T[];

	/**
	 * Declare which factory builds a relation (Adonis Lucid `.relation`), so
	 * `.with(name)` can create related rows. `name` must match a relation
	 * property declared with `@HasMany`/`@HasOne`/`@BelongsTo`/`@ManyToMany`.
	 */
	relation(name: string, resolver: RelationResolver): FactoryBuilder<T>;

	/**
	 * Queue related rows to create together with the next `create`/`createMany`
	 * (Adonis Lucid `.with`). `count` defaults to 1 (ignored past 1 for hasOne).
	 * The callback receives the related factory to customize it (`merge`/`apply`).
	 *
	 * One level deep: the related rows are persisted through the parent's
	 * relation proxy (which wires the FK / pivot), so a nested `.with()` on the
	 * callback's factory is not itself applied. `.with()` runs on persistence
	 * only — `make`/`makeStubbed` ignore it.
	 */
	with(
		name: string,
		count?: number,
		callback?: (factory: FactoryBuilder<BaseEntity>) => void,
	): FactoryBuilder<T>;

	/**
	 * Register a callback that runs on the built entity INSTANCE before it is
	 * persisted (Adonis Lucid `.tap`). Multiple taps run in order. Applies to the
	 * instance-producing paths — `create`/`createMany`/`makeStubbed`/
	 * `makeStubbedMany` — and is reset after consumption; `make`/`makeMany`
	 * return plain data, so they ignore it.
	 */
	tap(fn: (entity: T) => void): FactoryBuilder<T>;

	/**
	 * Register a lifecycle hook that runs BEFORE the given event (Adonis Lucid
	 * factory `before`). `create` fires before the INSERT; `makeStubbed` fires
	 * before the stub is finalised (so it can assign the primary key). The
	 * callback receives the factory and the model instance. Persistent (declared
	 * once, applies to every build).
	 */
	before(
		event: "create" | "makeStubbed",
		callback: (factory: FactoryBuilder<T>, model: T) => void,
	): FactoryBuilder<T>;

	/**
	 * Register a lifecycle hook that runs AFTER the given event (Adonis Lucid
	 * factory `after`). `create` fires after the INSERT; `makeStubbed` after the
	 * stub is built. Persistent.
	 */
	after(
		event: "create" | "makeStubbed",
		callback: (factory: FactoryBuilder<T>, model: T) => void,
	): FactoryBuilder<T>;
}

/**
 * Define a model factory.
 *
 *     const UserFactory = factory(User, () => ({
 *       email: `user-${Date.now()}@test.com`,
 *       name: 'Test User',
 *     }))
 */
/**
 * Adonis Lucid `Factory.define(Model, callback).build()` entry point. `define`
 * captures the model and its defaults; `build()` returns the usable factory
 * builder. Equivalent to the one-call {@link factory} shorthand, which stays.
 *
 *     const UserFactory = Factory.define(User, ({ faker }) => ({
 *       email: faker.internet.email(),
 *     })).build()
 */
export const Factory = {
	define<T extends BaseEntity>(
		entityClass: EntityConstructor<T>,
		defaults: DefaultsFn,
	): { build(): FactoryBuilder<T> } {
		return { build: () => factory(entityClass, defaults) };
	},
};

export function factory<T extends BaseEntity>(
	entityClass: EntityConstructor<T>,
	defaults: DefaultsFn,
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

	// Relations: persistent factory resolvers keyed by relation name, plus the
	// transient `.with()` queue consumed by create()/createMany().
	const relations = new Map<string, RelationResolver>();
	let pendingWith: WithRequest[] = [];
	// Transient `.tap()` callbacks run on the built instance before persistence.
	let pendingTap: Array<(entity: T) => void> = [];
	// Persistent connection bound via `.client()`/`.connection()`, used when
	// create()/createMany() are called without an explicit `db`.
	let boundClient: DatabaseConnection | undefined;

	/** The connection to persist through: the explicit arg wins over the bound one. */
	const resolveConnection = (db?: DatabaseConnection): DatabaseConnection => {
		const conn = db ?? boundClient;
		if (!conn) {
			throw new Error(
				`Factory ${entityClass.name}: no connection — pass one to create()/createMany() or bind one with .client()/.connection().`,
			);
		}
		return conn;
	};

	// Persistent lifecycle hooks (Adonis Lucid factory before/after), by event.
	type Hook = (factory: FactoryBuilder<T>, model: T) => void;
	const beforeHooks: Record<"create" | "makeStubbed", Hook[]> = {
		create: [],
		makeStubbed: [],
	};
	const afterHooks: Record<"create" | "makeStubbed", Hook[]> = {
		create: [],
		makeStubbed: [],
	};

	// Primary-key property, resolved once, for stub-id assignment. Same fallback
	// as BaseRepository so a model without an explicit `@PrimaryKey` uses `id`.
	const primaryKey = getPrimaryKey(entityClass) ?? "id";

	/** Hydrate a fresh (not-yet-persisted) instance from a data object. */
	const newInstance = (data: Record<string, unknown>): T => {
		const entity = new entityClass();
		for (const [key, value] of Object.entries(data)) {
			entity.setProp(key, value);
		}
		return entity;
	};

	/** Build a stubbed instance: hydrate, run taps, give it a stub id if none was
	 * supplied, then mark persisted so it mirrors a fetched row. Taps run before
	 * `markAsPersisted` so tapped fields land in the clean snapshot. */
	const stub = (
		data: Record<string, unknown>,
		taps: Array<(entity: T) => void>,
	): T => {
		const entity = newInstance(data);
		for (const tap of taps) tap(entity);
		// before('makeStubbed') runs BEFORE the stub id, so a hook can set the PK.
		for (const hook of beforeHooks.makeStubbed) hook(builder, entity);
		// Assign a stub id only when neither the data NOR a before hook set the PK.
		if (entity[primaryKey] === undefined) {
			entity.setProp(primaryKey, ++stubIdCounter);
		}
		entity.markAsPersisted();
		for (const hook of afterHooks.makeStubbed) hook(builder, entity);
		return entity;
	};

	const buildData = (): Record<string, unknown> => {
		let data: Record<string, unknown> = {
			...defaults({ faker }),
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

	/** Build one raw row and clear transient state — the persistence paths
	 * (`create`/`createMany`) need the plain object, not an instance. */
	const consumeData = (): Record<string, unknown> => {
		const data = buildData();
		resetPending();
		return data;
	};

	/** Build `count` raw rows (distinct faker/Date values per row), then reset. */
	const consumeDataMany = (count: number): Record<string, unknown>[] => {
		const rows: Record<string, unknown>[] = [];
		for (let i = 0; i < count; i++) rows.push(buildData());
		resetPending();
		return rows;
	};

	const resetPending = (): void => {
		pendingOverrides = {};
		pendingRecursive = {};
		pendingStates = [];
		pendingTap = [];
		// Also clear queued relations, so `.with(...).make()` — which ignores
		// relations — cannot leak them into the NEXT create() (create/createMany
		// capture the queue before make() runs, so they are unaffected).
		pendingWith = [];
	};

	/**
	 * Persist one row. With no taps this is the plain `repo.create(data)` path,
	 * untouched. With taps, the instance is built, tapped, then saved — so a tap
	 * can mutate the model before the INSERT (Adonis Lucid `.tap`). `save` on a
	 * fresh instance runs the same create hooks as `create`.
	 */
	const persist = async (
		repo: BaseRepository<T>,
		data: Record<string, unknown>,
		taps: Array<(entity: T) => void>,
	): Promise<T> => {
		if (
			taps.length === 0 &&
			beforeHooks.create.length === 0 &&
			afterHooks.create.length === 0
		) {
			return repo.create(data);
		}
		const entity = newInstance(data);
		for (const tap of taps) tap(entity);
		for (const hook of beforeHooks.create) hook(builder, entity);
		await repo.save(entity);
		for (const hook of afterHooks.create) hook(builder, entity);
		return entity;
	};

	/**
	 * Persist the queued `.with()` relations for one just-created parent, routing
	 * through the parent's relation proxy so the FK / pivot is wired by the
	 * already-tested relation-write code (not re-derived here).
	 */
	const applyRelations = async (
		parent: T,
		reqs: WithRequest[],
		db: DatabaseConnection,
	): Promise<void> => {
		const meta = getRelationMetadata(entityClass);
		for (const req of reqs) {
			const relMeta = meta.find((r) => r.propertyKey === req.name);
			if (!relMeta) {
				throw new Error(
					`Factory .with('${req.name}'): '${req.name}' is not a declared relation on ${entityClass.name}`,
				);
			}
			const resolver = relations.get(req.name);
			if (!resolver) {
				throw new Error(
					`Factory .with('${req.name}'): no related factory — declare it with .relation('${req.name}', () => XFactory)`,
				);
			}
			const childFactory = resolver();
			if (req.callback) req.callback(childFactory);

			const proxy = parent.related(req.name);
			if (proxy.type === "hasMany") {
				await proxy.createMany(childFactory.makeMany(req.count));
			} else if (proxy.type === "hasOne") {
				await proxy.create(childFactory.make());
			} else if (proxy.type === "manyToMany") {
				// The m2m proxy's create() inserts the related row AND the pivot link.
				for (const row of childFactory.makeMany(req.count)) {
					await proxy.create(row);
				}
			} else if (proxy.type === "belongsTo") {
				// FK lives on the parent: create the owner, then associate re-saves
				// the parent with the FK set.
				const owner = await childFactory.create(db);
				await proxy.associate(owner);
			} else {
				throw new Error(
					`Factory .with('${req.name}'): '${relMeta.type}' relations are not supported`,
				);
			}
		}
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

		relation(name, resolver) {
			relations.set(name, resolver);
			return builder;
		},

		with(name, count = 1, callback) {
			pendingWith.push({ name, count, callback });
			return builder;
		},

		tap(fn) {
			pendingTap.push(fn);
			return builder;
		},

		before(event, callback) {
			beforeHooks[event].push(callback);
			return builder;
		},

		after(event, callback) {
			afterHooks[event].push(callback);
			return builder;
		},

		client(connection) {
			boundClient = connection;
			return builder;
		},

		connection(name) {
			const conn = getConnection(name);
			if (!conn) {
				throw new Error(
					`Factory ${entityClass.name}: no connection registered under '${name}'.`,
				);
			}
			boundClient = conn;
			return builder;
		},

		make() {
			// Lucid `make`: an UN-persisted instance (no PK, `$isPersisted` false).
			// Taps run on it, like the other instance-producing paths.
			const taps = pendingTap;
			const entity = newInstance(buildData());
			for (const tap of taps) tap(entity);
			resetPending();
			return entity;
		},

		makeMany(count) {
			// Re-evaluate defaults for each row so `Date.now()` / faker generate
			// distinct values, each hydrated into its own un-persisted instance.
			const taps = pendingTap;
			const rows: T[] = [];
			for (let i = 0; i < count; i++) {
				const entity = newInstance(buildData());
				for (const tap of taps) tap(entity);
				rows.push(entity);
			}
			resetPending();
			return rows;
		},

		makeStubbed() {
			const taps = pendingTap;
			const data = buildData();
			const entity = stub(data, taps);
			resetPending();
			return entity;
		},

		makeStubbedMany(count) {
			// Re-evaluate defaults per row (distinct Date.now()/faker values), same
			// as makeMany; each row then gets its own stub id + taps via `stub`.
			const taps = pendingTap;
			const rows: T[] = [];
			for (let i = 0; i < count; i++) {
				rows.push(stub(buildData(), taps));
			}
			resetPending();
			return rows;
		},

		async create(db) {
			// Capture + clear the relation/tap queues, then consume the raw data
			// (which resets the rest of the pending state) — BEFORE resolveConnection,
			// so a missing connection can't throw with pending overrides/states/with/
			// tap still dirty and leak them into the next create().
			const withReqs = pendingWith;
			pendingWith = [];
			const taps = pendingTap;
			const data = consumeData();
			const conn = resolveConnection(db);
			const repo = new BaseRepository(entityClass, conn);
			const entity = await persist(repo, data, taps);
			if (withReqs.length > 0) await applyRelations(entity, withReqs, conn);
			return entity;
		},

		async createMany(count, db) {
			const withReqs = pendingWith;
			pendingWith = [];
			const taps = pendingTap;
			const rows = consumeDataMany(count);
			const conn = resolveConnection(db);
			const repo = new BaseRepository(entityClass, conn);
			const created: T[] = [];
			for (const data of rows) {
				const entity = await persist(repo, data, taps);
				// Each created parent gets its own related rows (Lucid semantics).
				if (withReqs.length > 0) await applyRelations(entity, withReqs, conn);
				created.push(entity);
			}
			return created;
		},
	};

	return builder;
}
