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
import { transaction } from "../Transaction.js";

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
 * Per-factory internals reached across factory closures to support NESTED
 * `.with()` (Adonis Lucid: `post.with('comments', 5)` inside a parent's `.with`
 * callback). Kept off the public {@link FactoryBuilder} type and keyed by the
 * builder object in a {@link WeakMap} — no `any`/cast, no leaked API surface.
 */
interface FactoryInternals {
	/** Take and clear the queued `.with()` requests (before a `make` clears them). */
	consumeWith(): WithRequest[];
	/** Take and clear the queued m2m `.pivotAttributes()` (set in a `.with` callback). */
	consumePivot():
		| Record<string, unknown>
		| Array<Record<string, unknown>>
		| undefined;
	/** Persist this factory's `.with()` relations onto an already-created parent. */
	applyRelations(
		parent: BaseEntity,
		reqs: WithRequest[],
		db: DatabaseConnection,
	): Promise<void>;
}

const factoryInternals = new WeakMap<object, FactoryInternals>();

/**
 * Runtime context handed to the factory callbacks (Adonis Lucid parity):
 * `faker` for fake data, `isStubbed` (true during a `makeStubbed*` build), and
 * `$trx` — the bound transaction/connection, if any, so a hook's own DB queries
 * commit/roll back alongside the factory. A callback that ignores it keeps
 * working.
 */
export interface FactoryContext {
	faker: Faker;
	isStubbed: boolean;
	$trx?: DatabaseConnection;
}

/** Attributes callback — receives {@link FactoryContext}, returns the row shape. */
type DefaultsFn = (ctx: FactoryContext) => Record<string, unknown>;

/**
 * A named state — mutates the built model INSTANCE (Adonis Lucid: states receive
 * the instance + runtime context, not a raw data object).
 */
type StateFn<T extends BaseEntity> = (model: T, ctx: FactoryContext) => void;

/**
 * Process-wide counter for stubbed primary keys (Lucid's stub id). Every
 * `makeStubbed*` build with no explicit PK gets the next value, so stubbed
 * instances have stable, distinct, DB-free identifiers — enough to look
 * persisted and to key relations/serialization without a round trip.
 */
let stubIdCounter = 0;

/**
 * Global stub-id generator override (Adonis Lucid `Factory.stubId`). Set via
 * {@link Factory.stubId} when models use non-integer primary keys (uuid, etc.);
 * receives the running counter and the instance, returns the id to assign.
 */
let globalStubId: ((counter: number, model: BaseEntity) => unknown) | undefined;

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

/**
 * A `merge` callback (Adonis Lucid) — receives the built model INSTANCE, the
 * resolved attributes, and the runtime context, e.g.
 * `.merge((user, attributes) => { user.merge(attributes) })`.
 */
type MergeFn<T extends BaseEntity> = (
	model: T,
	attributes: Record<string, unknown>,
	ctx: FactoryContext,
) => void;

export interface FactoryBuilder<T extends BaseEntity> {
	/**
	 * Override specific fields for the next call (reset after consumption). Pass an
	 * object to shallow-merge, or a CALLBACK to mutate the resolved attributes
	 * imperatively (Adonis Lucid `merge`) — the callback runs after the object
	 * merges, receiving the attributes and the runtime context.
	 */
	merge(
		overrides: Partial<Record<string, unknown>> | MergeFn<T>,
	): FactoryBuilder<T>;

	/**
	 * Like {@link merge} but deep — nested plain objects are merged key by key
	 * instead of replaced wholesale (Lucid `mergeRecursive`). Arrays and
	 * non-plain values still replace. Reset after consumption like `merge`.
	 */
	mergeRecursive(
		overrides: Partial<Record<string, unknown>>,
	): FactoryBuilder<T>;

	/**
	 * Declare a named variation. The callback receives the built model INSTANCE
	 * and the runtime context (Adonis Lucid `state`), e.g.
	 * `.state('admin', (user) => { user.role = 'admin' })`.
	 */
	state(name: string, fn: StateFn<T>): FactoryBuilder<T>;

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
	 * The callback receives the related factory to customize it — `merge`/`apply`,
	 * `.pivotAttributes()` for m2m pivot columns, and its own nested `.with()`
	 * (arbitrarily deep, Adonis Lucid nested factories). `.with()` runs on
	 * persistence only — `make`/`makeStubbed` ignore it.
	 */
	with(
		name: string,
		count?: number,
		callback?: (factory: FactoryBuilder<BaseEntity>) => void,
	): FactoryBuilder<T>;

	/**
	 * Register a callback that runs on the built entity INSTANCE (Adonis Lucid
	 * `.tap`). Receives the model, the runtime {@link FactoryContext}, and this
	 * factory builder. Multiple taps run in order, on every instance-producing
	 * path (`make`/`makeMany`/`create`/`createMany`/`makeStubbed*`); reset after
	 * consumption.
	 */
	tap(
		fn: (model: T, ctx: FactoryContext, builder: FactoryBuilder<T>) => void,
	): FactoryBuilder<T>;

	/**
	 * Replace the default `new Model()` instantiation (Adonis Lucid `.newUp`).
	 * The callback receives the resolved attributes and the runtime context and
	 * returns the model instance to use for every subsequent build.
	 */
	newUp(
		fn: (attributes: Record<string, unknown>, ctx: FactoryContext) => T,
	): FactoryBuilder<T>;

	/**
	 * Set pivot columns for the NEXT many-to-many `.with()` link (Adonis Lucid
	 * `.pivotAttributes`). Called on the RELATED factory inside a `.with()`
	 * callback; the values are written on the pivot row alongside the link. A
	 * single object applies to every linked row; pass an ARRAY for different
	 * values per row (its length should match the related-row count).
	 */
	pivotAttributes(
		attrs: Record<string, unknown> | Array<Record<string, unknown>>,
	): FactoryBuilder<T>;

	/**
	 * Bind the connection/transaction for subsequent `create`/`createMany` via an
	 * options object (Adonis Lucid `.query({ client }) / .query({ connection })`).
	 * Sugar over {@link client} / {@link connection}; `client` wins over `connection`.
	 */
	query(options: {
		client?: DatabaseConnection;
		connection?: string;
	}): FactoryBuilder<T>;

	/**
	 * Register a lifecycle hook that runs BEFORE the given event (Adonis Lucid
	 * factory `before`). `create` fires before the INSERT; `makeStubbed` fires
	 * before the stub is finalised (so it can assign the primary key). The
	 * callback receives the factory and the model instance. Persistent (declared
	 * once, applies to every build).
	 */
	before(
		event: "create" | "makeStubbed",
		callback: (
			factory: FactoryBuilder<T>,
			model: T,
			ctx: FactoryContext,
		) => void,
	): FactoryBuilder<T>;

	/**
	 * Register a lifecycle hook that runs AFTER the given event (Adonis Lucid
	 * factory `after`). `create` fires after the INSERT; `makeStubbed` after the
	 * stub is built. `make` fires after an un-persisted `make`/`makeMany` instance
	 * is built and tapped (Adonis Lucid `after('make')`). Persistent.
	 */
	after(
		event: "make" | "create" | "makeStubbed",
		callback: (
			factory: FactoryBuilder<T>,
			model: T,
			ctx: FactoryContext,
		) => void,
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

	/**
	 * Override the global stub-id generator (Adonis Lucid `Factory.stubId`). Use
	 * when your models have non-integer primary keys — the callback receives the
	 * running counter and the instance and returns the id to assign. Pass
	 * `null`/nothing to restore the default incrementing integer.
	 */
	stubId(generator?: ((counter: number, model: BaseEntity) => unknown) | null) {
		globalStubId = generator ?? undefined;
	},
};

export function factory<T extends BaseEntity>(
	entityClass: EntityConstructor<T>,
	defaults: DefaultsFn,
): FactoryBuilder<T> {
	// Persistent state — lives across calls.
	const states = new Map<string, StateFn<T>>();
	// Transient state — resets after every `make`/`create`.
	let pendingOverrides: Partial<Record<string, unknown>> = {};
	// `merge(callback)` mutators, applied on the built INSTANCE (with the resolved
	// attributes) after instantiation, in call order.
	let pendingMergeFns: MergeFn<T>[] = [];
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
	// Adonis Lucid passes the model, the runtime context, and the factory builder.
	let pendingTap: Array<
		(model: T, ctx: FactoryContext, builder: FactoryBuilder<T>) => void
	> = [];
	// Transient pivot columns for the NEXT m2m `.with()` link (Adonis Lucid
	// `.pivotAttributes()`), read by the parent factory's applyRelations. A single
	// object applies to every linked row; an array sets per-row values.
	let pendingPivot:
		| Record<string, unknown>
		| Array<Record<string, unknown>>
		| undefined;
	// Persistent custom instantiation (Adonis Lucid `.newUp`) — replaces
	// `new Model() + setProp` when set.
	let customNewUp:
		| ((attributes: Record<string, unknown>, ctx: FactoryContext) => T)
		| undefined;
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
	type Hook = (
		factory: FactoryBuilder<T>,
		model: T,
		ctx: FactoryContext,
	) => void;
	const beforeHooks: Record<"create" | "makeStubbed", Hook[]> = {
		create: [],
		makeStubbed: [],
	};
	// `make` has no `before` counterpart in Lucid (nothing to gate before an
	// un-persisted build), so it lives on the after-side only.
	const afterHooks: Record<"make" | "create" | "makeStubbed", Hook[]> = {
		make: [],
		create: [],
		makeStubbed: [],
	};

	// Primary-key property, resolved once, for stub-id assignment. Same fallback
	// as BaseRepository so a model without an explicit `@PrimaryKey` uses `id`.
	const primaryKey = getPrimaryKey(entityClass) ?? "id";

	/** Hydrate a fresh (not-yet-persisted) instance — or defer to `.newUp` when set. */
	const newInstance = (
		data: Record<string, unknown>,
		ctx: FactoryContext,
	): T => {
		if (customNewUp) return customNewUp(data, ctx);
		const entity = new entityClass();
		for (const [key, value] of Object.entries(data)) {
			entity.setProp(key, value);
		}
		return entity;
	};

	/** Run every tap with the Lucid `(model, ctx, builder)` signature. */
	const runTaps = (
		taps: Array<
			(model: T, ctx: FactoryContext, builder: FactoryBuilder<T>) => void
		>,
		entity: T,
		ctx: FactoryContext,
	): void => {
		for (const tap of taps) tap(entity, ctx, builder);
	};

	/** Build a stubbed instance: hydrate, run taps, give it a stub id if none was
	 * supplied, then mark persisted so it mirrors a fetched row. Taps run before
	 * `markAsPersisted` so tapped fields land in the clean snapshot. */
	/** Build the runtime context for a build (Adonis Lucid factory `ctx`). */
	const makeCtx = (isStubbed: boolean): FactoryContext => ({
		faker,
		isStubbed,
		$trx: boundClient,
	});

	const stub = (
		data: Record<string, unknown>,
		taps: Array<
			(model: T, ctx: FactoryContext, builder: FactoryBuilder<T>) => void
		>,
		mergeFns: MergeFn<T>[],
		stateNames: string[],
		ctx: FactoryContext,
	): T => {
		const entity = newInstance(data, ctx);
		applyMergeFns(entity, data, mergeFns, ctx);
		applyStates(entity, stateNames, ctx);
		runTaps(taps, entity, ctx);
		// before('makeStubbed') runs BEFORE the stub id, so a hook can set the PK.
		for (const hook of beforeHooks.makeStubbed) hook(builder, entity, ctx);
		// Assign a stub id only when neither the data NOR a before hook set the PK.
		// A global `Factory.stubId` override generates it (uuid, etc.) when set.
		if (entity[primaryKey] === undefined) {
			const next = ++stubIdCounter;
			entity.setProp(
				primaryKey,
				globalStubId ? globalStubId(next, entity) : next,
			);
		}
		entity.markAsPersisted();
		for (const hook of afterHooks.makeStubbed) hook(builder, entity, ctx);
		return entity;
	};

	const buildData = (ctx: FactoryContext): Record<string, unknown> => {
		let data: Record<string, unknown> = {
			...defaults(ctx),
			...pendingOverrides,
		};
		// Deep overrides layer on top of the shallow one so nested defaults survive.
		if (Object.keys(pendingRecursive).length > 0) {
			data = deepMerge(data, pendingRecursive);
		}
		return data;
	};

	/**
	 * Run `merge(callback)` mutators on the built INSTANCE (Adonis Lucid passes the
	 * model, the resolved attributes, and the context). Runs before states/taps.
	 */
	const applyMergeFns = (
		entity: T,
		attributes: Record<string, unknown>,
		mergeFns: MergeFn<T>[],
		ctx: FactoryContext,
	): void => {
		for (const fn of mergeFns) fn(entity, attributes, ctx);
	};

	/**
	 * Run the named states on the built INSTANCE (Adonis Lucid). Separated from
	 * {@link buildData} because states mutate the model, not the raw attributes.
	 */
	const applyStates = (
		entity: T,
		stateNames: string[],
		ctx: FactoryContext,
	): void => {
		for (const name of stateNames) {
			const fn = states.get(name);
			if (!fn) {
				throw new Error(
					`Factory state '${name}' is not defined on ${entityClass.name}Factory`,
				);
			}
			fn(entity, ctx);
		}
	};

	/** Build one raw row and clear transient state — the persistence paths
	 * (`create`/`createMany`) need the plain object, not an instance. */
	const consumeData = (ctx: FactoryContext): Record<string, unknown> => {
		const data = buildData(ctx);
		resetPending();
		return data;
	};

	/** Build `count` raw rows (distinct faker/Date values per row), then reset. */
	const consumeDataMany = (
		count: number,
		ctx: FactoryContext,
	): Record<string, unknown>[] => {
		const rows: Record<string, unknown>[] = [];
		for (let i = 0; i < count; i++) rows.push(buildData(ctx));
		resetPending();
		return rows;
	};

	const resetPending = (): void => {
		pendingOverrides = {};
		pendingMergeFns = [];
		pendingRecursive = {};
		pendingStates = [];
		pendingTap = [];
		pendingPivot = undefined;
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
		taps: Array<
			(model: T, ctx: FactoryContext, builder: FactoryBuilder<T>) => void
		>,
		mergeFns: MergeFn<T>[],
		stateNames: string[],
		ctx: FactoryContext,
	): Promise<T> => {
		if (
			taps.length === 0 &&
			mergeFns.length === 0 &&
			stateNames.length === 0 &&
			beforeHooks.create.length === 0 &&
			afterHooks.create.length === 0 &&
			!customNewUp
		) {
			return repo.create(data);
		}
		const entity = newInstance(data, ctx);
		applyMergeFns(entity, data, mergeFns, ctx);
		applyStates(entity, stateNames, ctx);
		runTaps(taps, entity, ctx);
		for (const hook of beforeHooks.create) hook(builder, entity, ctx);
		await repo.save(entity);
		for (const hook of afterHooks.create) hook(builder, entity, ctx);
		return entity;
	};

	/**
	 * Persist a parent and its queued `.with()` relations. With relations, the
	 * whole graph runs in a managed transaction (Adonis Lucid: if a related write
	 * fails the parent insert rolls back too); the parent is created through a
	 * `useTransaction`-bound repo so its `related()` proxies also use the trx.
	 * With no relations there's nothing to make atomic — a single insert already
	 * is — so it stays on the plain connection.
	 */
	const persistWithRelations = async (
		conn: DatabaseConnection,
		withReqs: WithRequest[],
		build: (repo: BaseRepository<T>, ctx: FactoryContext) => Promise<T>,
	): Promise<T> => {
		if (withReqs.length === 0) {
			return build(new BaseRepository(entityClass, conn), makeCtx(false));
		}
		return transaction(conn, async (trx) => {
			const repo = new BaseRepository(entityClass, conn).useTransaction(trx);
			const entity = await build(repo, { faker, isStubbed: false, $trx: trx });
			await applyRelations(entity, withReqs, trx);
			return entity;
		});
	};

	/**
	 * Persist the queued `.with()` relations for one just-created parent, routing
	 * through the parent's relation proxy so the FK / pivot is wired by the
	 * already-tested relation-write code (not re-derived here).
	 */
	const applyRelations = async (
		parent: BaseEntity,
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
			const childInternals = factoryInternals.get(childFactory);

			const proxy = parent.related(req.name);
			if (proxy.type === "belongsTo") {
				// FK lives on the parent: create() runs the owner's OWN with-graph,
				// then associate re-saves the parent with the FK set.
				const owner = await childFactory.create(db);
				await proxy.associate(owner);
				continue;
			}

			// The proxy-persisted branches bypass the child factory's create() path,
			// so its queued nested `.with()` must be captured before make() clears it,
			// then recursed onto each persisted child (Adonis Lucid nested factories:
			// `post.with('comments', 5)` inside the callback).
			const nestedWith = childInternals?.consumeWith() ?? [];
			// Pivot columns from a `.pivotAttributes()` in the callback — captured
			// now, before make() clears the child's transient state.
			const nestedPivot = childInternals?.consumePivot();
			const recurse = async (children: BaseEntity[]): Promise<void> => {
				if (nestedWith.length === 0 || !childInternals) return;
				for (const child of children) {
					await childInternals.applyRelations(child, nestedWith, db);
				}
			};

			if (proxy.type === "hasMany") {
				await recurse(await proxy.createMany(childFactory.makeMany(req.count)));
			} else if (proxy.type === "hasOne") {
				await recurse([await proxy.create(childFactory.make())]);
			} else if (proxy.type === "manyToMany") {
				// The m2m proxy's create() inserts the related row AND the pivot link,
				// with the `.pivotAttributes()` columns on the pivot row. An array of
				// pivot attrs applies per-row (`pivot[i]`); a single object, to all.
				const rows = childFactory.makeMany(req.count);
				// Adonis Lucid: an array's length must match the related-row count —
				// reject a mismatch rather than silently leave rows without pivot data.
				if (Array.isArray(nestedPivot) && nestedPivot.length !== rows.length) {
					throw new Error(
						`Factory .with('${req.name}'): pivotAttributes array length (${nestedPivot.length}) must match the related-row count (${rows.length}).`,
					);
				}
				for (let i = 0; i < rows.length; i++) {
					const pivot = Array.isArray(nestedPivot)
						? nestedPivot[i]
						: nestedPivot;
					await recurse([await proxy.create(rows[i], pivot)]);
				}
			} else {
				throw new Error(
					`Factory .with('${req.name}'): '${relMeta.type}' relations are not supported`,
				);
			}
		}
	};

	const builder: FactoryBuilder<T> = {
		merge(overrides) {
			if (typeof overrides === "function") {
				pendingMergeFns.push(overrides);
			} else {
				pendingOverrides = { ...pendingOverrides, ...overrides };
			}
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

		newUp(fn) {
			customNewUp = fn;
			return builder;
		},

		pivotAttributes(attrs) {
			// An array (per-row values) replaces wholesale; objects merge (repeated
			// `.pivotAttributes({...})` accumulate, matching the single-object case).
			pendingPivot =
				Array.isArray(attrs) || Array.isArray(pendingPivot)
					? attrs
					: { ...pendingPivot, ...attrs };
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

		query(options) {
			if (options.client) {
				boundClient = options.client;
			} else if (options.connection) {
				const conn = getConnection(options.connection);
				if (!conn) {
					throw new Error(
						`Factory ${entityClass.name}: no connection registered under '${options.connection}'.`,
					);
				}
				boundClient = conn;
			}
			return builder;
		},

		make() {
			// Lucid `make`: an UN-persisted instance (no PK, `$isPersisted` false).
			// Taps run on it, like the other instance-producing paths.
			const taps = pendingTap;
			const mergeFns = pendingMergeFns;
			const stateNames = pendingStates;
			const ctx = makeCtx(false);
			const data = buildData(ctx);
			const entity = newInstance(data, ctx);
			applyMergeFns(entity, data, mergeFns, ctx);
			applyStates(entity, stateNames, ctx);
			runTaps(taps, entity, ctx);
			for (const hook of afterHooks.make) hook(builder, entity, ctx);
			resetPending();
			return entity;
		},

		makeMany(count) {
			// Re-evaluate defaults for each row so `Date.now()` / faker generate
			// distinct values, each hydrated into its own un-persisted instance.
			const taps = pendingTap;
			const mergeFns = pendingMergeFns;
			const stateNames = pendingStates;
			const ctx = makeCtx(false);
			const rows: T[] = [];
			for (let i = 0; i < count; i++) {
				const data = buildData(ctx);
				const entity = newInstance(data, ctx);
				applyMergeFns(entity, data, mergeFns, ctx);
				applyStates(entity, stateNames, ctx);
				runTaps(taps, entity, ctx);
				for (const hook of afterHooks.make) hook(builder, entity, ctx);
				rows.push(entity);
			}
			resetPending();
			return rows;
		},

		makeStubbed() {
			const taps = pendingTap;
			const mergeFns = pendingMergeFns;
			const stateNames = pendingStates;
			const ctx = makeCtx(true);
			const entity = stub(buildData(ctx), taps, mergeFns, stateNames, ctx);
			resetPending();
			return entity;
		},

		makeStubbedMany(count) {
			// Re-evaluate defaults per row (distinct Date.now()/faker values), same
			// as makeMany; each row then gets its own stub id + taps via `stub`.
			const taps = pendingTap;
			const mergeFns = pendingMergeFns;
			const stateNames = pendingStates;
			const ctx = makeCtx(true);
			const rows: T[] = [];
			for (let i = 0; i < count; i++) {
				rows.push(stub(buildData(ctx), taps, mergeFns, stateNames, ctx));
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
			const mergeFns = pendingMergeFns;
			const stateNames = pendingStates;
			const ctx = makeCtx(false);
			const data = consumeData(ctx);
			const conn = resolveConnection(db);
			return persistWithRelations(conn, withReqs, (repo, txCtx) =>
				persist(repo, data, taps, mergeFns, stateNames, txCtx),
			);
		},

		async createMany(count, db) {
			const withReqs = pendingWith;
			pendingWith = [];
			const taps = pendingTap;
			const mergeFns = pendingMergeFns;
			const stateNames = pendingStates;
			const ctx = makeCtx(false);
			const rows = consumeDataMany(count, ctx);
			const conn = resolveConnection(db);
			const created: T[] = [];
			for (const data of rows) {
				// Each created parent + its relations are atomic (Lucid semantics).
				created.push(
					await persistWithRelations(conn, withReqs, (repo, txCtx) =>
						persist(repo, data, taps, mergeFns, stateNames, txCtx),
					),
				);
			}
			return created;
		},
	};

	// Register this builder's internals so a PARENT factory can drive nested
	// `.with()` on it (kept off the public type, keyed by the builder object).
	factoryInternals.set(builder, {
		consumeWith() {
			const reqs = pendingWith;
			pendingWith = [];
			return reqs;
		},
		consumePivot() {
			const pivot = pendingPivot;
			pendingPivot = undefined;
			return pivot;
		},
		applyRelations,
	});

	return builder;
}
