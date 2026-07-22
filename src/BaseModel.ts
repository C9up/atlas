import type {
	AsyncDatabaseConnection,
	TransactionOptions,
} from "./adapters/NapiDbAdapter.js";
import { BaseEntity } from "./BaseEntity.js";
import { BaseRepository, type DatabaseConnection } from "./BaseRepository.js";
import { ensureEntityMetadata } from "./decorators/entity.js";
import { AtlasError } from "./errors.js";
import { getConnection, getDb } from "./services/db.js";
import type { TransactionClient } from "./Transaction.js";
import { isTransactionClient } from "./utils/transactionBrand.js";

/**
 * Options accepted by the static finders/creators — Adonis Lucid's
 * `{ client: trx }`. Routes the operation through a transaction client instead
 * of the model's own connection.
 */
export interface ModelClientOptions {
	client?: DatabaseConnection;
}

/**
 * Narrow the optional trailing argument of an overloaded finder (which is a
 * value in the `(column, value)` form, or the options in the `(clause)` form) to
 * client options — without a cast. An options object without a `client` reads as
 * "no options", which is the same as passing none.
 */
function isClientOptions(x: unknown): x is ModelClientOptions {
	return typeof x === "object" && x !== null && "client" in x;
}

/** A concrete BaseModel subclass: a `new()` constructor plus the static façade. */
type ModelClass<T extends BaseModel> = (new () => T) & typeof BaseModel;

/**
 * AdonisJS Lucid–style Active Record façade. Subclass this (instead of
 * `BaseEntity`) to get static finders/creators + `instance.save()/delete()`
 * that delegate to a per-connection {@link BaseRepository}, while still
 * inheriting `fill`/`merge`/`refresh`, dirty-tracking and the model-state flags
 * (`$isPersisted`, `$isNew`, …) from BaseEntity.
 *
 *     class User extends BaseModel {
 *       @PrimaryKey() declare id: number
 *       @Column() declare email: string
 *     }
 *     const u = await User.find(1)
 *     if (u) { u.email = "x@y"; await u.save() }
 *
 * `@Entity('table')` is OPTIONAL on a BaseModel — the table name is inferred
 * from the class name via the naming strategy (or `static table`). The
 * Data-Mapper `BaseRepository` remains available for those who prefer it.
 */
export abstract class BaseModel extends BaseEntity {
	/** Override the inferred table name (AdonisJS `static table`). */
	static table?: string;
	/** Bind to a named connection (AdonisJS `static connection`); default otherwise. */
	static connection?: string;
	/** Override the primary-key column name (AdonisJS `static primaryKey`); `@PrimaryKey()` otherwise. */
	static primaryKey?: string;

	/**
	 * Ensure `@Entity` metadata exists — infer the table name from the class name
	 * (naming strategy) or `static table` when the decorator is absent. Idempotent;
	 * runs on first repository access.
	 */
	static $boot<T extends BaseModel>(this: ModelClass<T>): void {
		ensureEntityMetadata(this);
	}

	/** Resolve this model's connection (named via `static connection`, else default). */
	static $connection(): AsyncDatabaseConnection {
		const conn = this.connection ? getConnection(this.connection) : getDb();
		if (conn === undefined) {
			throw new AtlasError(
				"MISSING_CONNECTION",
				this.connection
					? `No connection named '${this.connection}' is registered for model '${this.name}'.`
					: `No default database connection for model '${this.name}' — is AtlasProvider booted?`,
				{
					hint: "Boot AtlasProvider (config/database.ts) before using models, or register the named connection.",
				},
			);
		}
		return conn;
	}

	/**
	 * The {@link BaseRepository} backing this model on its resolved connection.
	 * Pass `{ client: trx }` (Adonis Lucid) to bind the repository to a
	 * transaction, so every finder/creator routed through it runs on that trx.
	 */
	static $repo<T extends BaseModel>(
		this: ModelClass<T>,
		options?: ModelClientOptions,
	): BaseRepository<T> {
		this.$boot();
		const repo = new BaseRepository<T>(this, this.$connection());
		return options?.client ? repo.useTransaction(options.client) : repo;
	}

	/**
	 * Start a transaction on THIS model's connection (Adonis Lucid
	 * `Model.transaction`) — handy when the model overrides `static connection`.
	 * Managed when given a callback (auto commit on success, rollback on throw),
	 * manual otherwise. On the default connection it is equivalent to
	 * `db.transaction(...)`. Accepts an `isolationLevel` via the options.
	 */
	static transaction<T>(
		this: ModelClass<BaseModel>,
		callback: (trx: TransactionClient) => Promise<T> | T,
		options?: TransactionOptions,
	): Promise<T>;
	static transaction(
		this: ModelClass<BaseModel>,
		options?: TransactionOptions,
	): Promise<TransactionClient>;
	static transaction(
		this: ModelClass<BaseModel>,
		callbackOrOptions?:
			| ((trx: TransactionClient) => unknown)
			| TransactionOptions,
		options?: TransactionOptions,
	): Promise<unknown> {
		const conn = this.$connection();
		if (typeof conn.transaction !== "function") {
			throw new AtlasError(
				"E_NO_INTERACTIVE_TRANSACTION",
				`Model '${this.name}' connection has no interactive transaction().`,
			);
		}
		if (typeof callbackOrOptions === "function") {
			return conn.transaction(callbackOrOptions, options);
		}
		// Manual mode: forward the isolation options if any (the overload rejects an
		// explicit `undefined`, so call with no args when none were passed).
		return callbackOrOptions === undefined
			? conn.transaction()
			: conn.transaction(callbackOrOptions);
	}

	// — Static finders (AdonisJS Lucid) —

	static find<T extends BaseModel>(
		this: ModelClass<T>,
		id: string | number,
		options?: ModelClientOptions,
	): Promise<T | null> {
		return this.$repo(options).find(id);
	}

	static findOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		id: string | number,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).findOrFail(id);
	}

	static findBy<T extends BaseModel>(
		this: ModelClass<T>,
		column: string,
		value: unknown,
		options?: ModelClientOptions,
	): Promise<T | null>;
	static findBy<T extends BaseModel>(
		this: ModelClass<T>,
		clause: Record<string, unknown>,
		options?: ModelClientOptions,
	): Promise<T | null>;
	static findBy<T extends BaseModel>(
		this: ModelClass<T>,
		columnOrClause: string | Record<string, unknown>,
		valueOrOptions?: unknown,
		options?: ModelClientOptions,
	): Promise<T | null> {
		if (typeof columnOrClause === "string") {
			return this.$repo(options).findBy(columnOrClause, valueOrOptions);
		}
		const opts = isClientOptions(valueOrOptions) ? valueOrOptions : undefined;
		return this.$repo(opts).findBy(columnOrClause);
	}

	static findByOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		column: string,
		value: unknown,
		options?: ModelClientOptions,
	): Promise<T>;
	static findByOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		clause: Record<string, unknown>,
		options?: ModelClientOptions,
	): Promise<T>;
	static findByOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		columnOrClause: string | Record<string, unknown>,
		valueOrOptions?: unknown,
		options?: ModelClientOptions,
	): Promise<T> {
		if (typeof columnOrClause === "string") {
			return this.$repo(options).findByOrFail(columnOrClause, valueOrOptions);
		}
		const opts = isClientOptions(valueOrOptions) ? valueOrOptions : undefined;
		return this.$repo(opts).findByOrFail(columnOrClause);
	}

	static findMany<T extends BaseModel>(
		this: ModelClass<T>,
		ids: Array<string | number>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).findMany(ids);
	}

	static findManyBy<T extends BaseModel>(
		this: ModelClass<T>,
		column: string,
		values: Array<string | number>,
		options?: ModelClientOptions,
	): Promise<T[]>;
	static findManyBy<T extends BaseModel>(
		this: ModelClass<T>,
		clause: Record<string, unknown>,
		options?: ModelClientOptions,
	): Promise<T[]>;
	static findManyBy<T extends BaseModel>(
		this: ModelClass<T>,
		columnOrClause: string | Record<string, unknown>,
		valuesOrOptions?: Array<string | number> | ModelClientOptions,
		options?: ModelClientOptions,
	): Promise<T[]> {
		if (typeof columnOrClause === "string") {
			const values = Array.isArray(valuesOrOptions) ? valuesOrOptions : [];
			return this.$repo(options).findManyBy(columnOrClause, values);
		}
		const opts = isClientOptions(valuesOrOptions) ? valuesOrOptions : undefined;
		return this.$repo(opts).findManyBy(columnOrClause);
	}

	static all<T extends BaseModel>(
		this: ModelClass<T>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).all();
	}

	static query<T extends BaseModel>(
		this: ModelClass<T>,
		options?: ModelClientOptions,
	): ReturnType<BaseRepository<T>["query"]> {
		return this.$repo(options).query();
	}

	static first<T extends BaseModel>(
		this: ModelClass<T>,
		options?: ModelClientOptions,
	): Promise<T | null> {
		return this.$repo(options).query().first();
	}

	static firstOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).query().firstOrFail();
	}

	// — Static creators (AdonisJS Lucid) —

	static create<T extends BaseModel>(
		this: ModelClass<T>,
		data: Partial<Record<string, unknown>>,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).create(data);
	}

	static createMany<T extends BaseModel>(
		this: ModelClass<T>,
		rows: Array<Partial<Record<string, unknown>>>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).createMany(rows);
	}

	/** {@link create} without firing lifecycle hooks (AdonisJS Lucid `createQuietly`). */
	static createQuietly<T extends BaseModel>(
		this: ModelClass<T>,
		data: Partial<Record<string, unknown>>,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).createQuietly(data);
	}

	/** {@link createMany} without firing lifecycle hooks (AdonisJS Lucid `createManyQuietly`). */
	static createManyQuietly<T extends BaseModel>(
		this: ModelClass<T>,
		rows: Array<Partial<Record<string, unknown>>>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).createManyQuietly(rows);
	}

	static firstOrCreate<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values?: Record<string, unknown>,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).firstOrCreate(search, values);
	}

	static firstOrNew<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values?: Record<string, unknown>,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).firstOrNew(search, values);
	}

	static updateOrCreate<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values: Record<string, unknown>,
		options?: ModelClientOptions,
	): Promise<T> {
		return this.$repo(options).updateOrCreate(search, values);
	}

	static updateOrCreateMany<T extends BaseModel>(
		this: ModelClass<T>,
		key: string | string[],
		rows: Array<Record<string, unknown>>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).updateOrCreateMany(key, rows);
	}

	static fetchOrCreateMany<T extends BaseModel>(
		this: ModelClass<T>,
		key: string | string[],
		rows: Array<Record<string, unknown>>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).fetchOrCreateMany(key, rows);
	}

	static fetchOrNewUpMany<T extends BaseModel>(
		this: ModelClass<T>,
		key: string | string[],
		rows: Array<Record<string, unknown>>,
		options?: ModelClientOptions,
	): Promise<T[]> {
		return this.$repo(options).fetchOrNewUpMany(key, rows);
	}

	/** Empty this model's table (AdonisJS `Model.truncate`). `cascade` is Postgres-only. */
	static truncate<T extends BaseModel>(
		this: ModelClass<T>,
		cascade = false,
		options?: ModelClientOptions,
	): Promise<void> {
		return this.$repo(options).truncate(cascade);
	}

	// — Instance persistence (AdonisJS Lucid) —

	/** Transaction bound to this instance via {@link useTransaction}, if any. */
	#trx?: DatabaseConnection;
	/** Named connection bound at runtime via {@link useConnection}, if any. */
	#connectionOverride?: string;

	/** The transaction bound to this instance, if any (AdonisJS Lucid `$trx`). */
	get $trx(): DatabaseConnection | undefined {
		return this.#trx;
	}

	/**
	 * Bind this instance to a transaction so subsequent `save()` / `delete()` run
	 * inside it (AdonisJS Lucid `model.useTransaction`). Chainable.
	 *
	 * The binding is released when the transaction settles: Lucid clears `$trx`
	 * on commit/rollback so a reused instance falls back to the connection pool
	 * instead of a finished client. We mirror that by unbinding iff we are still
	 * bound to the same trx (a later `useTransaction` to another trx wins).
	 */
	useTransaction(trx: DatabaseConnection): this {
		this.#trx = trx;
		// `.after` lives on the transaction client (a plain pool connection has no
		// settle event). In practice useTransaction always receives a trx; guard so
		// the type narrows and a non-trx connection is simply left bound (Lucid too
		// only resets $trx for real transactions).
		if (isTransactionClient(trx)) {
			const release = () => {
				if (this.#trx === trx) this.#trx = undefined;
			};
			trx.after("commit", release);
			trx.after("rollback", release);
		}
		return this;
	}

	/**
	 * The repository backing this instance — transaction-bound when
	 * {@link useTransaction} was called. `this.constructor` is the concrete
	 * BaseModel subclass at runtime; TS types it only as `Function`, hence the
	 * single unavoidable narrowing (the pattern Lucid's own BaseModel uses).
	 */
	/**
	 * Bind this instance to a named connection at runtime (AdonisJS Lucid
	 * `model.useConnection`) — subsequent `save()`/`delete()`/relations run on it.
	 * A per-instance override of the class's `static connection`. Chainable.
	 */
	useConnection(name: string): this {
		this.#connectionOverride = name;
		return this;
	}

	#repo(): BaseRepository<this> {
		const model = this.constructor as ModelClass<this>;
		if (this.#connectionOverride !== undefined) {
			const conn = getConnection(this.#connectionOverride);
			if (!conn) {
				throw new AtlasError(
					"MISSING_CONNECTION",
					`Model '${model.name}': no connection named '${this.#connectionOverride}' is registered.`,
				);
			}
			model.$boot();
			const repo = new BaseRepository<this>(model, conn);
			return this.#trx ? repo.useTransaction(this.#trx) : repo;
		}
		const repo = model.$repo();
		return this.#trx ? repo.useTransaction(this.#trx) : repo;
	}

	/** INSERT this instance if new, else UPDATE its dirty columns. Returns `this`. */
	async save(): Promise<this> {
		await this.#repo().save(this);
		return this;
	}

	/** {@link save} without firing lifecycle hooks (AdonisJS Lucid `saveQuietly`). Returns `this`. */
	async saveQuietly(): Promise<this> {
		await this.#repo().saveQuietly(this);
		return this;
	}

	/** DELETE this instance's row (soft-delete aware). Sets `$isDeleted`. */
	async delete(): Promise<void> {
		await this.#repo().delete(this);
	}

	/** {@link delete} without firing lifecycle hooks (AdonisJS Lucid `deleteQuietly`). */
	async deleteQuietly(): Promise<void> {
		await this.#repo().deleteQuietly(this);
	}
}
