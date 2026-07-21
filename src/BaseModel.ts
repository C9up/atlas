import type { AsyncDatabaseConnection } from "./adapters/NapiDbAdapter.js";
import { BaseEntity } from "./BaseEntity.js";
import { BaseRepository, type DatabaseConnection } from "./BaseRepository.js";
import { ensureEntityMetadata } from "./decorators/entity.js";
import { AtlasError } from "./errors.js";
import { getConnection, getDb } from "./services/db.js";
import { isTransactionClient } from "./utils/transactionBrand.js";

/**
 * Options accepted by the static finders/creators — Adonis Lucid's
 * `{ client: trx }`. Routes the operation through a transaction client instead
 * of the model's own connection.
 */
export interface ModelClientOptions {
	client?: DatabaseConnection;
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
	): Promise<T | null>;
	static findBy<T extends BaseModel>(
		this: ModelClass<T>,
		clause: Record<string, unknown>,
	): Promise<T | null>;
	static findBy<T extends BaseModel>(
		this: ModelClass<T>,
		columnOrClause: string | Record<string, unknown>,
		value?: unknown,
	): Promise<T | null> {
		return typeof columnOrClause === "string"
			? this.$repo().findBy(columnOrClause, value)
			: this.$repo().findBy(columnOrClause);
	}

	static findByOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		column: string,
		value: unknown,
	): Promise<T>;
	static findByOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		clause: Record<string, unknown>,
	): Promise<T>;
	static findByOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		columnOrClause: string | Record<string, unknown>,
		value?: unknown,
	): Promise<T> {
		return typeof columnOrClause === "string"
			? this.$repo().findByOrFail(columnOrClause, value)
			: this.$repo().findByOrFail(columnOrClause);
	}

	static findMany<T extends BaseModel>(
		this: ModelClass<T>,
		ids: Array<string | number>,
	): Promise<T[]> {
		return this.$repo().findMany(ids);
	}

	static findManyBy<T extends BaseModel>(
		this: ModelClass<T>,
		column: string,
		values: Array<string | number>,
	): Promise<T[]>;
	static findManyBy<T extends BaseModel>(
		this: ModelClass<T>,
		clause: Record<string, unknown>,
	): Promise<T[]>;
	static findManyBy<T extends BaseModel>(
		this: ModelClass<T>,
		columnOrClause: string | Record<string, unknown>,
		values?: Array<string | number>,
	): Promise<T[]> {
		return typeof columnOrClause === "string"
			? this.$repo().findManyBy(columnOrClause, values ?? [])
			: this.$repo().findManyBy(columnOrClause);
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
	): Promise<T> {
		return this.$repo().createQuietly(data);
	}

	/** {@link createMany} without firing lifecycle hooks (AdonisJS Lucid `createManyQuietly`). */
	static createManyQuietly<T extends BaseModel>(
		this: ModelClass<T>,
		rows: Array<Partial<Record<string, unknown>>>,
	): Promise<T[]> {
		return this.$repo().createManyQuietly(rows);
	}

	static firstOrCreate<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values?: Record<string, unknown>,
	): Promise<T> {
		return this.$repo().firstOrCreate(search, values);
	}

	static firstOrNew<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values?: Record<string, unknown>,
	): Promise<T> {
		return this.$repo().firstOrNew(search, values);
	}

	static updateOrCreate<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values: Record<string, unknown>,
	): Promise<T> {
		return this.$repo().updateOrCreate(search, values);
	}

	static updateOrCreateMany<T extends BaseModel>(
		this: ModelClass<T>,
		key: string | string[],
		rows: Array<Record<string, unknown>>,
	): Promise<T[]> {
		return this.$repo().updateOrCreateMany(key, rows);
	}

	static fetchOrCreateMany<T extends BaseModel>(
		this: ModelClass<T>,
		key: string | string[],
		rows: Array<Record<string, unknown>>,
	): Promise<T[]> {
		return this.$repo().fetchOrCreateMany(key, rows);
	}

	static fetchOrNewUpMany<T extends BaseModel>(
		this: ModelClass<T>,
		key: string | string[],
		rows: Array<Record<string, unknown>>,
	): Promise<T[]> {
		return this.$repo().fetchOrNewUpMany(key, rows);
	}

	/** Empty this model's table (AdonisJS `Model.truncate`). `cascade` is Postgres-only. */
	static truncate<T extends BaseModel>(
		this: ModelClass<T>,
		cascade = false,
	): Promise<void> {
		return this.$repo().truncate(cascade);
	}

	// — Instance persistence (AdonisJS Lucid) —

	/** Transaction bound to this instance via {@link useTransaction}, if any. */
	#trx?: DatabaseConnection;

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
	#repo(): BaseRepository<this> {
		const model = this.constructor as ModelClass<this>;
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
