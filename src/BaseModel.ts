import type { AsyncDatabaseConnection } from "./adapters/NapiDbAdapter.js";
import { BaseEntity } from "./BaseEntity.js";
import { BaseRepository } from "./BaseRepository.js";
import { Entity, getEntityMetadata } from "./decorators/entity.js";
import { AtlasError } from "./errors.js";
import { getNamingStrategy } from "./naming/NamingStrategy.js";
import { getConnection, getDb } from "./services/db.js";

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

	/**
	 * Ensure `@Entity` metadata exists — infer the table name from the class name
	 * (naming strategy) or `static table` when the decorator is absent. Idempotent;
	 * runs on first repository access.
	 */
	static $boot<T extends BaseModel>(this: ModelClass<T>): void {
		if (getEntityMetadata(BaseModel) !== undefined) return;
		const table =
			BaseModel.table ?? getNamingStrategy(BaseModel).tableName(BaseModel.name);
		Entity(table)(BaseModel);
	}

	/** Resolve this model's connection (named via `static connection`, else default). */
	static $connection(): AsyncDatabaseConnection {
		const conn = BaseModel.connection
			? getConnection(BaseModel.connection)
			: getDb();
		if (conn === undefined) {
			throw new AtlasError(
				"MISSING_CONNECTION",
				BaseModel.connection
					? `No connection named '${BaseModel.connection}' is registered for model '${BaseModel.name}'.`
					: `No default database connection for model '${BaseModel.name}' — is AtlasProvider booted?`,
				{
					hint: "Boot AtlasProvider (config/database.ts) before using models, or register the named connection.",
				},
			);
		}
		return conn;
	}

	/** The {@link BaseRepository} backing this model on its resolved connection. */
	static $repo<T extends BaseModel>(this: ModelClass<T>): BaseRepository<T> {
		BaseModel.$boot();
		return new BaseRepository<T>(BaseModel, BaseModel.$connection());
	}

	// — Static finders (AdonisJS Lucid) —

	static find<T extends BaseModel>(
		this: ModelClass<T>,
		id: string | number,
	): Promise<T | null> {
		return BaseModel.$repo().find(id);
	}

	static findOrFail<T extends BaseModel>(
		this: ModelClass<T>,
		id: string | number,
	): Promise<T> {
		return BaseModel.$repo().findOrFail(id);
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
			? BaseModel.$repo().findBy(columnOrClause, value)
			: BaseModel.$repo().findBy(columnOrClause);
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
			? BaseModel.$repo().findByOrFail(columnOrClause, value)
			: BaseModel.$repo().findByOrFail(columnOrClause);
	}

	static findMany<T extends BaseModel>(
		this: ModelClass<T>,
		ids: Array<string | number>,
	): Promise<T[]> {
		return BaseModel.$repo().findMany(ids);
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
			? BaseModel.$repo().findManyBy(columnOrClause, values ?? [])
			: BaseModel.$repo().findManyBy(columnOrClause);
	}

	static all<T extends BaseModel>(this: ModelClass<T>): Promise<T[]> {
		return BaseModel.$repo().all();
	}

	static query<T extends BaseModel>(
		this: ModelClass<T>,
	): ReturnType<BaseRepository<T>["query"]> {
		return BaseModel.$repo().query();
	}

	static first<T extends BaseModel>(this: ModelClass<T>): Promise<T | null> {
		return BaseModel.$repo().query().first();
	}

	static firstOrFail<T extends BaseModel>(this: ModelClass<T>): Promise<T> {
		return BaseModel.$repo().query().firstOrFail();
	}

	// — Static creators (AdonisJS Lucid) —

	static create<T extends BaseModel>(
		this: ModelClass<T>,
		data: Partial<Record<string, unknown>>,
	): Promise<T> {
		return BaseModel.$repo().create(data);
	}

	static createMany<T extends BaseModel>(
		this: ModelClass<T>,
		rows: Array<Partial<Record<string, unknown>>>,
	): Promise<T[]> {
		return BaseModel.$repo().createMany(rows);
	}

	static firstOrCreate<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values?: Record<string, unknown>,
	): Promise<T> {
		return BaseModel.$repo().firstOrCreate(search, values);
	}

	static firstOrNew<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values?: Record<string, unknown>,
	): Promise<T> {
		return BaseModel.$repo().firstOrNew(search, values);
	}

	static updateOrCreate<T extends BaseModel>(
		this: ModelClass<T>,
		search: Record<string, unknown>,
		values: Record<string, unknown>,
	): Promise<T> {
		return BaseModel.$repo().updateOrCreate(search, values);
	}

	// — Instance persistence (AdonisJS Lucid) —

	/**
	 * INSERT this instance if new, else UPDATE its dirty columns. Returns `this`.
	 * `this.constructor` is the concrete BaseModel subclass at runtime — TS only
	 * types it as `Function`, so the single narrowing here is unavoidable (the
	 * same pattern Lucid's own BaseModel uses).
	 */
	async save(): Promise<this> {
		const model = this.constructor as ModelClass<this>;
		await model.$repo().save(this);
		return this;
	}

	/** DELETE this instance's row (soft-delete aware). Sets `$isDeleted`. */
	async delete(): Promise<void> {
		const model = this.constructor as ModelClass<this>;
		await model.$repo().delete(this);
	}
}
