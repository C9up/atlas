/**
 * The errors atlas raises, one export per code, named exactly like the code.
 *
 * An application tests `error.code`, or `error instanceof errors.E_…` with the
 * `errors` namespace the package entry exports. Each is a flare `Exception`:
 * `status` is what an HTTP exception handler answers with (500 unless stated),
 * and `help`, when present, says how to fix the cause.
 *
 * The Rust query compiler and the Rust driver can only hand a string across
 * the NAPI boundary; their refusals are rethrown here as the class their
 * message names (see `src/utils/nativeErrors.ts`).
 */

import { createError, Exception, format } from "@c9up/flare";

/** A model class, as `E_ROW_NOT_FOUND.model` carries it. */
type ModelClass = abstract new (...args: never[]) => unknown;

/**
 * A query that must return a row found none: the repository's `findOrFail`,
 * `findByOrFail`, `refresh` and `fresh`, and `firstOrFail()` / `sole()` on
 * the model query and on the `db.from()` builder. One code for all of them,
 * with HTTP status 404, so an exception handler answers a missing row the same
 * way whichever path looked for it.
 */
export class E_ROW_NOT_FOUND extends Exception {
	static override status = 404;
	static override code = "E_ROW_NOT_FOUND";
	static override message = "Row not found";

	/** The model whose query found no row; undefined for a `db.from()` query. */
	readonly model: ModelClass | undefined;

	/**
	 * The lookup criteria, when the finder had any. Kept off the message on
	 * purpose: a 404's message reaches the client, and the criteria are what
	 * the client searched for (an id, an email, a token).
	 */
	readonly criteria: unknown;

	constructor(model?: ModelClass, criteria?: unknown, options?: ErrorOptions) {
		super(undefined, options);
		this.model = model;
		this.criteria = criteria;
	}
}

/**
 * Mass assignment (`fill` / `merge` / `create`) tried to set a field that is
 * not in the model's `fillable` list, or is in its `guarded` list.
 */
export class E_MASS_ASSIGNMENT extends Exception {
	static override code = "E_MASS_ASSIGNMENT";

	constructor(
		public readonly entityClass: string,
		public readonly attribute: string,
		options?: ErrorOptions,
	) {
		super(
			`Attribute '${attribute}' on ${entityClass} is not mass-assignable.`,
			options,
		);
		this.help = `Add '${attribute}' to the static 'fillable' array or remove it from 'guarded'.`;
	}
}

/** A fill named a property that is not a declared column. */
export class E_EXTRA_PROPERTIES extends Exception {
	static override code = "E_EXTRA_PROPERTIES";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/**
 * A value was assigned to a key that is not a column. Pass `{
 * allowExtraProperties: true }` to keep unknown keys in `$extras`.
 */
export const E_UNKNOWN_COLUMN = createError<[unknown, unknown]>(
	"Cannot assign '%s' — it is not a column on %s. Pass { allowExtraProperties: true } to keep unknown keys in $extras.",
	"E_UNKNOWN_COLUMN",
);

/** A query names a column the entity does not declare. */
export class E_INVALID_COLUMN extends Exception {
	static override code = "E_INVALID_COLUMN";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/** `find()` was called without a usable primary key value. */
export const E_INVALID_FIND_VALUE = createError<[unknown, unknown]>(
	"%s.find() expects a value, received %s.",
	"E_INVALID_FIND_VALUE",
);

/**
 * An operation that needs a stored row (`delete()`, `refresh()`, `load*()`…)
 * ran on a local instance.
 */
export class E_MODEL_NOT_PERSISTED extends Exception {
	static override code = "E_MODEL_NOT_PERSISTED";
	static override help =
		"Load it from the database (find/query) first — a locally-built instance with a manual primary key is not a database row.";
}

/** The same operation ran on a persisted projection that holds no primary key. */
export class E_MISSING_PRIMARY_KEY extends Exception {
	static override code = "E_MISSING_PRIMARY_KEY";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/** `save()` was called on a deleted instance. */
export const E_MODEL_DELETED = createError(
	"Cannot mutate delete model instance",
	"E_MODEL_DELETED",
	500,
);
E_MODEL_DELETED.help =
	"The instance was already deleted; re-fetch it before saving again.";

/** `associate()` was given a parent whose key is not set. */
export class E_MISSING_OWNER_KEY extends Exception {
	static override code = "E_MISSING_OWNER_KEY";
	static override help =
		"Pass an owner whose key is set — a keyless aggregate/alias projection can't be a foreign-key target.";
}

/**
 * A relation was accessed that was neither eager-loaded nor has a lazy loader
 * on the entity. Reserved: the runtime does not raise it today.
 */
export class E_RELATION_NOT_LOADED extends Exception {
	static override code = "E_RELATION_NOT_LOADED";

	constructor(
		public readonly entityClass: string,
		public readonly relationName: string,
		options?: ErrorOptions,
	) {
		super(
			`Relation '${relationName}' on ${entityClass} was not loaded.`,
			options,
		);
		this.help = `Call .preload('${relationName}') on the query or .load('${relationName}') on the instance.`;
	}
}

/**
 * A save found the row modified by another transaction since it was read.
 * Reserved: atlas has no version column yet, so nothing raises it today.
 */
export class E_OPTIMISTIC_LOCK extends Exception {
	static override code = "E_OPTIMISTIC_LOCK";
	static override help =
		"Reload the entity, reapply your changes, and try again.";

	constructor(
		public readonly entityClass: string,
		public readonly primaryKey: unknown,
		public readonly expectedVersion: number,
		options?: ErrorOptions,
	) {
		super(
			`Optimistic lock failure saving ${entityClass}#${String(primaryKey)}: the row was modified by another transaction (expected version ${expectedVersion}).`,
			options,
		);
	}
}

/**
 * Raw SQL refused by strict mode: a repository's `raw()`, a `db.raw()` value
 * written by a repository, or `whereRaw()`, `havingRaw()`, `joinRaw()`.
 */
export class E_STRICT_MODE extends Exception {
	static override code = "E_STRICT_MODE";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/**
 * The connection has no interactive `transaction()` (a model transaction, or
 * `useTransaction()` in tests).
 */
export class E_NO_INTERACTIVE_TRANSACTION extends Exception {
	static override code = "E_NO_INTERACTIVE_TRANSACTION";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/**
 * A list was bound to a single placeholder. Expand it into one placeholder per
 * element.
 */
export const E_ARRAY_PARAM = createError<[number]>(
	"Parameter $%d is an array, which cannot be bound as a single value. " +
		"For a list of values, expand it into one placeholder per element — `whereIn(column, values)` " +
		"does this, and so does building the placeholders yourself: " +
		`\`IN (\${values.map((_, i) => '$' + (i + 1)).join(', ')})\`. ` +
		"For a JSON column, declare it — `@Column({ type: 'jsonb' })` — and the array is serialised " +
		"and parsed for you; on a raw query with no entity behind it, pass `JSON.stringify(value)`. " +
		"Binding the array itself sends Postgres text where it expects an array, and it reports " +
		"a nonsensical dimension count rather than a type error.",
	"E_ARRAY_PARAM",
);

/** A connection name that was never registered. */
export const E_UNMANAGED_DB_CONNECTION = createError<[string]>(
	"Cannot connect to unregistered connection %s",
	"E_UNMANAGED_DB_CONNECTION",
	500,
);
E_UNMANAGED_DB_CONNECTION.help =
	"Declare it under `connections` in config/database.ts, or register it with manager.add(name, config).";

/**
 * There is no connection to use at all: no default one (atlas has not
 * booted), a default name the config does not declare, or a repository built
 * without a connection.
 */
export class E_ATLAS_MISSING_CONNECTION extends Exception {
	static override code = "E_ATLAS_MISSING_CONNECTION";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/** `select()` was called with no columns. Provide at least one column name. */
export const E_ATLAS_EMPTY_SELECT = createError(
	"select() requires at least one column",
	"E_ATLAS_EMPTY_SELECT",
);

/** `paginate()` was called with `page < 1`. */
export const E_ATLAS_INVALID_PAGE = createError(
	"page must be >= 1",
	"E_ATLAS_INVALID_PAGE",
);

/** `paginate()` was called with `perPage < 1`. */
export const E_ATLAS_INVALID_PER_PAGE = createError(
	"perPage must be >= 1",
	"E_ATLAS_INVALID_PER_PAGE",
);

/** A page, page size or row count that is not a finite, non-negative integer. */
export class E_ATLAS_INVALID_PAGINATION extends Exception {
	static override code = "E_ATLAS_INVALID_PAGINATION";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/** A CTE name must be a valid identifier (letters, numbers, underscores). */
export const E_ATLAS_INVALID_CTE_NAME = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_CTE_NAME",
);

/** A `where()` or `wherePivot()` operator outside the allowed list. */
export const E_ATLAS_INVALID_OPERATOR = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_OPERATOR",
);

/**
 * `increment()` / `decrement()` was given an amount that is not a finite
 * number.
 */
export class E_ATLAS_INVALID_INCREMENT extends Exception {
	static override code = "E_ATLAS_INVALID_INCREMENT";
	static override help =
		"Validate the amount (Number.isFinite) before calling it.";
}

/**
 * A date column was given a value that is not a valid date. The arguments
 * are `[model.column, reason]`; `help` says what to pass instead.
 */
export class E_INVALID_DATE_COLUMN_VALUE extends Exception {
	static override code = "E_INVALID_DATE_COLUMN_VALUE";
	static override status = 500;
	static override message = 'Invalid value for "%s". %s';

	constructor(
		args: [string, string],
		options?: ErrorOptions & { help?: string },
	) {
		super(format(E_INVALID_DATE_COLUMN_VALUE.message, args), options);
		this.name = "Exception";
		if (options?.help !== undefined) this.help = options.help;
	}
}

/**
 * A query value is `undefined`. Validate the input first; use `whereNull()` to
 * match NULL.
 */
export class E_ATLAS_UNDEFINED_BINDING extends Exception {
	static override code = "E_ATLAS_UNDEFINED_BINDING";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/**
 * `useTransaction(trx)` was given a transaction of another dialect than the
 * query was built for. Start the transaction on the query's connection, or
 * build the query from the transaction.
 */
export class E_ATLAS_DIALECT_MISMATCH extends Exception {
	static override code = "E_ATLAS_DIALECT_MISMATCH";
	static override help =
		"Start the transaction on the connection the query was built from, or build the query from the transaction (trx.from(), Model.query({ client: trx })).";
}

/**
 * A raw fragment's placeholders and bindings do not match: a placeholder with
 * no binding, a binding no placeholder uses, `?` mixed with `$N` or with named
 * bindings, or a `??` binding that is not a string.
 */
export const E_ATLAS_RAW_BINDINGS = createError<[string]>(
	"%s",
	"E_ATLAS_RAW_BINDINGS",
);

/**
 * A `db.raw()` was used where it cannot go: `defaultTo(db.raw())` with
 * bindings, which a column default cannot carry, or a raw value compared with
 * an operator that takes none (`IS NULL`, `BETWEEN`).
 */
export class E_ATLAS_RAW_VALUE extends Exception {
	static override code = "E_ATLAS_RAW_VALUE";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/**
 * `defaultTo()` was given a number that is not a SQL number (`NaN`,
 * `Infinity`).
 */
export class E_ATLAS_NON_FINITE_NUMBER extends Exception {
	static override code = "E_ATLAS_NON_FINITE_NUMBER";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/** A factory uses faker, an optional peer of atlas that is not installed. */
export class E_ATLAS_FACTORY_FAKER_MISSING extends Exception {
	static override code = "E_ATLAS_FACTORY_FAKER_MISSING";
	static override help =
		"pnpm add -D @faker-js/faker — it is only needed by projects that define factories, so it is not installed with the ORM.";
}

/**
 * `make:factory` got a model name that would leave the factories folder (a
 * path separator, a quote, a backtick or `..`).
 */
export const E_ATLAS_FACTORY_INVALID = createError<[string]>(
	"%s",
	"E_ATLAS_FACTORY_INVALID",
);

/**
 * `make:seeder` got a name that would leave the seeders folder (a path
 * separator, a quote, a backtick or `..`).
 */
export const E_ATLAS_SEEDER_INVALID = createError<[string]>(
	"%s",
	"E_ATLAS_SEEDER_INVALID",
);

/**
 * The Rust query compiler is not loaded: the native package for your platform
 * is missing. The message names the platform package to install; the load
 * error is its `cause`. See
 * [Troubleshooting](/en/guides/database/orm/troubleshooting).
 */
export const E_ATLAS_NAPI_NOT_FOUND = createError<[fix: string]>(
	"Rust query compiler not available. Fix: %s",
	"E_ATLAS_NAPI_NOT_FOUND",
);

/**
 * The connection asks for `sslmode=prefer` or `allow` (URL or `PGSSLMODE`),
 * which fall back to plaintext without saying so. Use `disable` or `verify-
 * full`. See [Encrypted
 * connections](/en/guides/database/orm/connections#encrypted-connections-tls).
 */
export const E_ATLAS_INSECURE_SSLMODE = createError<[string]>(
	"%s",
	"E_ATLAS_INSECURE_SSLMODE",
);

/** LISTEN / NOTIFY was used on a connection that is not PostgreSQL. */
export const E_ATLAS_LISTEN_UNSUPPORTED = createError<[string]>(
	"%s",
	"E_ATLAS_LISTEN_UNSUPPORTED",
);

/** The listener could not open its own PostgreSQL connection. */
export const E_ATLAS_LISTEN_CONNECT = createError<[string]>(
	"%s",
	"E_ATLAS_LISTEN_CONNECT",
);

/** The listener's connection failed; every subscription is told. */
export const E_ATLAS_LISTEN_CONNECTION = createError<[string]>(
	"%s",
	"E_ATLAS_LISTEN_CONNECTION",
);

/** A LISTEN channel name is empty, longer than 63 bytes, or holds a NUL. */
export const E_ATLAS_LISTEN_CHANNEL = createError<[string]>(
	"%s",
	"E_ATLAS_LISTEN_CHANNEL",
);

/** The listener was closed while a request was pending. */
export const E_ATLAS_LISTEN_CLOSED = createError<[string]>(
	"%s",
	"E_ATLAS_LISTEN_CLOSED",
);

/** The native listener reported an event atlas does not define. */
export const E_ATLAS_LISTEN_EVENT = createError<[unknown]>(
	"atlas: the native listener reported an event it does not define: %s",
	"E_ATLAS_LISTEN_EVENT",
);

/** `migrations.tableName` is not a plain identifier. */
export const E_ATLAS_MIGRATION_INVALID_TABLE_NAME = createError<[unknown]>(
	"Invalid tracking-table name: %s. Must match /^[A-Za-z_][A-Za-z0-9_]*$/ (letters, digits, underscores; not starting with a digit).",
	"E_ATLAS_MIGRATION_INVALID_TABLE_NAME",
);

/**
 * A migration recorded as applied has no file in the migrations directory, so
 * it cannot be rolled back.
 */
export const E_MISSING_SCHEMA_FILES = createError<[string]>(
	'Cannot perform rollback. Schema file "%s" is missing',
	"E_MISSING_SCHEMA_FILES",
	500,
);

/** A migration file does not default-export a class. */
export const E_ATLAS_MIGRATION_INVALID = createError<[string]>(
	"%s",
	"E_ATLAS_MIGRATION_INVALID",
);

/** Two migration files share a name. */
export class E_ATLAS_MIGRATION_DUPLICATE extends Exception {
	static override code = "E_ATLAS_MIGRATION_DUPLICATE";
	static override help =
		"Keep one file per migration — point migrationsDir at either the sources or the compiled output, not a folder holding both.";
}

/** A migration compiled to no SQL statement. */
export const E_ATLAS_MIGRATION_COMPILE_EMPTY = createError<[string]>(
	"%s",
	"E_ATLAS_MIGRATION_COMPILE_EMPTY",
);

/**
 * Another run holds the migration lock. Wait for it, or clear a stale lock
 * with `migration:unlock`; `help` names the holder when it is known.
 */
export const E_UNABLE_ACQUIRE_LOCK = createError(
	"Unable to acquire lock. Concurrent migrations are not allowed",
	"E_UNABLE_ACQUIRE_LOCK",
	500,
);

/**
 * The run finished, but its lock was no longer there to release: something
 * cleared it while the run held it (`migration:unlock`, or another run
 * clearing a lock it took for stale), so another run may have migrated at
 * the same time.
 */
export const E_UNABLE_RELEASE_LOCK = createError(
	"Migration completed, but unable to release database lock",
	"E_UNABLE_RELEASE_LOCK",
	500,
);

/** A rollback, reset, refresh, fresh or wipe in production without `--force`. */
export class E_ROLLBACK_DISABLED_IN_PRODUCTION extends Exception {
	static override code = "E_ROLLBACK_DISABLED_IN_PRODUCTION";
	static override help =
		"This guard exists to prevent dropping production data by accident.";
}

/** A migration tried to write while running under `dryRun()`. */
export class E_ATLAS_DRY_RUN_WRITE extends Exception {
	static override code = "E_ATLAS_DRY_RUN_WRITE";
	static override help =
		"Check `this.dryRun` in up()/down() to skip side effects while the SQL is only being previewed.";
}

/**
 * A migration uses `this.defer()` on an adapter without an interactive
 * `transaction()`.
 */
export class E_DEFER_REQUIRES_TRANSACTION extends Exception {
	static override code = "E_DEFER_REQUIRES_TRANSACTION";
	static override help =
		"Use a real connection (createNapiConnection) or an adapter that implements transaction(); otherwise remove this.defer().";
}

/**
 * Suspending MySQL foreign-key checks needs an interactive `transaction()` to
 * restore them.
 */
export class E_MYSQL_FK_REQUIRES_TRANSACTION extends Exception {
	static override code = "E_MYSQL_FK_REQUIRES_TRANSACTION";
	static override help =
		"Use a real connection (createNapiConnection) or an adapter that implements transaction().";
}

/** The schema dump was made for another dialect. */
export const E_SCHEMA_DUMP_DIALECT_MISMATCH = createError<[unknown, unknown]>(
	"Schema dump is for '%s', but this connection is '%s'.",
	"E_SCHEMA_DUMP_DIALECT_MISMATCH",
);

/** The schema dump uses another migrations tracking table. */
export const E_SCHEMA_DUMP_TABLE_MISMATCH = createError<[unknown, unknown]>(
	"Schema dump uses tracking table '%s', but this runner expects '%s'.",
	"E_SCHEMA_DUMP_TABLE_MISMATCH",
);

/** The seeders directory does not exist. */
export class E_SEEDER_DIR_NOT_FOUND extends Exception {
	static override code = "E_SEEDER_DIR_NOT_FOUND";
	static override help =
		"Create the directory or adjust the path passed to runSeederDirectory.";
}

/** A seeder file does not export a seeder class. */
export class E_SEEDER_INVALID extends Exception {
	static override code = "E_SEEDER_INVALID";

	constructor(message: string, help?: string, options?: ErrorOptions) {
		super(message, options);
		if (help !== undefined) this.help = help;
	}
}

/** A seeder path resolves outside the seeders directory. */
export const E_SEEDER_INVALID_PATH = createError<[string]>(
	"%s",
	"E_SEEDER_INVALID_PATH",
);

/**
 * `schema.<method>()` needs a live connection; it is available inside a
 * migration, not on a standalone `Schema`.
 */
export const E_NO_CONNECTION = createError<[unknown]>(
	"schema.%s() needs a live connection — it is available inside a migration, not on a standalone Schema.",
	"E_NO_CONNECTION",
);

/** `schema.alterTable()` declared no operation. */
export const E_ALTER_EMPTY = createError<[unknown]>(
	"schema.alterTable('%s') declared no operations",
	"E_ALTER_EMPTY",
);

/**
 * `.alter()` does not follow a column definition, or an alter-only method was
 * used on a new table.
 */
export const E_ALTER_MISUSE = createError<[string]>("%s", "E_ALTER_MISUSE");

/** A check constraint (or `checkBetween`) was given no value or interval. */
export const E_CHECK_EMPTY = createError<[string]>("%s", "E_CHECK_EMPTY");

/** `checkBetween` was given an interval that is not `[min, max]`. */
export const E_CHECK_RANGE = createError<[string]>("%s", "E_CHECK_RANGE");

/** A constraint lists no columns. */
export const E_CONSTRAINT_EMPTY = createError<[string]>(
	"%s",
	"E_CONSTRAINT_EMPTY",
);

/** An enum column declares no values. */
export const E_ENUM_EMPTY = createError<[string]>("%s", "E_ENUM_EMPTY");

/**
 * A view has no SELECT, or a materialized view was asked of a dialect that has
 * none.
 */
export const E_EMPTY_VIEW_SELECT = createError<[string]>(
	"%s",
	"E_EMPTY_VIEW_SELECT",
);

/** `specificType()` was given an empty type. */
export const E_SPECIFIC_TYPE_EMPTY = createError<[string]>(
	"%s",
	"E_SPECIFIC_TYPE_EMPTY",
);

/** A decimal precision SQLite cannot store exactly. */
export const E_SQLITE_DECIMAL_PRECISION = createError<[string]>(
	"%s",
	"E_SQLITE_DECIMAL_PRECISION",
);

/**
 * A SQL feature the connection's dialect does not have (`DISTINCT ON` off
 * Postgres, `UNION ALL` variants on SQLite, advisory locks on SQLite…).
 */
export const E_UNSUPPORTED = createError<[string]>("%s", "E_UNSUPPORTED");

/**
 * An identifier (table, column, alias) is not a plain SQL name. Atlas checks
 * it before compiling, and the Rust compiler checks it again.
 */
export const E_UNSAFE_IDENTIFIER = createError<[string]>(
	"%s",
	"E_UNSAFE_IDENTIFIER",
);

/**
 * The Rust compiler refused a SQL fragment, or a keyword outside its allowed
 * list (set operation, ORDER BY direction, lock mode, referential action…).
 */
export const E_UNSAFE_SQL = createError<[string]>("%s", "E_UNSAFE_SQL");

/** The Rust compiler refused an expression. */
export const E_UNSAFE_EXPRESSION = createError<[string]>(
	"%s",
	"E_UNSAFE_EXPRESSION",
);

/** The Rust compiler refused a literal value. */
export const E_UNSAFE_LITERAL = createError<[string]>("%s", "E_UNSAFE_LITERAL");

/** The Rust compiler refused an input that looks like an injection attempt. */
export const E_INJECTION_PATTERN = createError<[string]>(
	"%s",
	"E_INJECTION_PATTERN",
);

/**
 * The statement the Rust compiler received is malformed: a clause misses a
 * field it needs, a field has the wrong shape, or the spec does not parse.
 * Atlas builds every spec itself, so this points at an atlas bug or at a spec
 * built by hand.
 */
export const E_ATLAS_INVALID_SPEC = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_SPEC",
);

/**
 * An INSERT or an upsert reached the compiler with no row (`insert([])`,
 * `multiInsert([])`), or with several rows that set no column: only a single
 * row can take every column's default.
 */
export const E_ATLAS_EMPTY_INSERT = createError<[string]>(
	"%s",
	"E_ATLAS_EMPTY_INSERT",
);

/** `schema.createTable()` declared no column. */
export const E_ATLAS_EMPTY_TABLE = createError<[string]>(
	"%s",
	"E_ATLAS_EMPTY_TABLE",
);

/**
 * The connection names a dialect atlas does not know. Atlas compiles for
 * `sqlite`, `postgres` (or `postgresql`) and `mysql` (or `mariadb`).
 */
export const E_UNSUPPORTED_CLIENT = createError<[string]>(
	"%s",
	"E_UNSUPPORTED_CLIENT",
);

/**
 * A connection config has no `url` (or `connection`), or the database config
 * has neither a top-level `url` nor `connections`.
 */
export const E_ATLAS_NO_URL = createError<[string]>("%s", "E_ATLAS_NO_URL");

/**
 * The `db` service (`@c9up/atlas/services/db`) was used before
 * `AtlasProvider.boot()` ran.
 */
export const E_ATLAS_NOT_BOOTED = createError(
	"[atlas] db singleton accessed before AtlasProvider.boot() ran. Check that `@c9up/atlas/provider` is listed in your reamrc.ts providers and that `config/database.ts` defines at least one connection.",
	"E_ATLAS_NOT_BOOTED",
);

/**
 * A container connection token was resolved before boot opened it, or after
 * shutdown closed it.
 */
export const E_ATLAS_CONNECTION_UNAVAILABLE = createError<[unknown]>(
	"AtlasProvider: connection '%s' is not available (boot failed or the provider was shut down).",
	"E_ATLAS_CONNECTION_UNAVAILABLE",
);

/**
 * `AtlasProvider.boot()` could not open one or more configured connections;
 * `cause` holds the first failure.
 */
export const E_ATLAS_BOOT_FAILED = createError<[string]>(
	"%s",
	"E_ATLAS_BOOT_FAILED",
);

/**
 * A write ran on a query builder scoped to a connection opened with `{ mode:
 * 'read' }`.
 */
export const E_ATLAS_READ_ONLY = createError(
	"[atlas] write blocked: this query builder is scoped to a connection opened with { mode: 'read' }. Use { mode: 'write' } for mutations.",
	"E_ATLAS_READ_ONLY",
);

/** A SQLite pragma key or value is outside `[A-Za-z0-9_]`. */
export const E_ATLAS_INVALID_PRAGMA = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_PRAGMA",
);

/**
 * The Rust database driver could not be loaded, or the loaded binary is stale.
 * The message names the platform package to install; `cause` holds the load
 * failure.
 */
export const E_ATLAS_DB_DRIVER_NOT_FOUND = createError<[string]>(
	"%s",
	"E_ATLAS_DB_DRIVER_NOT_FOUND",
);

/**
 * `refresh()`, `fresh()`, `load()`, `loadCount()`, `loadAggregate()` or
 * `related()` was called on an entity no repository hydrated.
 */
export const E_ATLAS_NO_REPOSITORY = createError<[string]>(
	"%s",
	"E_ATLAS_NO_REPOSITORY",
);

/** A model declares both `static fillable` and `static guarded`. */
export const E_ATLAS_FILLABLE_AND_GUARDED = createError<[unknown]>(
	"%s: cannot declare both 'fillable' and 'guarded'",
	"E_ATLAS_FILLABLE_AND_GUARDED",
);

/**
 * A relation's target (or through model) does not resolve to a `BaseEntity`
 * subclass.
 */
export const E_ATLAS_INVALID_ENTITY_CLASS = createError<[unknown]>(
	"%s must resolve to an entity class (a BaseEntity subclass).",
	"E_ATLAS_INVALID_ENTITY_CLASS",
);

/**
 * A hook decorator is not on a static method, or `addHook()` was given
 * something other than a function.
 */
export const E_ATLAS_INVALID_HOOK = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_HOOK",
);

/**
 * A date column decorator was applied to something other than a class
 * property.
 */
export const E_ATLAS_INVALID_DECORATOR = createError(
	"a date column decorates a class property",
	"E_ATLAS_INVALID_DECORATOR",
);

/**
 * A relation name the model does not declare (`load()`, `withCount()`,
 * `whereHas()`…).
 */
export const E_UNDEFINED_RELATIONSHIP = createError<[string, string]>(
	'"%s" is not defined as a relationship on "%s" model',
	"E_UNDEFINED_RELATIONSHIP",
	500,
);

/**
 * Relation metadata lacks what its kind needs: a `@ManyToMany` without pivot
 * options, a through relation without a through model.
 */
export const E_ATLAS_INVALID_RELATION = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_RELATION",
);

/**
 * `related()` was asked for an operation its relation does not have: a write
 * on `@BelongsTo` other than `associate()` / `dissociate()`, a write on a
 * read-only through relation, a bulk write on `@HasOne`.
 */
export const E_ATLAS_RELATION_UNSUPPORTED_OPERATION = createError<[string]>(
	"%s",
	"E_ATLAS_RELATION_UNSUPPORTED_OPERATION",
);

/** `associate()` was given `null` or `undefined`; use `dissociate()`. */
export const E_ATLAS_ASSOCIATE_NULL = createError<[unknown]>(
	"related('%s').associate() rejects null/undefined — use dissociate() instead",
	"E_ATLAS_ASSOCIATE_NULL",
);

/**
 * `attach()` / `sync()` extras name a pivot key column or a pivot timestamp
 * column.
 */
export const E_ATLAS_PIVOT_KEY_COLLISION = createError<[string]>(
	"%s",
	"E_ATLAS_PIVOT_KEY_COLLISION",
);

/**
 * A `@Column` `prepare` / `consume` adapter threw (`cause` holds its error) or
 * returned a Promise.
 */
export const E_ATLAS_COLUMN_ADAPTER = createError<[string]>(
	"%s",
	"E_ATLAS_COLUMN_ADAPTER",
);

/** A `@HasOne` preload found more than one related row for a parent. */
export const E_ATLAS_HAS_ONE_MULTIPLE_ROWS = createError<
	[unknown, unknown, unknown, unknown, unknown, unknown, unknown]
>(
	'@HasOne invariant violated: %s.%s found %s rows in "%s" for parent %s=%s. Use @HasMany if multiple rows are expected, or add a unique index on "%s".',
	"E_ATLAS_HAS_ONE_MULTIPLE_ROWS",
);

/** A through relation's `secondLocalKey` maps to several parents. */
export const E_ATLAS_THROUGH_KEY_NOT_UNIQUE = createError<
	[unknown, unknown, unknown, unknown, unknown]
>(
	"@HasManyThrough/@HasOneThrough '%s': duplicate secondLocalKey='%s' on %s maps to multiple parents. Either set secondLocalKey to a unique column (default: %s.%s) or fix the underlying data.",
	"E_ATLAS_THROUGH_KEY_NOT_UNIQUE",
);

/** `sole()` matched more than one row. */
export const E_ATLAS_MULTIPLE_ROWS = createError<[unknown]>(
	"Expected exactly one %s but the query matched multiple rows (sole()).",
	"E_ATLAS_MULTIPLE_ROWS",
);

/** `select()` was given a subquery not named with `.as()`. */
export const E_ATLAS_SUBQUERY_ALIAS = createError(
	"select(subquery) requires the subquery to be named with .as('alias')",
	"E_ATLAS_SUBQUERY_ALIAS",
);

/** An alias is not a bare identifier. */
export const E_ATLAS_INVALID_ALIAS = createError<[unknown]>(
	"Invalid alias '%s' — expected a bare identifier.",
	"E_ATLAS_INVALID_ALIAS",
);

/**
 * A `whereExpr()` extra expression holds forbidden characters, unbalanced
 * parentheses, a SQL keyword, or a function outside the allowed list.
 */
export const E_ATLAS_INVALID_EXPRESSION = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_EXPRESSION",
);

/** A `whereJsonPath()` path does not start with `$`. */
export const E_ATLAS_INVALID_JSON_PATH = createError<[unknown]>(
	"whereJsonPath: path '%s' must start with '$' (e.g. '$.a.b' or '$.items[0]')",
	"E_ATLAS_INVALID_JSON_PATH",
);

/** `groupLimit()` was given a value that is not a non-negative integer. */
export const E_ATLAS_INVALID_GROUP_LIMIT = createError<[unknown]>(
	"groupLimit must be a non-negative integer, got %s",
	"E_ATLAS_INVALID_GROUP_LIMIT",
);

/** `skipLocked()` / `noWait()` was used without a base row lock. */
export const E_ATLAS_INVALID_LOCK = createError<[unknown]>(
	"%s requires a base row lock — call forUpdate()/forShare()/forNoKeyUpdate()/forKeyShare() as well (a modifier alone emits no lock at all).",
	"E_ATLAS_INVALID_LOCK",
);

/** `comment()` text contains `*\/`. */
export const E_ATLAS_INVALID_COMMENT = createError(
	"comment() text may not contain '*/'",
	"E_ATLAS_INVALID_COMMENT",
);

/** A string-form `join()` is missing its right operand. */
export const E_ATLAS_INVALID_JOIN = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_JOIN",
);

/** A `withAggregate()` callback set no aggregate. */
export const E_ATLAS_INVALID_AGGREGATE = createError<[unknown]>(
	"withAggregate('%s') callback must set an aggregate via .sum/.avg/.min/.max/.count",
	"E_ATLAS_INVALID_AGGREGATE",
);

/** `pluck()` read a column whose value is an object or a relation. */
export const E_ATLAS_INVALID_PLUCK = createError<[unknown]>(
	"pluck('%s') rejected — column is an object/relation",
	"E_ATLAS_INVALID_PLUCK",
);

/**
 * `apply()` reached a scope the model does not declare, or a model with no
 * `static scopes`.
 */
export const E_ATLAS_UNKNOWN_SCOPE = createError<[string]>(
	"%s",
	"E_ATLAS_UNKNOWN_SCOPE",
);

/** `cursorPaginate()` was called without an order column. */
export const E_ATLAS_CURSOR_ORDER_REQUIRED = createError(
	"cursorPaginate requires at least one orderBy column",
	"E_ATLAS_CURSOR_ORDER_REQUIRED",
);

/**
 * A `cursorPaginate()` cursor does not decode, or its tuple length does not
 * match the order columns.
 */
export const E_ATLAS_INVALID_CURSOR = createError<[string]>(
	"%s",
	"E_ATLAS_INVALID_CURSOR",
);

/**
 * An update was given an empty payload: a model query's `update()`, or an
 * UPDATE with no SET assignment that reached the compiler.
 */
export const E_ATLAS_EMPTY_UPDATE = createError(
	"update() requires a non-empty payload",
	"E_ATLAS_EMPTY_UPDATE",
);

/** A query ran longer than its `timeout(ms)`. */
export const E_ATLAS_QUERY_TIMEOUT = createError<[unknown]>(
	"Query timed out after %sms",
	"E_ATLAS_QUERY_TIMEOUT",
);

/** The query compiler returned no statement. */
export const E_ATLAS_COMPILE_EMPTY = createError(
	"atlas: the query compiler produced no statement",
	"E_ATLAS_COMPILE_EMPTY",
);

/**
 * `verifySchema()` in `throw` mode found drift between the models and the
 * database; the message lists the findings.
 */
export const E_ATLAS_SCHEMA_MISMATCH = createError<[string]>(
	"%s",
	"E_ATLAS_SCHEMA_MISMATCH",
);

/**
 * A schema-dump manifest (`.meta.json`) is not version 1 with a
 * `schemaTableName`.
 */
export const E_ATLAS_SCHEMA_DUMP_MANIFEST = createError<[unknown]>(
	"Invalid schema-dump manifest at %s: expected version 1 with a schemaTableName.",
	"E_ATLAS_SCHEMA_DUMP_MANIFEST",
);

/**
 * `schema:dump` read a sequence parameter from the catalog that is not an
 * integer.
 */
export const E_ATLAS_SCHEMA_DUMP_SEQUENCE = createError<[unknown]>(
	"[atlas:schema:dump] unexpected sequence parameter: %s",
	"E_ATLAS_SCHEMA_DUMP_SEQUENCE",
);

/** `schema:dump --prune` ran without a `migrationsDir`. */
export const E_ATLAS_SCHEMA_DUMP_PRUNE = createError(
	"schema:dump --prune requires a migrationsDir",
	"E_ATLAS_SCHEMA_DUMP_PRUNE",
);

/**
 * `schema:generate` read a table or column name that does not make a
 * TypeScript identifier.
 */
export const E_ATLAS_SCHEMA_GENERATE_IDENTIFIER = createError<
	[unknown, unknown, unknown]
>(
	"[atlas:schema:generate] the %s %s does not make a TypeScript identifier (%s); rename it or skip this table.",
	"E_ATLAS_SCHEMA_GENERATE_IDENTIFIER",
);

/**
 * A database assertion (`assertHas`, `assertMissing`, `assertCount`,
 * `assertEmpty`) does not hold.
 */
export const E_ATLAS_DB_ASSERTION = createError<[string]>(
	"%s",
	"E_ATLAS_DB_ASSERTION",
);

/**
 * A factory's `create()` has no connection: none passed and none bound with
 * `.client()` / `.connection()`.
 */
export const E_ATLAS_FACTORY_NO_CONNECTION = createError<[unknown]>(
	"Factory %s: no connection — pass one to create()/createMany() or bind one with .client()/.connection().",
	"E_ATLAS_FACTORY_NO_CONNECTION",
);

/** A factory state is not defined. */
export const E_ATLAS_FACTORY_STATE = createError<[unknown, unknown]>(
	"Factory state '%s' is not defined on %sFactory",
	"E_ATLAS_FACTORY_STATE",
);

/**
 * A factory's `with()` names a relation the model does not declare, or one
 * with no related factory.
 */
export const E_ATLAS_FACTORY_UNKNOWN_RELATION = createError<[string]>(
	"%s",
	"E_ATLAS_FACTORY_UNKNOWN_RELATION",
);

/**
 * A factory's `pivotAttributes` array length differs from the related-row
 * count.
 */
export const E_ATLAS_FACTORY_PIVOT_LENGTH = createError<
	[unknown, unknown, unknown]
>(
	"Factory .with('%s'): pivotAttributes array length (%s) must match the related-row count (%s).",
	"E_ATLAS_FACTORY_PIVOT_LENGTH",
);

/**
 * A factory's `with()` names a relation kind it cannot build (a through
 * relation).
 */
export const E_ATLAS_FACTORY_UNSUPPORTED_RELATION = createError<
	[unknown, unknown]
>(
	"Factory .with('%s'): '%s' relations are not supported",
	"E_ATLAS_FACTORY_UNSUPPORTED_RELATION",
);

/** `testUtils().db().seed()` ran without a `seedersDir`. */
export const E_ATLAS_SEEDER_DIR_REQUIRED = createError(
	"testUtils.db().seed() needs a `seedersDir` (pass it to testUtils()).",
	"E_ATLAS_SEEDER_DIR_REQUIRED",
);

/**
 * An invariant inside atlas broke, or the Rust compiler panicked or could not
 * serialize what it compiled: always a bug in atlas itself.
 */
export const E_ATLAS_INTERNAL = createError<[string]>("%s", "E_ATLAS_INTERNAL");
