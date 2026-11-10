// GENERATED FROM THE RUST — do not edit.
//
// Produced by scripts/generate-napi-types.mjs from napi-derive's type-def
// output. Editing this file by hand puts it back where it started: a
// description that can disagree with the code it describes.

/** A raw fragment's named bindings rewritten to the `?` notation. */

export interface NamedBindings {
	/** The fragment with `:name` as `?` and `:name:` as `??`. */
	sql: string;
	/** The binding names in placeholder order. */
	names: Array<string>;
}

/** NAPI-exposed database connection. */

export declare class ReamDatabase {
	/**
	 * Connect to a database. URL determines the driver:
	 * - "sqlite:path/to/db.sqlite" or "sqlite::memory:"
	 * - "postgres://user:pass@host/dbname"
	 * - "mysql://user:pass@host/dbname"
	 */
	static connect(
		url: string,
		poolMin?: number | undefined | null,
		poolMax?: number | undefined | null,
		pragmas?: Array<Array<string>> | undefined | null,
		connectRetries?: number | undefined | null,
		connectBackoffMs?: number | undefined | null,
		connectTimeoutMs?: number | undefined | null,
	): Promise<ReamDatabase>;
	/** Execute a SELECT query. Returns JSON array of row objects. */
	query(sql: string, paramsJson: string): Promise<string>;
	/**
	 * Like {@link query} with a server-side statement timeout (Lucid
	 * `timeout(ms, { cancel: true })`) — Postgres `statement_timeout`, MySQL
	 * `MAX_EXECUTION_TIME` (SELECT). Returns JSON array of row objects.
	 */
	queryTimed(
		sql: string,
		paramsJson: string,
		timeoutMs: number,
	): Promise<string>;
	/**
	 * Like {@link execute} with a server-side statement timeout (Postgres
	 * `statement_timeout`). Returns the JSON-serialized `ExecResult`.
	 */
	executeTimed(
		sql: string,
		paramsJson: string,
		timeoutMs: number,
	): Promise<string>;
	/**
	 * Execute an INSERT/UPDATE/DELETE. Returns the JSON-serialized `ExecResult`
	 * (`{ rows_affected, last_insert_id }`) so the JS side can read the MySQL/
	 * SQLite auto-increment id (Lucid's insert-without-returning shape).
	 */
	execute(sql: string, paramsJson: string): Promise<string>;
	/**
	 * Run a batch of `[sql, params_json]` pairs atomically in a single transaction.
	 * Accepts JSON `[[sql, params], ...]` and returns the total affected rows.
	 */
	runInTransaction(batchJson: string): Promise<number>;
	/**
	 * Open an interactive transaction pinned to a single pooled connection.
	 * Returns a handle whose `query`/`execute` run on that one connection;
	 * `commit`/`rollback` release it. This is what makes a TS
	 * read-then-decide-then-write atomic — `BEGIN`/`COMMIT` pulled through the
	 * pool would land on different connections and guarantee nothing.
	 */
	begin(isolationLevel?: string | undefined | null): Promise<ReamTransaction>;
	/** Health check. */
	ping(): Promise<void>;
	/** Get pool size. */
	poolSize(): number;
	/**
	 * Open a dedicated PostgreSQL `LISTEN` connection, outside the pool, with
	 * the pool's connect options. Rejects on SQLite and MySQL.
	 */
	listener(): Promise<ReamListener>;
	/** Close the connection pool. */
	close(): Promise<void>;
}

/**
 * A dedicated `LISTEN` connection (see [`ReamDatabase::listener`]). A
 * background task owns the connection; this handle sends it commands and
 * reads what it reports.
 */

export declare class ReamListener {
	/** `LISTEN channel`; resolves once the server acknowledged it. */
	listen(channel: string): Promise<void>;
	/** `UNLISTEN channel`. */
	unlisten(channel: string): Promise<void>;
	/**
	 * The next event as JSON, `null` once the listener is closed:
	 * `{"kind":"notification","channel","payload","processId"}`,
	 * `{"kind":"reconnected"}` or `{"kind":"error","message"}`.
	 */
	next(): Promise<string | null>;
	/** Stop listening and close the connection. Idempotent. */
	close(): Promise<void>;
}

/**
 * An interactive transaction handle (see [`ReamDatabase::begin`]). The pinned
 * `DbTransaction` lives in an async mutex so the separate NAPI calls
 * (execute… → commit) all hit the SAME connection; `commit`/`rollback` take it
 * out (the connection returns to the pool) and any later call sees a clear
 * "transaction already finished" error instead of a silent no-op.
 */

export declare class ReamTransaction {
	/** SELECT on the pinned connection. Returns a JSON array of row objects. */
	query(sql: string, paramsJson: string): Promise<string>;
	/**
	 * INSERT/UPDATE/DELETE on the pinned connection. Returns the JSON-serialized
	 * `ExecResult` (`{ rows_affected, last_insert_id }`).
	 */
	execute(sql: string, paramsJson: string): Promise<string>;
	/**
	 * Commit and release the connection back to the pool. Idempotent-safe: a
	 * second commit/rollback errors with "transaction already finished".
	 */
	commit(): Promise<void>;
	/** Roll back and release the connection back to the pool. */
	rollback(): Promise<void>;
}

/**
 * Compile a full statement (SELECT / INSERT / UPDATE / DELETE / DDL) via a tagged JSON spec.
 * `dialect` is one of "sqlite" | "postgres" | "mysql".
 * Returns a JSON-encoded `CompiledStatement` ({ statements: [...], params: [...] }).
 */

export declare function compileStatement(
	specJson: string,
	dialect: string,
): string;

/** Quote a SQL identifier (validates and wraps in double quotes). */

export declare function quoteIdent(name: string): string;

/**
 * Resolve Knex named bindings (`:name`, `:name:`) with the same SQL scanner
 * as every other placeholder: only names in `keys` and only in SQL code —
 * never inside a string, quoted identifier, comment or dollar-quoted body.
 */

export declare function namedToPositional(
	sql: string,
	keys: Array<string>,
	dialect: string,
): NamedBindings;
