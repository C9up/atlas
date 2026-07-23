/**
 * Query observability — atlas's equivalent of Lucid's `db:query` event.
 *
 * Agnostic by design: atlas cannot import the framework's emitter (it is a
 * standalone package), so it owns a tiny listener registry instead. An
 * integration package bridges it to whatever emitter the app uses:
 *
 *     onDbQuery((event) => emitter.emit('db:query', event))
 *
 * Emission is opt-in. It costs a `performance.now()` pair per query, so it only
 * happens when a connection is configured with `debug: true` or a single query
 * asks via `.debug()` — and even then, only if someone is listening.
 */

/**
 * A single executed SQL statement. Mirrors Lucid's `db:query` payload, minus
 * the fields that only make sense inside AdonisJS.
 */
export interface DbQueryEvent {
	/** The SQL as sent to the driver, placeholders included. */
	sql: string;
	/** The bound parameters. Never interpolated into `sql`. */
	bindings: readonly unknown[];
	/** Wall-clock duration in milliseconds, including the NAPI round-trip. */
	duration: number;
	/** Connection name, when the app named it. */
	connection?: string;
	/** Entity class name, when the query came from a repository/model. */
	model?: string;
	/** The call that produced it (`exec`, `first`, `paginate`, …). */
	method?: string;
	/** True for schema statements (migrations), false for DML/queries. */
	ddl?: boolean;
	/** True when the statement ran inside an interactive transaction. */
	inTransaction?: boolean;
	/** Set when the statement threw — the event is emitted either way. */
	error?: Error;
	/**
	 * Arbitrary metadata a caller attached via `query.reporterData({...})`
	 * (Adonis Lucid `reporterData`) — request id, user id, feature flag, …
	 */
	reporterData?: Record<string, unknown>;
}

export type DbQueryListener = (event: DbQueryEvent) => void;

const listeners = new Set<DbQueryListener>();

/**
 * Subscribe to every observed query. Returns an unsubscribe function.
 *
 * A listener that throws would otherwise take down the query that triggered it,
 * so throws are swallowed — observability must never change behaviour.
 */
export function onDbQuery(listener: DbQueryListener): () => void {
	listeners.add(listener);
	return () => {
		listeners.delete(listener);
	};
}

/** Remove every listener. Intended for test teardown. */
export function clearDbQueryListeners(): void {
	listeners.clear();
}

/**
 * Whether anyone is listening. Checked before timing a query so the
 * instrumentation costs nothing when unused.
 */
export function hasDbQueryListeners(): boolean {
	return listeners.size > 0;
}

/** Emit to every listener. Package-internal. */
export function emitDbQuery(event: DbQueryEvent): void {
	for (const listener of listeners) {
		try {
			listener(event);
		} catch {
			// A broken listener must not fail the query it is reporting on.
		}
	}
}

/**
 * Render a query event as a single log line (Lucid's `prettyPrint`).
 *
 * The bindings are appended as JSON, NOT interpolated into the SQL: an
 * interpolated line reads like runnable SQL while having none of the escaping
 * that made the real statement safe, and it is exactly the string someone
 * copies into a console later.
 */
export function prettyPrintQuery(event: DbQueryEvent): string {
	const parts = [`${event.duration.toFixed(2)}ms`];
	if (event.connection) parts.push(event.connection);
	if (event.model) parts.push(event.model);
	if (event.method) parts.push(event.method);
	if (event.inTransaction) parts.push("trx");
	if (event.error) parts.push(`ERROR: ${event.error.message}`);

	const head = `[atlas] ${parts.join(" ")}`;
	const bindings =
		event.bindings.length > 0 ? ` -- ${safeJson(event.bindings)}` : "";
	return `${head} ${event.sql}${bindings}`;
}

/** JSON that can't throw on a circular / non-serialisable binding. */
function safeJson(value: unknown): string {
	try {
		return JSON.stringify(value, (_k, v) =>
			typeof v === "bigint" ? `${v}n` : v,
		);
	} catch {
		return "[unserialisable bindings]";
	}
}
