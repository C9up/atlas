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

/**
 * A statement the compiler REFUSED to build.
 *
 * The query builder binds every value, so a payload like `'; DROP TABLE x; --`
 * arriving as a *value* is compared and never parsed — it produces no event
 * here, and needs none. What this reports is the other vector: user input that
 * reached a place where a value cannot go — a column name, a sort direction, a
 * SELECT expression — where the only defence is refusing the statement.
 *
 * `kind` is the part a host acts on, and the two are not equally strong:
 *
 *   - `injection-pattern` — the expression carries `;`, `--`, a nested
 *     `SELECT`, `UNION`, or a quote inside an identifier. Nobody writes that by
 *     hand. Treat it as an attack.
 *   - `invalid-shape` — an unknown function, a column name with a space, two
 *     columns in one string. An attack looks like this too, but so does a
 *     legacy schema or an application-defined function, so it is a count to
 *     accumulate rather than a verdict.
 *
 * A static string passed to `select()` fails the same way every time, so it is
 * caught the first time the code runs. For a refusal to appear in production,
 * on code a test suite has exercised, the string must have CHANGED — and what
 * changes between two runs of the same code is what came in with the request.
 * That is why the environment is the right place to decide what to do: a
 * notification while developing, a block in production.
 *
 * atlas reports; it does not decide. It has no idea who made the request.
 */
export interface UnsafeStatementEvent {
	/** How strong the signal is — see above. */
	kind: "injection-pattern" | "invalid-shape";
	/** The framework code the refusal carries, e.g. `E_INJECTION_PATTERN`. */
	code: string;
	/** The refusal, verbatim, including the offending fragment. */
	message: string;
	/** Connection name, when the app named it. */
	connection?: string;
}

export type UnsafeStatementListener = (event: UnsafeStatementEvent) => void;

const unsafeListeners = new Set<UnsafeStatementListener>();

/**
 * Subscribe to every statement the compiler refused. Returns an unsubscribe
 * function.
 *
 * Throws from a listener are swallowed, as with `onDbQuery`: reporting must
 * never change what the refusal does, which is to throw at the caller.
 */
export function onUnsafeStatement(
	listener: UnsafeStatementListener,
): () => void {
	unsafeListeners.add(listener);
	return () => {
		unsafeListeners.delete(listener);
	};
}

/** Remove every listener. Intended for test teardown. */
export function clearUnsafeStatementListeners(): void {
	unsafeListeners.clear();
}

/** Whether anyone is listening. */
export function hasUnsafeStatementListeners(): boolean {
	return unsafeListeners.size > 0;
}

/** The codes the compiler raises for a refusal, and how strong each one is. */
const REFUSAL_KINDS: ReadonlyArray<[string, UnsafeStatementEvent["kind"]]> = [
	["E_INJECTION_PATTERN", "injection-pattern"],
	// A quote or a NUL inside an identifier is an escape attempt, not a typo:
	// the compiler names that case separately from a merely invalid one.
	[
		"E_UNSAFE_IDENTIFIER: identifier contains an illegal character",
		"injection-pattern",
	],
	["E_UNSAFE_IDENTIFIER", "invalid-shape"],
	["E_UNSAFE_EXPRESSION", "invalid-shape"],
];

/**
 * Classify a compiler refusal, or return `undefined` when the error is not one.
 *
 * Matched on the code the message carries rather than on its prose, so
 * rewording a message cannot silently turn an attack into a non-event.
 */
export function classifyRefusal(
	error: unknown,
): Pick<UnsafeStatementEvent, "kind" | "code"> | undefined {
	const message = error instanceof Error ? error.message : String(error);
	for (const [code, kind] of REFUSAL_KINDS) {
		if (message.includes(code)) {
			return { kind, code: code.split(":")[0] ?? code };
		}
	}
	return undefined;
}

/** @internal Report a refused statement to every listener. */
export function emitUnsafeStatement(event: UnsafeStatementEvent): void {
	for (const listener of unsafeListeners) {
		try {
			listener(event);
		} catch {
			/* a reporter must never change what the refusal does */
		}
	}
}
