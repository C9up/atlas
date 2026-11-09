import { AtlasError } from "../errors.js";
import type { QueryExecutor } from "./DatabaseQueryBuilder.js";
import {
	type CompiledStatement,
	compiledStatement,
	interpolateQuery,
} from "./interpolate.js";
import { type AtlasDialect, compileStatementNative } from "./native.js";

/**
 * Lucid/Knex named bindings → the positional notation the compiler reads.
 *
 * `:name` becomes `?` and `:name:` becomes `??` (a quoted identifier), with the
 * values in occurrence order; `\:` escapes a colon. A `:name` that is not an
 * own key of the bindings is left as written, as Knex does: a time literal
 * `'10:30:00'` is text, not a binding, and a typo'd key reaches the server as
 * the syntax error it is instead of binding NULL.
 */
export function namedToPositional(
	sql: string,
	bindings: Record<string, unknown>,
): { sql: string; bindings: unknown[] } {
	const values: unknown[] = [];
	const out = sql.replace(
		/\\?(:(\w+):(?=::)|:(\w+):(?!:)|:(\w+))/g,
		(match: string, whole: string, a?: string, b?: string, c?: string) => {
			if (match !== whole) return whole;
			const name = a ?? b ?? c ?? "";
			if (!Object.hasOwn(bindings, name)) return match;
			values.push(bindings[name]);
			return whole.endsWith(":") ? "??" : "?";
		},
	);
	return { sql: out, bindings: assertDefinedBindings(out, values) };
}

/**
 * Refuse an `undefined` binding (Knex: "Undefined binding(s) detected"). JSON
 * carries it as `null`, so the statement silently compared or wrote NULL.
 */
export function assertDefinedBindings(
	sql: string,
	bindings: unknown[],
): unknown[] {
	const missing = bindings.flatMap((v, i) => (v === undefined ? [i] : []));
	if (missing.length > 0) {
		throw new AtlasError(
			"E_ATLAS_RAW_BINDINGS",
			`Undefined binding(s) detected at index ${missing.join(", ")} for raw SQL: ${sql}`,
		);
	}
	return bindings;
}

/** Bindings in positional form: named ones are rewritten into the SQL first. */
export function positionalRaw(
	sql: string,
	bindings: unknown[] | Record<string, unknown>,
): { sql: string; bindings: unknown[] } {
	return Array.isArray(bindings)
		? { sql, bindings: assertDefinedBindings(sql, bindings) }
		: namedToPositional(sql, bindings);
}

/**
 * Chainable raw query — Lucid `db.rawQuery(sql, bindings)`. Unlike a raw
 * fragment (`db.raw`), this executes independently and exposes the standard
 * query surface: `toSQL()`, `toQuery()`, `debug()`, `timeout()`,
 * `reporterData()`. Thenable, so it can be awaited directly.
 */
export class RawQueryBuilder<T = Record<string, unknown>>
	implements PromiseLike<T[]>
{
	readonly #exec: QueryExecutor;
	readonly #dialect: AtlasDialect;
	readonly #rawSql: string;
	readonly #rawBindings: unknown[] | Record<string, unknown>;
	#timeoutMs?: number;
	#debugFlag = false;
	#reporter?: Record<string, unknown>;

	constructor(
		exec: QueryExecutor,
		dialect: AtlasDialect,
		sql: string,
		bindings: unknown[] | Record<string, unknown> = [],
	) {
		this.#exec = exec;
		this.#dialect = dialect;
		this.#rawSql = sql;
		this.#rawBindings = bindings;
	}

	/**
	 * The compiled statement, shaped like every other builder's: `.sql` carries
	 * `?` on every dialect (Lucid's normalisation) and `.toNative()` yields the
	 * `$N` form Postgres is actually sent.
	 */
	toSQL(): CompiledStatement {
		const { sql, params } = this.#native();
		return compiledStatement(sql, params);
	}

	/** The SQL with bindings substituted for display/debug (Lucid `toQuery`). */
	toQuery(): string {
		const { sql, params } = this.#native();
		return interpolateQuery(sql, params);
	}

	/**
	 * The statement as the driver receives it — native placeholders, ordered
	 * params. Compiled by the same grammar as every raw fragment: `?`, `??`,
	 * `\?` or `$N`, with the binding count checked against the placeholders.
	 */
	#native(): { sql: string; params: unknown[] } {
		const { sql, bindings } = positionalRaw(this.#rawSql, this.#rawBindings);
		const compiled = compileStatementNative(
			{ kind: "raw", sql, bindings },
			this.#dialect,
		);
		return { sql: compiled.statements.join(";"), params: compiled.params };
	}

	/** Enable debug logging of the compiled query on execution (Lucid `debug`). */
	debug(value = true): this {
		this.#debugFlag = value;
		return this;
	}

	/**
	 * Caller-facing statement timeout (Lucid `timeout(ms, options)`). Matches
	 * Lucid's DEFAULT non-cancelling timeout — the awaiter rejects after `ms`.
	 * `{ cancel: true }` server-side cancellation is not wired at this layer.
	 */
	timeout(ms?: number, _options?: { cancel?: boolean }): this {
		this.#timeoutMs = ms;
		return this;
	}

	/** Attach metadata to the query (Lucid `reporterData`). */
	reporterData(data: Record<string, unknown>): this {
		this.#reporter = data;
		return this;
	}

	/** Execute and return the rows. */
	async exec(): Promise<T[]> {
		// The NATIVE statement, not `toSQL().sql`: that one is `?`-normalised for
		// display, and sqlx hands what it is given straight to the server.
		const { sql, params } = this.#native();
		// Carry `debug`/`reporterData` in the query meta so the `db:query` observer
		// fires exactly like Lucid — not a bare console.debug.
		const meta: { debug?: boolean; reporterData?: Record<string, unknown> } =
			{};
		if (this.#debugFlag) meta.debug = true;
		if (this.#reporter) meta.reporterData = this.#reporter;
		const work = this.#exec.query<T>(
			sql,
			params,
			Object.keys(meta).length > 0 ? meta : undefined,
		);
		return this.#race(work);
	}

	// biome-ignore lint/suspicious/noThenProperty: Lucid raw queries are awaitable by design — `await db.rawQuery(...)` must resolve to the rows; the thenable is the intended public API, matching the other builders.
	then<R1 = T[], R2 = never>(
		onfulfilled?: ((value: T[]) => R1 | PromiseLike<R1>) | undefined | null,
		onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | undefined | null,
	): PromiseLike<R1 | R2> {
		return this.exec().then(onfulfilled, onrejected);
	}

	/** Race the query against the configured `.timeout(ms)`. */
	#race(work: Promise<T[]>): Promise<T[]> {
		const ms = this.#timeoutMs;
		if (!ms || ms <= 0) return work;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const guard = new Promise<never>((_, reject) => {
			timer = setTimeout(
				() => reject(new Error(`Query timed out after ${ms}ms`)),
				ms,
			);
		});
		work.catch(() => {});
		return Promise.race([work, guard]).finally(() => clearTimeout(timer));
	}
}
