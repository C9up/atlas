import type { QueryExecutor } from "./DatabaseQueryBuilder.js";
import type { AtlasDialect } from "./native.js";

/** Dialect-quote a possibly dotted identifier (`users.id` → `"users"."id"`). */
function quoteIdent(ident: string, dialect: AtlasDialect): string {
	if (!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(ident)) {
		throw new Error(`Invalid identifier binding '${ident}'.`);
	}
	const q = dialect === "mysql" ? "`" : '"';
	return ident
		.split(".")
		.map((s) => `${q}${s}${q}`)
		.join(".");
}

/**
 * Resolve Lucid/Knex raw bindings into positional `?` SQL + ordered params.
 *
 * Positional (array bindings): `?` binds a value, `??` inlines a quoted
 * identifier. Named (object bindings): `:name` binds a value, `:name:` inlines a
 * quoted identifier. Identifiers are quoted for the dialect; values become `?`
 * placeholders bound in occurrence order.
 */
export function resolveRawBindings(
	sql: string,
	bindings: unknown[] | Record<string, unknown>,
	dialect: AtlasDialect,
): { sql: string; params: unknown[] } {
	const params: unknown[] = [];
	if (Array.isArray(bindings)) {
		let i = 0;
		const out = sql.replace(/\?\?|\?/g, (m) => {
			const v = bindings[i++];
			if (m === "??") return quoteIdent(String(v), dialect);
			params.push(v);
			return "?";
		});
		return { sql: out, params };
	}
	// `::` (Postgres cast) is matched first and passed through untouched, so
	// `:payload::jsonb` reads as the value binding `:payload` + the cast `::jsonb`
	// rather than an `:payload:` identifier. `:name:` only matches when its
	// trailing colon is NOT part of a `::` cast (`(?!:)`).
	const out = sql.replace(
		/::|:(\w+):(?!:)|:(\w+)/g,
		(m: string, ident?: string, name?: string) => {
			if (m === "::") return "::";
			if (ident !== undefined) {
				return quoteIdent(String(bindings[ident]), dialect);
			}
			if (name !== undefined) {
				params.push(bindings[name]);
				return "?";
			}
			return m;
		},
	);
	return { sql: out, params };
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

	/** `{ sql, bindings }` with named/identifier bindings resolved (Lucid `toSQL`). */
	toSQL(): { sql: string; bindings: unknown[] } {
		const { sql, params } = resolveRawBindings(
			this.#rawSql,
			this.#rawBindings,
			this.#dialect,
		);
		return { sql, bindings: params };
	}

	/** The SQL with bindings substituted for display/debug (Lucid `toQuery`). */
	toQuery(): string {
		const { sql, bindings } = this.toSQL();
		let i = 0;
		return sql.replace(/\?/g, () => {
			const v = bindings[i++];
			if (v === null || v === undefined) return "NULL";
			if (typeof v === "number" || typeof v === "boolean") return String(v);
			return `'${String(v).replace(/'/g, "''")}'`;
		});
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
		const { sql, bindings } = this.toSQL();
		if (this.#debugFlag) {
			console.debug("[atlas:rawQuery]", this.toQuery());
		}
		const work = this.#exec.query<T>(
			sql,
			bindings,
			this.#reporter ? { reporterData: this.#reporter } : undefined,
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
