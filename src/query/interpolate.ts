/**
 * Query-inspection helpers shared by every builder's `toQuery()` / `toSQL()`.
 *
 * `toQuery()` returns the SQL with bindings substituted as literals (Adonis
 * Lucid semantics — `where "is_active" = 1`), for ad-hoc inspection ONLY. It is
 * NOT injection-safe; forward `toSQL()` + bindings to logs instead.
 */

/** SQL-literal render of a single binding (inspection only — not injection-safe). */
export function literalEscape(v: unknown): string {
	if (v === null || v === undefined) return "NULL";
	if (typeof v === "number") return String(v);
	if (typeof v === "boolean") return v ? "1" : "0";
	if (v instanceof Date) return `'${v.toISOString()}'`;
	// Strings — escape single quotes per SQL. NOT hardened against `\'`.
	return `'${String(v).replace(/'/g, "''")}'`;
}

/**
 * Substitute native placeholders (`?` sequentially, `$N` by 1-based index) with
 * dialect-safe literals — Lucid `toQuery`. `$N` is resolved by its own index so
 * a reused/reordered Postgres placeholder still maps to the right binding.
 */
export function interpolateQuery(
	sql: string,
	params: readonly unknown[],
): string {
	let seq = 0;
	return sql.replace(/\?|\$\d+/g, (tok) =>
		literalEscape(params[tok[0] === "$" ? Number(tok.slice(1)) - 1 : seq++]),
	);
}

/**
 * Normalize dialect-native positional placeholders (`$1`, `$2`, … — Postgres) to
 * Knex-style `?`, matching what Lucid's `toSQL().sql` returns for every dialect.
 * MySQL/SQLite already emit `?`, so this is a no-op there. Display concern only:
 * atlas executes with the native statement (see `toNative`).
 */
export function toQuestionMarks(sql: string): string {
	return sql.replace(/\$\d+/g, "?");
}

/**
 * The compiled statement Lucid's `toSQL()` returns: `{ sql, bindings }` with `?`
 * placeholders (Knex-normalized, same on every dialect), plus a `toNative()` that
 * yields the dialect-native `{ sql, bindings }` (Postgres `$N`) that atlas
 * actually executes. `params` is atlas's historical alias of `bindings` (same
 * array), kept so either name ports.
 */
export interface CompiledStatement {
	sql: string;
	bindings: unknown[];
	params: unknown[];
	/** Dialect-native `{ sql, bindings }` (Lucid `toSQL().toNative()`). */
	toNative(): { sql: string; bindings: unknown[] };
}

/**
 * Wrap a NATIVE compiled `sql` + `params` as a Lucid-shaped {@link CompiledStatement}:
 * the public `.sql` is `?`-normalized, `.toNative()` yields the native form.
 */
export function compiledStatement(
	nativeSql: string,
	params: unknown[],
): CompiledStatement {
	return {
		sql: toQuestionMarks(nativeSql),
		bindings: params,
		params,
		toNative: () => ({ sql: nativeSql, bindings: params }),
	};
}
