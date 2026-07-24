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
 * The compiled statement Lucid's `toSQL()` returns: `{ sql, bindings }` plus a
 * `toNative()` that yields the dialect-native `{ sql, bindings }`. Atlas already
 * compiles to native placeholders, so `toNative()` returns the same statement —
 * the method exists for API parity and to guarantee the native shape. `params`
 * is atlas's historical alias of `bindings` (same array), kept so either name
 * ports.
 */
export interface CompiledStatement {
	sql: string;
	bindings: unknown[];
	params: unknown[];
	/** Dialect-native `{ sql, bindings }` (Lucid `toSQL().toNative()`). */
	toNative(): { sql: string; bindings: unknown[] };
}

/** Wrap a compiled `sql` + `params` as a Lucid-shaped {@link CompiledStatement}. */
export function compiledStatement(
	sql: string,
	params: unknown[],
): CompiledStatement {
	return {
		sql,
		bindings: params,
		params,
		toNative: () => ({ sql, bindings: params }),
	};
}
