/**
 * SQL identifier validation helpers — used by the TS-side fluent builders
 * before a name is sent to the Rust compiler. Catches obvious garbage
 * (empty / wrong chars / leading digit) early with a clear error site.
 *
 * The Rust compiler ALSO validates and quotes identifiers — these helpers
 * are a defence-in-depth check, not the source of truth.
 */

/** Pattern: starts with letter or underscore, then letters/digits/underscore/dot. */
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_.]*$/;

/** Returns true if `name` is a syntactically valid SQL identifier (table, column, alias, CTE name, …). */
export function isValidIdentifier(name: unknown): name is string {
	return (
		typeof name === "string" && name.length > 0 && IDENTIFIER_RE.test(name)
	);
}

/**
 * Asserts that `name` is a valid identifier; throws a structured error otherwise.
 *
 * @param name  the value to validate
 * @param kind  human-readable label used in the error message (e.g. "CTE name", "column name")
 * @param errorFactory  builds the error to throw — keeps this helper independent of any specific error class
 */
export function assertValidIdentifier(
	name: unknown,
	kind: string,
	errorFactory: (message: string) => Error,
): asserts name is string {
	if (!isValidIdentifier(name)) {
		throw errorFactory(`${kind} must be a valid identifier: '${String(name)}'`);
	}
}
