/** SQL operator helpers shared by the query builders. */

const NEGATION: Record<string, string> = {
	"=": "!=",
	"!=": "=",
	"<>": "=",
	"<": ">=",
	">": "<=",
	"<=": ">",
	">=": "<",
	IN: "NOT IN",
	"NOT IN": "IN",
	LIKE: "NOT LIKE",
	"NOT LIKE": "LIKE",
	ILIKE: "NOT ILIKE",
	BETWEEN: "NOT BETWEEN",
	"NOT BETWEEN": "BETWEEN",
	"IS NULL": "IS NOT NULL",
	"IS NOT NULL": "IS NULL",
};

/** The logical negation of a comparison operator (`>=` → `<`, `=` → `!=`, …). */
export function negateOperator(op: string): string {
	const negated = NEGATION[op.toUpperCase()] ?? NEGATION[op];
	if (!negated) throw new Error(`whereNot: unsupported operator '${op}'`);
	return negated;
}

/** The `[operator,] value` tail of a comparison call, split by arity. */
export interface ComparisonTail {
	/** Whether the caller named the operator (the 3-argument form). */
	explicit: boolean;
	operator: string;
	value: unknown;
}

/**
 * Split the `(value)` / `(operator, value)` tail of a comparison by how many
 * arguments the caller passed, as Knex does. Testing the value for
 * `undefined` instead read `where('id', '=', undefined)` as `where('id', '=')`
 * and compared the column with the string `'='`; by arity, that call keeps
 * its `undefined` value and the compiler refuses it.
 */
export function comparisonTail(rest: readonly unknown[]): ComparisonTail {
	if (rest.length >= 2) {
		return { explicit: true, operator: String(rest[0]), value: rest[1] };
	}
	return { explicit: false, operator: "=", value: rest[0] };
}
