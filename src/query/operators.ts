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
