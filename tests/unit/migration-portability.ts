/**
 * Portability assertion for framework-shipped migration templates.
 *
 * MySQL InnoDB enforces a 3072-byte index limit on utf8mb4 tables using the
 * default DYNAMIC row format. A `VARCHAR(N)` column declared as part of a
 * `PRIMARY KEY` or `UNIQUE` constraint must satisfy `N * 4 ≤ 3072`, else
 * `CREATE TABLE` succeeds but the index is silently truncated to a prefix or
 * the column refuses to be indexed at all.
 *
 * `assertInnodbPkBudget(ddl)` parses a CREATE TABLE statement, extracts every
 * VARCHAR-typed PRIMARY KEY / UNIQUE column, and throws if any exceeds the
 * budget. The parser is regex-driven — it targets framework-shipped DDL (small,
 * predictable, hand-readable by the audit), NOT arbitrary user SQL.
 *
 * Lives under `tests/` (not `src/`) because the helper is audit-only — it
 * never runs at app runtime. Promote to `src/` if a non-test caller ever
 * needs it.
 */

const MAX_INNODB_UTF8MB4_INDEX_BYTES = 3072;
const UTF8MB4_BYTES_PER_CHAR = 4;

/** A single offending column extracted from the DDL. */
export interface InnodbBudgetViolation {
	column: string;
	length: number;
	bytes: number;
	reason: "PRIMARY KEY" | "UNIQUE";
}

/** Detail-bearing error so callers can pinpoint the failing column. */
export class InnodbBudgetError extends Error {
	readonly violations: readonly InnodbBudgetViolation[];
	constructor(violations: readonly InnodbBudgetViolation[]) {
		const summary = violations
			.map(
				(v) =>
					`  - column \`${v.column}\` (${v.reason}): ${v.length} chars × 4 bytes/char = ${v.bytes} bytes (limit: ${MAX_INNODB_UTF8MB4_INDEX_BYTES})`,
			)
			.join("\n");
		super(
			`InnoDB utf8mb4 index budget exceeded for ${violations.length} column(s):\n${summary}`,
		);
		this.name = "InnodbBudgetError";
		this.violations = violations;
	}
}

function bytesForChars(length: number): number {
	return length * UTF8MB4_BYTES_PER_CHAR;
}

/**
 * Scan a DDL string for `VARCHAR(N)` PK or UNIQUE columns that exceed the
 * 3072-byte InnoDB index limit on utf8mb4. Throws {@link InnodbBudgetError}
 * with the full violation list when at least one column is over budget.
 *
 * Supports the two MySQL DDL forms Atlas can emit:
 *
 *   1. `\`endpoint\` VARCHAR(2048) PRIMARY KEY` — inline PK on the column line.
 *   2. `PRIMARY KEY (\`endpoint\`)` / `UNIQUE (\`endpoint\`)` — separate constraint clause.
 *
 * Both single-column and **composite** PK/UNIQUE constraints are checked:
 * for a composite clause `PRIMARY KEY (a, b)`, the helper sums the byte
 * budget across every VARCHAR-typed member column and flags the WHOLE group
 * when the sum exceeds 3072 bytes. The violation's `column` field shows the
 * comma-joined member list and `length` is the total char count.
 *
 * Backtick OR double-quote identifier quoting is accepted (`compileStatementNative`
 * picks one per dialect; the helper does not assume a single dialect).
 */
export function assertInnodbPkBudget(ddl: string): void {
	const violations: InnodbBudgetViolation[] = [];

	const columnLengths = collectVarcharLengths(ddl);

	for (const [column, length] of columnLengths) {
		const reason = classifyColumn(ddl, column);
		if (reason === null) continue;
		const bytes = bytesForChars(length);
		if (bytes > MAX_INNODB_UTF8MB4_INDEX_BYTES) {
			violations.push({ column, length, bytes, reason });
		}
	}

	for (const group of collectCompositeIndexGroups(ddl)) {
		const varcharMembers = group.columns.filter((c) => columnLengths.has(c));
		if (varcharMembers.length < 2) continue;
		const totalChars = varcharMembers.reduce(
			(acc, col) => acc + (columnLengths.get(col) ?? 0),
			0,
		);
		const totalBytes = bytesForChars(totalChars);
		if (totalBytes > MAX_INNODB_UTF8MB4_INDEX_BYTES) {
			violations.push({
				column: varcharMembers.join(", "),
				length: totalChars,
				bytes: totalBytes,
				reason: group.kind,
			});
		}
	}

	if (violations.length > 0) {
		throw new InnodbBudgetError(violations);
	}
}

/**
 * Walk every `PRIMARY KEY (...)` / `UNIQUE (...)` / `UNIQUE KEY name (...)`
 * constraint clause in the DDL and capture its member columns in declaration
 * order. Single-member groups are returned too — the caller filters them out
 * for composite handling.
 */
function collectCompositeIndexGroups(
	ddl: string,
): Array<{ kind: "PRIMARY KEY" | "UNIQUE"; columns: string[] }> {
	const groups: Array<{ kind: "PRIMARY KEY" | "UNIQUE"; columns: string[] }> =
		[];
	const re =
		/(PRIMARY\s+KEY|UNIQUE(?:\s+KEY\s+["`][^"`]+["`])?)\s*\(\s*([^)]+?)\s*\)/gi;
	let match: RegExpExecArray | null = re.exec(ddl);
	while (match !== null) {
		const kindStr = match[1];
		const colsRaw = match[2];
		if (typeof kindStr !== "string" || typeof colsRaw !== "string") {
			match = re.exec(ddl);
			continue;
		}
		const kind: "PRIMARY KEY" | "UNIQUE" = /UNIQUE/i.test(kindStr)
			? "UNIQUE"
			: "PRIMARY KEY";
		const columns = colsRaw
			.split(",")
			.map((c) => c.trim().replace(/^["`]|["`]$/g, ""))
			.filter((c) => c.length > 0);
		if (columns.length > 0) {
			groups.push({ kind, columns });
		}
		match = re.exec(ddl);
	}
	return groups;
}

/**
 * For every `VARCHAR(N)` column declared anywhere in the DDL, capture its name
 * and length. The map preserves insertion order so callers get stable error
 * output across runs.
 */
function collectVarcharLengths(ddl: string): Map<string, number> {
	const out = new Map<string, number>();
	// Column lines have the form: `"name" VARCHAR(1024) ...` or `\`name\` VARCHAR(1024) ...`
	const re = /["`]([A-Za-z_][A-Za-z0-9_]*)["`]\s+VARCHAR\((\d+)\)/g;
	let match: RegExpExecArray | null = re.exec(ddl);
	while (match !== null) {
		const column = match[1];
		const lengthStr = match[2];
		if (typeof column === "string" && typeof lengthStr === "string") {
			out.set(column, Number.parseInt(lengthStr, 10));
		}
		match = re.exec(ddl);
	}
	return out;
}

/**
 * Decide whether `column` participates in a PRIMARY KEY or UNIQUE index in
 * the given DDL. Returns null if the column is neither, in which case the
 * byte budget does not apply (regular non-indexed VARCHAR can be any length).
 *
 * Two forms checked, in the order Atlas emits them:
 *   1. Inline PRIMARY KEY / UNIQUE on the column line itself.
 *   2. Trailing PRIMARY KEY (col) / UNIQUE (col) / UNIQUE KEY name (col) constraint clause.
 */
function classifyColumn(
	ddl: string,
	column: string,
): "PRIMARY KEY" | "UNIQUE" | null {
	const inlinePk = new RegExp(
		`["\`]${escapeRegex(column)}["\`]\\s+VARCHAR\\(\\d+\\)[^,\\n]*PRIMARY\\s+KEY`,
		"i",
	);
	if (inlinePk.test(ddl)) return "PRIMARY KEY";

	const constraintPk = new RegExp(
		`PRIMARY\\s+KEY\\s*\\([^)]*["\`]${escapeRegex(column)}["\`][^)]*\\)`,
		"i",
	);
	if (constraintPk.test(ddl)) return "PRIMARY KEY";

	const inlineUnique = new RegExp(
		`["\`]${escapeRegex(column)}["\`]\\s+VARCHAR\\(\\d+\\)[^,\\n]*\\bUNIQUE\\b`,
		"i",
	);
	if (inlineUnique.test(ddl)) return "UNIQUE";

	const constraintUnique = new RegExp(
		`UNIQUE(?:\\s+KEY\\s+["\`][^"\`]+["\`])?\\s*\\([^)]*["\`]${escapeRegex(column)}["\`][^)]*\\)`,
		"i",
	);
	if (constraintUnique.test(ddl)) return "UNIQUE";

	return null;
}

function escapeRegex(literal: string): string {
	return literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
