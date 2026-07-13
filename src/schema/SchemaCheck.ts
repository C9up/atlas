/**
 * Schema verification — reconciles each model's `@Column` metadata against the
 * LIVE database schema (via {@link introspectTable}) and reports drift BEFORE
 * it bites at runtime. This is what pure-JS ORMs (Lucid) structurally cannot
 * do; atlas can because its driver introspects the real database.
 *
 * Four drift categories:
 *   - `missing-table`    — the model's table does not exist.
 *   - `missing-in-db`    — a model column maps to a non-existent DB column
 *                          (typo → `did you mean`).
 *   - `type-mismatch`    — a declared `@Column({ type })` clashes with the DB
 *                          column's type (conservative: only clear num↔text).
 *   - `missing-in-model` — a NOT NULL DB column with no default that no model
 *                          property maps to → inserts will fail.
 *
 * The check is dialect-agnostic; only {@link introspectTable} is dialect-aware.
 * Kept free of any `@c9up/ream` import — atlas stays framework-agnostic.
 */

import { getColumnMetadata, getEntityMetadata } from "../decorators/entity.js";
import { getNamingStrategy } from "../naming/NamingStrategy.js";
import type { AtlasDialect } from "../query/native.js";
import {
	type IntrospectedColumn,
	introspectTable,
	type SchemaIntrospectable,
} from "./introspect.js";

type Constructor = new (...args: unknown[]) => unknown;

export type SchemaFindingKind =
	| "missing-table"
	| "missing-in-db"
	| "type-mismatch"
	| "missing-in-model";

export interface SchemaFinding {
	entity: string;
	table: string;
	kind: SchemaFindingKind;
	column: string;
	detail: string;
	/** A close DB column name, for typo diagnostics (`did you mean`). */
	suggestion?: string;
}

// ─── Type compatibility (conservative) ───────────────────────────────

/**
 * Coarse group of a type string. We only ever flag a mismatch between the two
 * UNAMBIGUOUS groups (`num` vs `text`); everything storage-dependent
 * (date/time, binary, blob — SQLite stores these as TEXT/NUMERIC) is `other`
 * and never flagged, so the check has NO false positives.
 */
function typeGroup(raw: string): "num" | "text" | "other" {
	const t = raw.toLowerCase();
	if (/(^|[^a-z])(int|serial|decimal|numeric|real|float|double|bool)/.test(t)) {
		return "num";
	}
	if (/char|text|clob|string|uuid|json/.test(t)) return "text";
	return "other";
}

/** Compatible unless the model and DB types are in clearly-different groups. */
export function typesCompatible(modelType: string, dbType: string): boolean {
	const m = typeGroup(modelType);
	const d = typeGroup(dbType);
	if (m === "other" || d === "other") return true;
	return m === d;
}

// ─── `did you mean` (atlas-local; no @c9up/ream import) ───────────────

function levenshtein(a: string, b: string): number {
	const dp = Array.from({ length: b.length + 1 }, (_, i) => i);
	for (let i = 1; i <= a.length; i++) {
		let prev = dp[0];
		dp[0] = i;
		for (let j = 1; j <= b.length; j++) {
			const tmp = dp[j];
			dp[j] = Math.min(
				dp[j] + 1,
				dp[j - 1] + 1,
				prev + (a[i - 1] === b[j - 1] ? 0 : 1),
			);
			prev = tmp;
		}
	}
	return dp[b.length];
}

/** Closest candidate within edit distance 2 (typo suggestion), else undefined. */
export function suggestColumn(
	name: string,
	candidates: string[],
): string | undefined {
	let best: string | undefined;
	let bestD = Number.POSITIVE_INFINITY;
	for (const c of candidates) {
		const d = levenshtein(name, c);
		if (d < bestD) {
			bestD = d;
			best = c;
		}
	}
	return bestD <= 2 ? best : undefined;
}

// ─── Reconciler ──────────────────────────────────────────────────────

function reconcile(
	entityName: string,
	table: string,
	cols: ReturnType<typeof getColumnMetadata>,
	columnNameOf: (property: string) => string,
	dbCols: IntrospectedColumn[],
): SchemaFinding[] {
	const findings: SchemaFinding[] = [];
	const dbByName = new Map(dbCols.map((c) => [c.name, c]));
	const dbNames = dbCols.map((c) => c.name);
	const mappedDbColumns = new Set<string>();

	for (const col of cols) {
		const dbName = columnNameOf(col.propertyKey);
		mappedDbColumns.add(dbName);
		const dbCol = dbByName.get(dbName);
		if (!dbCol) {
			findings.push({
				entity: entityName,
				table,
				kind: "missing-in-db",
				column: dbName,
				detail: `model property \`${col.propertyKey}\` maps to column \`${dbName}\`, which does not exist`,
				suggestion: suggestColumn(dbName, dbNames),
			});
			continue;
		}
		if (col.type && !typesCompatible(col.type, dbCol.type)) {
			findings.push({
				entity: entityName,
				table,
				kind: "type-mismatch",
				column: dbName,
				detail: `declared \`${col.type}\` but column is \`${dbCol.type}\``,
			});
		}
	}

	// Reverse drift — a NOT NULL column with no default that no model property
	// writes: every insert omitting it fails. The dangerous, easy-to-miss case
	// (e.g. a migration added a column the model never caught up to). PKs are
	// excluded (DB-generated).
	for (const dbCol of dbCols) {
		if (dbCol.primaryKey) continue;
		if (
			!dbCol.nullable &&
			!dbCol.hasDefault &&
			!mappedDbColumns.has(dbCol.name)
		) {
			findings.push({
				entity: entityName,
				table,
				kind: "missing-in-model",
				column: dbCol.name,
				detail: `column \`${dbCol.name}\` is NOT NULL with no default but no model property maps to it — inserts will fail`,
			});
		}
	}
	return findings;
}

// ─── Public API ──────────────────────────────────────────────────────

/**
 * Reconcile every given entity against the live database. Pass the app's model
 * classes (atlas has no global entity registry by design — mirror Lucid, where
 * `ace` is pointed at your models). Returns a flat list of findings (empty when
 * the schema and models agree).
 */
export async function checkSchema(
	entities: readonly Constructor[],
	db: SchemaIntrospectable,
	dialect: AtlasDialect,
): Promise<SchemaFinding[]> {
	const findings: SchemaFinding[] = [];
	for (const entity of entities) {
		const meta = getEntityMetadata(entity);
		if (!meta?.tableName) continue; // not an @Entity — skip silently
		const table = meta.tableName;
		const entityName = entity.name;

		const dbCols = await introspectTable(db, dialect, table);
		if (dbCols === null) {
			findings.push({
				entity: entityName,
				table,
				kind: "missing-table",
				column: table,
				detail: `table \`${table}\` does not exist in the database — run your migrations`,
			});
			continue;
		}

		const strategy = getNamingStrategy(entity);
		findings.push(
			...reconcile(
				entityName,
				table,
				getColumnMetadata(entity),
				(p) => strategy.columnName(p),
				dbCols,
			),
		);
	}
	return findings;
}

/** Render findings as a didactic, Adonis-style diff (grouped per table). */
export function formatSchemaFindings(findings: SchemaFinding[]): string {
	if (findings.length === 0)
		return "[atlas:check] schema OK — models match the database.";
	const byTable = new Map<string, SchemaFinding[]>();
	for (const f of findings) {
		const key = `${f.table} (${f.entity})`;
		const list = byTable.get(key) ?? [];
		list.push(f);
		byTable.set(key, list);
	}
	const lines: string[] = [
		`[atlas:check] ${findings.length} schema issue(s) found:`,
	];
	for (const [key, list] of byTable) {
		lines.push(`\n  ${key}`);
		for (const f of list) {
			const hint = f.suggestion ? ` — did you mean \`${f.suggestion}\`?` : "";
			lines.push(`    ✗ ${f.column}: ${f.detail}${hint}`);
		}
	}
	return lines.join("\n");
}

/**
 * Boot-time guard: run {@link checkSchema} and either throw (fail-fast, for CI /
 * dev startup) or warn (non-blocking). Returns the findings. `mode` defaults to
 * `"throw"` — a schema mismatch is a misconfiguration that should stop the boot
 * before requests serve stale assumptions.
 */
export async function verifySchema(
	entities: readonly Constructor[],
	db: SchemaIntrospectable,
	dialect: AtlasDialect,
	opts: { mode?: "throw" | "warn" } = {},
): Promise<SchemaFinding[]> {
	const findings = await checkSchema(entities, db, dialect);
	if (findings.length > 0) {
		const report = formatSchemaFindings(findings);
		if ((opts.mode ?? "throw") === "throw") {
			throw new Error(report);
		}
		console.warn(report);
	}
	return findings;
}

/**
 * CLI body: run the check, print the report (diff or the OK line), and return
 * the process exit code (`0` = schema matches, `1` = drift). Used by the
 * `atlas:check` console command; safe to call from any script.
 */
export async function runSchemaCheck(
	entities: readonly Constructor[],
	db: SchemaIntrospectable,
	dialect: AtlasDialect,
): Promise<number> {
	const findings = await checkSchema(entities, db, dialect);
	console.log(formatSchemaFindings(findings));
	return findings.length > 0 ? 1 : 0;
}
