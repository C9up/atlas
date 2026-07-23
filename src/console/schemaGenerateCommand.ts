/**
 * `schema:generate` — database-first codegen (Adonis Lucid parity). Introspects
 * the live database and writes a `schema.ts` file with one `BaseModel` subclass
 * per table, mirroring Lucid's `schema:generate` → `database/schema.ts`.
 *
 * @example
 *   // commands/schema-generate.ts
 *   import { schemaGenerateCommand } from '@c9up/atlas'
 *   export default schemaGenerateCommand({ outputPath: 'database/schema.ts' })
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import { listUserTables } from "../schema/catalog.js";
import {
	type IntrospectedColumn,
	introspectTable,
} from "../schema/introspect.js";
import { getConnection, getDb } from "../services/db.js";
import type { AtlasCommand } from "./schemaCheckCommand.js";

export interface SchemaGenerateOptions {
	/** File to write the generated schema classes to (Lucid `outputPath`). */
	outputPath: string;
	/** Extra tables to skip (framework tables are always skipped). */
	excludeTables?: string[];
	/**
	 * When `false`, the `schema:generate` command and post-migration regeneration
	 * are both disabled (Adonis Lucid `schemaGeneration.enabled`). Any other value
	 * (incl. `undefined`) leaves generation on.
	 */
	enabled?: boolean;
	/**
	 * Paths to rule modules that customise how columns are emitted (Adonis Lucid
	 * `schemaGeneration.rulesPaths`). Each module default-exports a {@link SchemaRules}
	 * object; multiple files deep-merge, later paths overriding earlier ones.
	 */
	rulesPaths?: string[];
	/**
	 * Emit a denser file — no blank line between a class's `$columns` and its
	 * column declarations (Adonis Lucid `--compact-output`). Also settable per-run
	 * with the `--compact-output` flag.
	 */
	compact?: boolean;
	/**
	 * PostgreSQL only — restrict generation to these schemas (Adonis Lucid
	 * `schemaGeneration.schemas`). Ignored on sqlite/mysql. NOTE: the runtime
	 * behaviour is not yet proven against a live PostgreSQL in atlas's test env.
	 */
	schemas?: string[];
}

/**
 * Per-column override for schema generation (Adonis Lucid schema rules). A rule
 * can force the TypeScript `tsType`, the `decorator`, and the `imports` the
 * generated file needs for that type.
 */
export interface ColumnRule {
	/** TypeScript type for the property (e.g. `"UserStatus"`). */
	tsType?: string;
	/** Full decorator string (e.g. `"@column()"`, `"@column({ isPrimary: true })"`). */
	decorator?: string;
	/** Imports the `tsType`/`decorator` needs, added to the generated file header. */
	imports?: Array<{ source: string; namedImports: string[] }>;
}

/**
 * Schema-generation rules (Adonis Lucid). Resolution is most-specific-first:
 * `tables[table].columns[col]` > `columns[col]` > `types[rawType]`.
 */
export interface SchemaRules {
	/** Rules keyed by column name, applied to that column on every table. */
	columns?: Record<string, ColumnRule>;
	/** Rules keyed by raw dialect type (lower-cased), applied by column type. */
	types?: Record<string, ColumnRule>;
	/** Table-scoped column rules, the highest-priority match. */
	tables?: Record<string, { columns?: Record<string, ColumnRule> }>;
}

/**
 * Load + deep-merge the rule modules named by `rulesPaths` (later files win).
 * Each module default-exports a {@link SchemaRules}; a module without a usable
 * default export is skipped.
 */
export async function loadSchemaRules(
	rulesPaths: string[] | undefined,
): Promise<SchemaRules> {
	const merged: SchemaRules = {};
	for (const p of rulesPaths ?? []) {
		const mod = await import(p);
		const rules: unknown = mod.default ?? mod;
		if (rules && typeof rules === "object") {
			mergeRules(merged, rules as SchemaRules);
		}
	}
	return merged;
}

/** Deep-merge `src` rules into `dst` (column/type/table maps), src winning. */
function mergeRules(dst: SchemaRules, src: SchemaRules): void {
	dst.columns = { ...dst.columns, ...src.columns };
	dst.types = { ...dst.types, ...src.types };
	dst.tables = { ...dst.tables };
	for (const [table, rule] of Object.entries(src.tables ?? {})) {
		dst.tables[table] = {
			columns: { ...dst.tables[table]?.columns, ...rule.columns },
		};
	}
}

/** Resolve the winning rule for a column (table-scoped > column > type), if any. */
function resolveRule(
	rules: SchemaRules,
	table: string,
	col: IntrospectedColumn,
): ColumnRule | undefined {
	return (
		rules.tables?.[table]?.columns?.[col.name] ??
		rules.columns?.[col.name] ??
		rules.types?.[col.type.toLowerCase()]
	);
}

/** `snake_case` / `snake_case_id` → `camelCase`. */
function toCamel(name: string): string {
	return name.replace(/_([a-z0-9])/g, (_, c: string) => c.toUpperCase());
}

/** `snake_case` table → `PascalCase` class stem. */
function toPascal(name: string): string {
	const camel = toCamel(name);
	return camel.charAt(0).toUpperCase() + camel.slice(1);
}

/** `camelCase` → `snake_case`, to detect when a DB column needs `columnName`. */
function toSnake(name: string): string {
	return name.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

/** Is this a date/time column (→ `@column.dateTime` + a Chronos `DateTime`)? */
function isDateType(rawType: string): boolean {
	return /timestamp|datetime|date|time/.test(rawType.toLowerCase());
}

/** Map a NON-date raw dialect column type to a TypeScript type. */
function toTsType(rawType: string): string {
	const t = rawType.toLowerCase();
	if (/bool/.test(t)) return "boolean";
	if (/int|serial|real|float|double|number/.test(t)) return "number";
	// decimal/numeric hydrate as strings (atlas keeps precision) — Lucid parity.
	if (/decimal|numeric/.test(t)) return "string";
	if (/json/.test(t)) return "unknown";
	if (/bytea|blob|binary/.test(t)) return "Uint8Array";
	return "string"; // varchar / text / char / uuid / everything else
}

/** `columnName: "..."` fragment when the DB name doesn't round-trip from camelCase. */
function columnNameOpt(col: IntrospectedColumn, prop: string): string {
	return toSnake(prop) === col.name ? "" : `columnName: "${col.name}"`;
}

/** A source → named-imports accumulator, deduped, for the generated file header. */
type ImportSink = Map<string, Set<string>>;

/** Record a rule's imports into the sink. */
function collectImports(sink: ImportSink, rule: ColumnRule | undefined): void {
	for (const imp of rule?.imports ?? []) {
		const set = sink.get(imp.source) ?? new Set<string>();
		for (const n of imp.namedImports) set.add(n);
		sink.set(imp.source, set);
	}
}

/**
 * Render one column declaration line (Lucid decorator + typed property). A
 * matching {@link SchemaRules} entry overrides the decorator and TS type and
 * contributes its imports to `sink`.
 */
function renderColumn(
	col: IntrospectedColumn,
	table: string,
	rules: SchemaRules,
	sink: ImportSink,
): string {
	const prop = toCamel(col.name);
	const cn = columnNameOpt(col, prop);
	const nullable = col.nullable && !col.primaryKey ? " | null" : "";

	// A rule wins over the built-in mapping (custom enum types, decorators, …).
	const rule = resolveRule(rules, table, col);
	if (rule?.decorator || rule?.tsType) {
		collectImports(sink, rule);
		const decorator =
			rule.decorator ?? (col.primaryKey ? "@PrimaryKey()" : "@Column()");
		const tsType = rule.tsType ?? toTsType(col.type);
		return `\t${decorator} declare ${prop}: ${tsType}${nullable};`;
	}

	if (isDateType(col.type) && !col.primaryKey) {
		const opts = cn ? `{ ${cn} }` : "";
		const dateNullable = col.nullable ? " | null" : "";
		return `\t@column.dateTime(${opts}) declare ${prop}: DateTime${dateNullable};`;
	}
	const opts = cn ? `{ ${cn} }` : "";
	const decorator = col.primaryKey ? "@PrimaryKey()" : `@Column(${opts})`;
	return `\t${decorator} declare ${prop}: ${toTsType(col.type)}${nullable};`;
}

/**
 * Render one `<Table>Schema extends BaseModel` class (with Lucid `$columns`).
 * A non-default `schema` qualifies `static table` (`"schema.table"` — the Rust
 * compiler quotes it as `"schema"."table"`) and prefixes the class name to keep
 * `public.users` / `tenant.users` distinct.
 */
function renderClass(
	table: string,
	columns: IntrospectedColumn[],
	rules: SchemaRules,
	sink: ImportSink,
	compact: boolean,
	schema?: string,
): string {
	const className = schema
		? `${toPascal(schema)}${toPascal(table)}Schema`
		: `${toPascal(table)}Schema`;
	const qualifiedTable = schema ? `${schema}.${table}` : table;
	const props = columns.map((c) => toCamel(c.name));
	const cols = props.map((p) => `"${p}"`).join(", ");
	const body = columns
		.map((c) => renderColumn(c, table, rules, sink))
		.join("\n");
	// Compact drops the blank line between `$columns` and the column declarations.
	const gap = compact ? "\n" : "\n\n";
	return (
		`export class ${className} extends BaseModel {\n` +
		`\tstatic table = "${qualifiedTable}";\n` +
		`\tstatic $columns = [${cols}] as const;\n` +
		`\t$columns = ${className}.$columns;${gap}${body}\n}`
	);
}

/** Emit the deduped `import { a, b } from "source"` lines from a sink. */
function renderRuleImports(sink: ImportSink): string {
	const lines: string[] = [];
	for (const [source, names] of sink) {
		lines.push(`import { ${[...names].join(", ")} } from "${source}";\n`);
	}
	return lines.join("");
}

/**
 * Render the full `schema.ts` source from introspected tables. Pure — no DB
 * access — so it is unit-testable. `rules` (Adonis Lucid `rulesPaths`) override
 * per-column TS types/decorators. Manual edits to the output are lost on
 * regeneration (same contract as Lucid).
 */
export function renderSchemaFile(
	tables: ReadonlyArray<{
		table: string;
		columns: IntrospectedColumn[];
		schema?: string;
	}>,
	rules: SchemaRules = {},
	compact = false,
): string {
	const sink: ImportSink = new Map();
	// Render the classes first so rule imports are fully collected for the header.
	const classes = tables
		.map((t) => renderClass(t.table, t.columns, rules, sink, compact, t.schema))
		.join(compact ? "\n" : "\n\n");
	const hasDate = tables.some((t) =>
		t.columns.some(
			(c) => isDateType(c.type) && !resolveRule(rules, t.table, c)?.tsType,
		),
	);
	const header =
		"// Generated by @c9up/atlas schema:generate — DO NOT EDIT.\n" +
		"// Manual edits are lost on regeneration.\n" +
		'import { BaseModel, Column, column, PrimaryKey } from "@c9up/atlas";\n' +
		(hasDate ? 'import { DateTime } from "@c9up/chronos";\n' : "") +
		renderRuleImports(sink);
	if (tables.length === 0) return `${header}\n// (no tables)\n`;
	return `${header}\n${classes}\n`;
}

/**
 * Introspect the live database and (re)write the schema file. Returns the number
 * of tables written. Reused by the {@link schemaGenerateCommand} and by the
 * migration commands' auto-regeneration (Adonis Lucid regenerates after
 * migrate/rollback/etc).
 */
export async function generateSchemaFile(
	db: AsyncDatabaseConnection,
	options: SchemaGenerateOptions,
): Promise<number> {
	const exclude = new Set(options.excludeTables ?? []);
	const tables: Array<{
		table: string;
		columns: IntrospectedColumn[];
		schema?: string;
	}> = [];

	// With `schemas`, list + introspect each named schema's tables against THAT
	// schema (Lucid). Without it, use the current schema/database as before.
	const schemaScopes: Array<string | undefined> =
		options.schemas && options.schemas.length > 0
			? options.schemas
			: [undefined];
	for (const schema of schemaScopes) {
		const names = (
			await listUserTables(db, db.dialect, {
				schemas: schema ? [schema] : undefined,
			})
		)
			.filter((n) => !exclude.has(n))
			.sort();
		for (const table of names) {
			const columns = await introspectTable(db, db.dialect, table, schema);
			if (columns) tables.push({ table, columns, schema });
		}
	}

	const rules = await loadSchemaRules(options.rulesPaths);
	const source = renderSchemaFile(tables, rules, options.compact ?? false);
	await fsp.mkdir(path.dirname(options.outputPath), { recursive: true });
	await fsp.writeFile(options.outputPath, source);
	return tables.length;
}

/** `schema:generate` — introspect the DB and (re)write the schema file. */
export function schemaGenerateCommand(
	options: SchemaGenerateOptions,
): AtlasCommand {
	return {
		name: "schema:generate",
		description:
			"Introspect the database and generate BaseModel schema classes",
		async run(_args, flags) {
			// `enabled: false` disables the command too (Adonis Lucid), not just the
			// post-migration regeneration.
			if (options.enabled === false) {
				console.log("[atlas] schema generation is disabled (enabled: false)");
				return;
			}
			// `--connection <name>` targets a specific registered connection (Adonis
			// Lucid); without it, the default connection is used.
			const connName =
				typeof flags.connection === "string" ? flags.connection : undefined;
			const db = connName ? getConnection(connName) : getDb();
			if (!db) {
				console.error(
					connName
						? `[atlas] no connection registered under '${connName}'`
						: "[atlas] no database connection — is AtlasProvider registered?",
				);
				process.exitCode = 1;
				return;
			}
			// `--compact-output` overrides the configured `compact`.
			const compact =
				flags["compact-output"] === true || flags["compact-output"] === "true"
					? true
					: options.compact;
			const n = await generateSchemaFile(db, { ...options, compact });
			console.log(
				`Generated ${options.outputPath} (${n} table${n === 1 ? "" : "s"})`,
			);
		},
	};
}
