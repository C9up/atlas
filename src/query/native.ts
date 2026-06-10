/**
 * Native query compiler loader — loads the Rust NAPI binary.
 *
 * @implements FR36
 */

import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { arch, platform } from "node:process";
import { fileURLToPath } from "node:url";

const require2 = createRequire(import.meta.url);
const __dirname2 = dirname(fileURLToPath(import.meta.url));

const platformMap: Record<string, string> = {
	"linux-x64": "linux-x64-gnu",
	"darwin-x64": "darwin-x64",
	"darwin-arm64": "darwin-arm64",
	"win32-x64": "win32-x64-msvc",
	"linux-arm64": "linux-arm64-gnu",
};

interface NativeBinding {
	compileStatement: (specJson: string, dialect: string) => string;
	quoteIdent: (name: string) => string;
}

let native: NativeBinding | undefined;

let loadError: unknown;

try {
	const suffix = platformMap[`${platform}-${arch}`];
	if (suffix) {
		native = require2(join(__dirname2, `../../index.${suffix}.node`));
	}
} catch (e) {
	loadError = e;
}

export type AtlasDialect = "sqlite" | "postgres" | "mysql";

/** Module-level default dialect. Set by AtlasProvider at boot. */
let defaultDialect: AtlasDialect = "sqlite";

export function setAtlasDialect(dialect: AtlasDialect): void {
	defaultDialect = dialect;
}

export function getAtlasDialect(): AtlasDialect {
	return defaultDialect;
}

export interface CompiledStatement {
	statements: string[];
	params: unknown[];
}

/**
 * Per-table Postgres cast hints (snake column → logical type, e.g. `uuid`).
 * Populated once per entity by `BaseRepository` and consulted at the single
 * compile chokepoint below, so EVERY statement built anywhere (the fluent
 * `ModelQuery`, relation loaders, direct repo methods) gets `$N::uuid` casts
 * on its WHERE/SET/value params without threading the cast map through every
 * call site. Postgres-only at the SQL level; the Rust compiler emits casts
 * only on the postgres dialect.
 */
const castRegistry = new Map<string, Record<string, string>>();

/**
 * Register an entity table's cast map (MERGES — a FK cast another entity already
 * published for this table via `registerColumnCast` survives, regardless of
 * which repository is constructed first).
 */
export function registerTableCasts(
	table: string,
	casts: Record<string, string>,
): void {
	if (Object.keys(casts).length === 0) return;
	const existing = castRegistry.get(table);
	castRegistry.set(table, existing ? { ...existing, ...casts } : { ...casts });
}

/**
 * Register a single column's cast on a table. Used by `BaseRepository` to publish
 * relation FK column types (a FK references a typed PK) onto the OTHER table, so
 * eager/lazy relation WHEREs on an untyped uuid FK still get `::uuid`.
 */
export function registerColumnCast(
	table: string,
	column: string,
	type: string,
): void {
	const existing = castRegistry.get(table);
	if (existing) existing[column] = type;
	else castRegistry.set(table, { [column]: type });
}

const CAST_BEARING_KINDS = new Set([
	"select",
	"insert",
	"update",
	"delete",
	"upsert",
]);

/**
 * Inject the registered cast map for a statement's table. A spec that already
 * carries explicit `casts` for a NON-registered table (e.g. an m2m pivot insert
 * with bespoke `pivotCasts`) is left untouched — only entity tables are in the
 * registry, and their explicit casts are identical to the registered map.
 */
function isStringRecord(v: unknown): v is Record<string, string> {
	return v !== null && typeof v === "object" && !Array.isArray(v);
}

function withRegistryCasts(spec: object): object {
	if (!("table" in spec) || !("kind" in spec)) return spec;
	const { table, kind } = spec;
	if (
		typeof table !== "string" ||
		typeof kind !== "string" ||
		!CAST_BEARING_KINDS.has(kind)
	) {
		return spec;
	}
	const registered = castRegistry.get(table);
	if (!registered) return spec;
	// Registered casts are the base; any explicit per-statement casts (e.g. a
	// relation loader hinting a FK column the related entity didn't type) win.
	const explicit =
		"casts" in spec && isStringRecord(spec.casts) ? spec.casts : {};
	return { ...spec, casts: { ...registered, ...explicit } };
}

/**
 * Compile any statement (SELECT/INSERT/UPDATE/DELETE/DDL) via the Rust compiler.
 * `spec` is a tagged object: `{ kind: 'select' | 'insert' | ..., ... }`.
 */
export function compileStatementNative(
	spec: object,
	dialect: AtlasDialect = defaultDialect,
): CompiledStatement {
	if (!native) {
		throw new Error(
			`[ATLAS_NAPI_NOT_FOUND] Rust query compiler not available: ${loadError ?? "binary not found"}`,
		);
	}
	const json = native.compileStatement(
		JSON.stringify(withRegistryCasts(spec)),
		dialect,
	);
	return JSON.parse(json);
}
