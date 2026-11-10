/**
 * Native query compiler loader — loads the Rust NAPI binary.
 *
 * @implements FR36
 */

import { AtlasError } from "../errors.js";
import {
	classifyRefusal,
	emitUnsafeStatement,
	hasUnsafeStatementListeners,
} from "../events.js";
import type {
	compileStatement,
	NamedBindings,
	namedToPositional as nativeNamedToPositional,
} from "../native/generated.js";
import { hasFunctions, loadNativeBinary } from "../vendor/nativeBinary.js";
import { RawSql } from "./RawSql.js";

/**
 * What this module calls on the `index.<platform>.node` binary.
 *
 * The signature comes from `../native/generated.js`, derived from the `#[napi]`
 * items themselves, so it cannot drift from the Rust the way a restatement
 * here silently would. Run `pnpm build:napi-types` after touching one. The
 * guard checks the same name at load: a stale binary loads just as well.
 */
interface NativeBinding {
	compileStatement: typeof compileStatement;
	namedToPositional: typeof nativeNamedToPositional;
}

const NATIVE_CALLS = ["compileStatement", "namedToPositional"] as const;

function isNativeBinding(value: unknown): value is NativeBinding {
	return hasFunctions(value, NATIVE_CALLS);
}

const attempt = loadNativeBinary(isNativeBinding, NATIVE_CALLS);
const native = attempt.loaded ? attempt.binary : undefined;
const loadError = attempt.loaded ? undefined : attempt.cause;

export type AtlasDialect = "sqlite" | "postgres" | "mysql";

/**
 * Dialect names accepted by `ifDialect`/`unlessDialect`, including the Lucid/Knex
 * client aliases — so a callback ported from Lucid (`ifDialect('sqlite3', …)`,
 * which Lucid names `sqlite3`/`better-sqlite3`, not `sqlite`) still matches.
 */
export type DialectName =
	| AtlasDialect
	| "sqlite3"
	| "better-sqlite3"
	| "postgresql"
	| "pg"
	| "mysql2";

const DIALECT_ALIASES: Record<string, AtlasDialect> = {
	sqlite: "sqlite",
	sqlite3: "sqlite",
	"better-sqlite3": "sqlite",
	postgres: "postgres",
	postgresql: "postgres",
	pg: "postgres",
	mysql: "mysql",
	mysql2: "mysql",
};

/** Map a Lucid/Knex dialect name to the atlas dialect, or `undefined` if unknown. */
export function normalizeDialect(name: string): AtlasDialect | undefined {
	return DIALECT_ALIASES[name];
}

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
 * Drop every registered cast. Called by `AtlasProvider.shutdown()` so a re-boot
 * (tests, hot-reload) starts from a clean registry rather than accumulating casts
 * across runs — two suites declaring the same table with different column types
 * would otherwise pollute each other depending on execution order.
 */
export function clearCastRegistry(): void {
	castRegistry.clear();
}

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

function isRecord(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

/**
 * JSON replacer for every spec sent to the compiler.
 *
 * JSON has no NaN or Infinity: `JSON.stringify` writes them as `null`, so a
 * `limit(NaN)` dropped the LIMIT and `increment('balance', NaN)` wrote
 * `balance + NULL`. They are refused here, where every statement passes.
 *
 * A `db.raw()` given as a WHERE/HAVING comparison value becomes the clause's
 * `rawValue`, which the compiler inlines with its own bindings. Anywhere else a
 * `RawSql` would be bound as the JSON object `{ sql, params }`, so it is
 * refused rather than compared or stored as text.
 */
function lowerSpecValue(
	dialect: AtlasDialect,
): (key: string, value: unknown) => unknown {
	return (key, value) => lowerValue(key, value, dialect);
}

function lowerValue(
	key: string,
	value: unknown,
	dialect: AtlasDialect,
): unknown {
	if (typeof value === "number" && !Number.isFinite(value)) {
		throw new AtlasError(
			"E_ATLAS_NON_FINITE_NUMBER",
			`Cannot bind ${value} (${key || "value"}): SQL parameters must be finite numbers.`,
			{
				hint: "Validate numeric input (Number.isFinite) before it reaches the query.",
			},
		);
	}
	if (value instanceof RawSql) {
		throw new AtlasError(
			"E_ATLAS_RAW_VALUE",
			`A db.raw() fragment (${value.sql}) cannot be used as a value here (${key || "value"}).`,
			{
				hint: "Raw fragments are accepted in select, where/having values, orderBy, and as insert/update/merge values.",
			},
		);
	}
	if (isRecord(value) && typeof value.column === "string") {
		const raw = value.value;
		if (raw instanceof RawSql) {
			const resolved = rawForDialect(raw, dialect);
			const clause: Record<string, unknown> = {
				...value,
				rawValue: { sql: resolved.sql, bindings: resolved.params },
			};
			delete clause.value;
			return clause;
		}
	}
	return value;
}

/** A `[column, value]` pair as insert rows and update SETs carry them. */
function isPair(v: unknown): v is [string, unknown] {
	return Array.isArray(v) && v.length === 2 && typeof v[0] === "string";
}

/**
 * Move every `db.raw()` VALUE of a write out of the data, the way Knex inlines
 * a Raw in insert/update (`client.parameter` → `raw.toSQL()`): an insert row
 * cell becomes a `raws` entry rendered by the compiler with its bindings, and
 * an update SET or a merge assignment becomes its `{ raw, rawParams }` form.
 * Kept beside the data, never encoded in it, so no row value can turn into SQL.
 */
function liftRawWriteValues(spec: object, dialect: AtlasDialect): object {
	if (!isRecord(spec) || typeof spec.kind !== "string") return spec;
	if (spec.kind === "insert" || spec.kind === "upsert") {
		const raws: Array<{
			row: number;
			column: string;
			sql: string;
			bindings: unknown[];
		}> = [];
		const liftRow = (row: unknown, index: number): unknown =>
			Array.isArray(row)
				? row.map((pair) => {
						if (!isPair(pair) || !(pair[1] instanceof RawSql)) return pair;
						const resolved = rawForDialect(pair[1], dialect);
						raws.push({
							row: index,
							column: pair[0],
							sql: resolved.sql,
							bindings: resolved.params,
						});
						return [pair[0], null];
					})
				: row;
		const out: Record<string, unknown> = { ...spec };
		if (Array.isArray(spec.rows)) out.rows = spec.rows.map(liftRow);
		if (Array.isArray(spec.values)) out.values = liftRow(spec.values, 0);
		if (Array.isArray(spec.updateSet)) {
			out.updateSet = spec.updateSet.map((item) => {
				if (!isRecord(item) || !(item.value instanceof RawSql)) return item;
				const resolved = rawForDialect(item.value, dialect);
				return {
					column: item.column,
					raw: resolved.sql,
					rawParams: resolved.params,
				};
			});
		}
		if (raws.length > 0) out.raws = raws;
		return out;
	}
	if (spec.kind === "update" && Array.isArray(spec.set)) {
		return {
			...spec,
			set: spec.set.map((pair) => {
				if (!isPair(pair) || !(pair[1] instanceof RawSql)) return pair;
				const resolved = rawForDialect(pair[1], dialect);
				return [pair[0], { raw: resolved.sql, rawParams: resolved.params }];
			}),
		};
	}
	return spec;
}

/** The binary, or the error that says why it is not there. */
function nativeBinding(): NativeBinding {
	if (!native) {
		throw new Error(
			"[E_ATLAS_NAPI_NOT_FOUND] Rust query compiler not available. Build with: cargo build --release",
			loadError !== undefined ? { cause: loadError } : undefined,
		);
	}
	return native;
}

/**
 * Rewrite Knex named bindings (`:name`, `:name:`) to the `?` notation with
 * the compiler's own SQL scanner, so a name is only a binding in SQL code —
 * never inside a string, quoted identifier, comment or dollar-quoted body.
 * Only the names in `keys` are bindings; the rest stay as written.
 */
export function namedToPositionalNative(
	sql: string,
	keys: string[],
	dialect: AtlasDialect,
): NamedBindings {
	return nativeBinding().namedToPositional(sql, keys, dialect);
}

/**
 * Lucid/Knex named bindings → the positional notation the compiler reads.
 *
 * `:name` becomes `?` and `:name:` becomes `??` (a quoted identifier), with the
 * values in occurrence order; `\:` escapes a colon. The SQL is read by the
 * compiler's own scanner for `dialect`: a name is a binding only in SQL code,
 * never inside a string, a quoted identifier, a comment or a dollar-quoted
 * body (Knex rewrites those too), and a `::` cast is never one. A `:name` that
 * is not an own key of the bindings is left as written, as Knex does: a typo'd
 * key reaches the server as the syntax error it is instead of binding NULL.
 */
export function namedToPositional(
	sql: string,
	bindings: Record<string, unknown>,
	dialect: AtlasDialect,
): { sql: string; bindings: unknown[] } {
	const rewritten = namedToPositionalNative(
		sql,
		Object.getOwnPropertyNames(bindings),
		dialect,
	);
	const values = rewritten.names.map((name) => bindings[name]);
	return {
		sql: rewritten.sql,
		bindings: assertDefinedBindings(rewritten.sql, values),
	};
}

/**
 * Refuse an `undefined` binding (Knex: "Undefined binding(s) detected"). JSON
 * carries it as `null`, so the statement silently compared or wrote NULL.
 */
export function assertDefinedBindings(
	sql: string,
	bindings: unknown[],
): unknown[] {
	const missing = bindings.flatMap((v, i) => (v === undefined ? [i] : []));
	if (missing.length > 0) {
		throw new AtlasError(
			"E_ATLAS_RAW_BINDINGS",
			`Undefined binding(s) detected at index ${missing.join(", ")} for raw SQL: ${sql}`,
		);
	}
	return bindings;
}

/**
 * A fragment's SQL and positional params for `dialect`: named bindings are
 * resolved here, by the query that compiles the fragment, so a `db.raw()`
 * built before any connection is read with the right dialect's scanner.
 */
export function rawForDialect(
	raw: RawSql,
	dialect: AtlasDialect,
): { sql: string; params: unknown[] } {
	const named = raw.namedBindings;
	if (named === undefined) return { sql: raw.sql, params: raw.params };
	const resolved = namedToPositional(raw.sql, { ...named }, dialect);
	return { sql: resolved.sql, params: resolved.bindings };
}

/**
 * Compile any statement (SELECT/INSERT/UPDATE/DELETE/DDL) via the Rust compiler.
 * `spec` is a tagged object: `{ kind: 'select' | 'insert' | ..., ... }`.
 */
export function compileStatementNative(
	spec: object,
	dialect: AtlasDialect = defaultDialect,
): CompiledStatement {
	const binding = nativeBinding();
	let json: string;
	try {
		json = binding.compileStatement(
			JSON.stringify(
				withRegistryCasts(liftRawWriteValues(spec, dialect)),
				lowerSpecValue(dialect),
			),
			dialect,
		);
	} catch (error) {
		// Every statement built anywhere passes through here, so this is the one
		// place a refusal can be reported from. Reported, not handled: the error
		// still reaches the caller unchanged, and atlas has no idea who made the
		// request — naming the offender, and deciding what to do about them, is
		// the host's to do. See `UnsafeStatementEvent`.
		if (hasUnsafeStatementListeners()) {
			const refusal = classifyRefusal(error);
			if (refusal !== undefined) {
				emitUnsafeStatement({
					...refusal,
					message: error instanceof Error ? error.message : String(error),
					connection: currentConnectionName,
				});
			}
		}
		throw error;
	}
	return JSON.parse(json);
}

/**
 * The connection a refusal is reported against.
 *
 * The compiler is a pure function with no connection of its own, so the name is
 * carried alongside the dialect the same way — set when a connection becomes
 * the active one.
 */
let currentConnectionName: string | undefined;

/** @internal Name the connection subsequent statements compile for. */
export function setAtlasConnectionName(name: string | undefined): void {
	currentConnectionName = name;
}
