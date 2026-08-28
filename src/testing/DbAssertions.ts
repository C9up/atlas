/**
 * `db()` — a helix plugin (AdonisJS Lucid database-assertions parity) that
 * injects database assertions on the test context as `ctx.db`:
 *
 *   // tests/bootstrap.ts
 *   import { configure } from "@c9up/helix";
 *   import { db } from "@c9up/atlas/testing";
 *   await configure({ plugins: [db(connection)] });
 *
 *   test("registers the user", async ({ db }) => {
 *     await db.assertHas("users", { email: "a@b.c" });
 *     await db.assertCount("users", 1);
 *   });
 *
 * Values are always bound as parameters; table/column identifiers are validated
 * against a strict pattern (letters, digits, `_`, `.`) and quoted, so neither a
 * value nor an identifier can inject SQL.
 *
 * NOTE (namespace): the AdonisJS docs expose these as `db.assert*`; the exact
 * surface is re-verified against the Adonis docs when available. Model-based
 * `assertModelExists`/`assertModelMissing` (entity PK metadata) are pending.
 */

import type { Plugin } from "@c9up/helix";

/** The minimal connection the assertions need — a parameterized query runner. */
export interface DbConnectionLike {
	query(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
	/**
	 * The engine this connection speaks. Absent on a hand-rolled stub, in which
	 * case the assertions fall back to `?` placeholders and emit no cast, which
	 * is what SQLite and MySQL want.
	 */
	dialect?: string;
}

/**
 * Column types Postgres will NOT compare against a text-bound parameter.
 *
 * A driver binds a JS string as `text`; `uuid = text` then fails with
 * "operator does not exist". The cast is derived from the COLUMN's declared
 * type, read from the catalog — never guessed from the value, since a text
 * column holding a UUID-shaped string must keep comparing as text.
 */
const PG_CASTABLE = new Set([
	"uuid",
	"timestamp without time zone",
	"timestamp with time zone",
	"date",
	"time without time zone",
	"time with time zone",
	"numeric",
	"json",
	"jsonb",
	"inet",
	"interval",
]);

/** Catalog lookups are per (table, connection) and never change mid-run. */
const columnTypeCache = new WeakMap<
	DbConnectionLike,
	Map<string, Map<string, string>>
>();

/**
 * `column → declared type` for one table, from `information_schema`.
 *
 * A failure here is not fatal: the assertions simply emit no cast, which is
 * exactly the pre-existing behaviour. A broken catalog read must not turn a
 * passing assertion into an error about introspection.
 */
async function columnTypes(
	conn: DbConnectionLike,
	table: string,
): Promise<Map<string, string>> {
	let perConn = columnTypeCache.get(conn);
	if (!perConn) {
		perConn = new Map();
		columnTypeCache.set(conn, perConn);
	}
	const cached = perConn.get(table);
	if (cached) return cached;

	const types = new Map<string, string>();
	try {
		// `table` may be schema-qualified: compare on the last segment.
		const bare = table.includes(".")
			? table.slice(table.lastIndexOf(".") + 1)
			: table;
		const rows = await conn.query(
			"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
			[bare],
		);
		for (const row of rows) {
			const name = row.column_name;
			const type = row.data_type;
			if (typeof name === "string" && typeof type === "string") {
				types.set(name, type.toLowerCase());
			}
		}
	} catch {
		// No catalog, no cast — see the doc above.
	}
	perConn.set(table, types);
	return types;
}

/** Reject identifiers that could break out of the quoted context. */
function quoteIdent(name: string): string {
	if (!/^[A-Za-z_][A-Za-z0-9_.]*$/.test(name)) {
		throw new Error(`db assertion: unsafe identifier ${JSON.stringify(name)}`);
	}
	return `"${name.replace(/\./g, '"."')}"`;
}

async function countRows(
	conn: DbConnectionLike,
	table: string,
	payload?: Record<string, unknown>,
): Promise<number> {
	// Postgres numbers its placeholders; MySQL and SQLite use `?`. Emitting `?`
	// on Postgres produced "syntax error at or near AND" — the marker was never
	// dialect-aware.
	const isPostgres = conn.dialect === "postgres";
	let where = "";
	let params: unknown[] = [];
	if (payload) {
		const keys = Object.keys(payload);
		if (keys.length > 0) {
			const casts = isPostgres ? await columnTypes(conn, table) : undefined;
			const predicates = keys.map((k, i) => {
				if (!isPostgres) return `${quoteIdent(k)} = ?`;
				const declared = casts?.get(k);
				const cast =
					declared &&
					PG_CASTABLE.has(declared) &&
					typeof payload[k] === "string"
						? `::${declared}`
						: "";
				return `${quoteIdent(k)} = $${i + 1}${cast}`;
			});
			where = ` WHERE ${predicates.join(" AND ")}`;
			params = keys.map((k) => payload[k]);
		}
	}
	const rows = await conn.query(
		`SELECT COUNT(*) AS c FROM ${quoteIdent(table)}${where}`,
		params,
	);
	return Number(rows[0]?.c ?? 0);
}

/** The `ctx.db` assertion surface (AdonisJS Lucid database-assertions parity). */
export interface DbAssertions {
	/** At least one row in `table` matches every column in `payload`. */
	assertHas(table: string, payload: Record<string, unknown>): Promise<void>;
	/** No row in `table` matches `payload`. */
	assertMissing(table: string, payload: Record<string, unknown>): Promise<void>;
	/** `table` has exactly `expected` rows (optionally matching `payload`). */
	assertCount(
		table: string,
		expected: number,
		payload?: Record<string, unknown>,
	): Promise<void>;
	/** `table` has no rows at all. */
	assertEmpty(table: string): Promise<void>;
}

/** Build the assertion surface bound to a connection. */
export function createDbAssertions(conn: DbConnectionLike): DbAssertions {
	return {
		async assertHas(table, payload) {
			const n = await countRows(conn, table, payload);
			if (n === 0) {
				throw new Error(
					`Expected "${table}" to have a row matching ${JSON.stringify(payload)}, found none.`,
				);
			}
		},
		async assertMissing(table, payload) {
			const n = await countRows(conn, table, payload);
			if (n > 0) {
				throw new Error(
					`Expected "${table}" to have NO row matching ${JSON.stringify(payload)}, found ${n}.`,
				);
			}
		},
		async assertCount(table, expected, payload) {
			const n = await countRows(conn, table, payload);
			if (n !== expected) {
				const scope = payload ? ` matching ${JSON.stringify(payload)}` : "";
				throw new Error(
					`Expected "${table}" to have ${expected} row(s)${scope}, found ${n}.`,
				);
			}
		},
		async assertEmpty(table) {
			const n = await countRows(conn, table);
			if (n > 0) {
				throw new Error(`Expected "${table}" to be empty, found ${n} row(s).`);
			}
		},
	};
}

/** The `db()` helix plugin — registers {@link DbAssertions} as `ctx.db`. */
export function db(conn: DbConnectionLike): Plugin {
	const assertions = createDbAssertions(conn);
	return (api) => {
		api.context.macro("db", assertions);
	};
}

// Typing side of the plugin — importing `@c9up/atlas/testing` augments the
// helix test context with `db` (the helix pattern).
declare module "@c9up/helix" {
	interface TestContext {
		db: DbAssertions;
	}
}
