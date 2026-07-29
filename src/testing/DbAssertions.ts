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
	let where = "";
	let params: unknown[] = [];
	if (payload) {
		const keys = Object.keys(payload);
		if (keys.length > 0) {
			where = ` WHERE ${keys.map((k) => `${quoteIdent(k)} = ?`).join(" AND ")}`;
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
// helix test context with `db` (the Japa pattern).
declare module "@c9up/helix" {
	interface TestContext {
		db: DbAssertions;
	}
}
