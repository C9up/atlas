/**
 * Inside a Postgres transaction, a statement that does not parse reports its
 * own error, against a REAL PostgreSQL, gated on ATLAS_TEST_PG_URL.
 *
 * The binder first PREPAREs a statement to learn its parameter types and
 * dropped that PREPARE's error. Inside a transaction the failed PREPARE had
 * already aborted it, so the caller only ever saw "current transaction is
 * aborted": a failing migration named its statement but never why.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

describePg("atlas > Postgres transaction parse errors", () => {
	let conn: AsyncDatabaseConnection;

	async function begin() {
		if (!conn.transaction)
			throw new Error("the napi connection opens transactions");
		return conn.transaction();
	}

	beforeAll(async () => {
		conn = await createNapiConnection(PG_URL, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS tx_parse_items");
		await conn.execute("CREATE TABLE tx_parse_items (id integer PRIMARY KEY)");
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS tx_parse_items");
		await conn.close();
	});

	it.each([
		["without parameters", "INSERT INTO tx_parse_nope (x) VALUES (1)", []],
		["with parameters", "INSERT INTO tx_parse_nope (x) VALUES ($1)", [1]],
	])(
		"a migration batch reports the real cause %s",
		async (_label, sql, params) => {
			const error = await conn
				.runInTransaction([
					{ sql: "INSERT INTO tx_parse_items (id) VALUES (1)" },
					{ sql, params },
				])
				.then(
					() => undefined,
					(e: unknown) => e,
				);
			expect(String(error)).toMatch(/relation "tx_parse_nope" does not exist/);
			expect(String(error)).not.toMatch(/current transaction is aborted/);
			expect(await conn.query("SELECT id FROM tx_parse_items")).toEqual([]);
		},
	);

	it("an interactive transaction reports the real cause on execute and query", async () => {
		const trx = await begin();
		try {
			await expect(
				trx.execute("UPDATE tx_parse_nope SET x = $1", [1]),
			).rejects.toThrow(/relation "tx_parse_nope" does not exist/);
		} finally {
			await trx.rollback();
		}
		const second = await begin();
		try {
			await expect(
				second.query(
					"SELECT missing_column FROM tx_parse_items WHERE id = $1",
					[1],
				),
			).rejects.toThrow(/column "missing_column" does not exist/);
		} finally {
			await second.rollback();
		}
	});

	it("still runs a parameter Postgres cannot type outside a transaction", async () => {
		expect(await conn.query("SELECT $1 AS v", ["x"])).toEqual([{ v: "x" }]);
	});
});
