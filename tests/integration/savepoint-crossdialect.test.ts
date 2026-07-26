/**
 * Nested transactions via SAVEPOINT across dialects. SQLite always runs; Postgres
 * and MySQL run when their URL is set. MySQL is the point of this suite: it
 * rejects SAVEPOINT over the prepared protocol (error 1295), so those statements
 * route through the text protocol (`block_in_place` + `raw_sql` in the napi layer).
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasDialect } from "../../src/query/native.js";

function begin(conn: AsyncDatabaseConnection) {
	if (!conn.transaction) throw new Error("connection lacks transaction()");
	return conn.transaction();
}

function suite(
	label: string,
	url: string,
	dialect: "sqlite" | "postgres" | "mysql",
	pk: string,
) {
	(url ? describe : describe.skip)(`savepoint ${label}`, () => {
		let c: AsyncDatabaseConnection;
		beforeAll(async () => {
			setAtlasDialect(dialect);
			c = await createNapiConnection(url, 1, 2);
			await c.execute("DROP TABLE IF EXISTS sp_x");
			await c.execute(`CREATE TABLE sp_x (id ${pk})`);
		});
		afterAll(async () => {
			await c?.execute("DROP TABLE IF EXISTS sp_x");
			await c?.close();
		});

		it("manual trx.transaction(): rollback undoes the savepoint, keeps root", async () => {
			await c.execute("DELETE FROM sp_x", []);
			const trx = await begin(c);
			await trx.execute("INSERT INTO sp_x (id) VALUES (1)", []);
			const sp = await trx.transaction();
			await sp.execute("INSERT INTO sp_x (id) VALUES (2)", []);
			await sp.rollback();
			await trx.commit();
			const rows = await c.query<{ id: number }>(
				"SELECT id FROM sp_x ORDER BY id",
				[],
			);
			expect(rows.map((r) => Number(r.id))).toEqual([1]);
		});

		it("managed trx.transaction(cb): commits the savepoint", async () => {
			await c.execute("DELETE FROM sp_x", []);
			const trx = await begin(c);
			await trx.transaction(async (sp) => {
				await sp.execute("INSERT INTO sp_x (id) VALUES (5)", []);
			});
			await trx.commit();
			const rows = await c.query<{ id: number }>("SELECT id FROM sp_x", []);
			expect(rows.map((r) => Number(r.id))).toEqual([5]);
		});

		it("managed nested rollback on throw undoes only the savepoint", async () => {
			await c.execute("DELETE FROM sp_x", []);
			const trx = await begin(c);
			await trx.execute("INSERT INTO sp_x (id) VALUES (7)", []);
			await expect(
				trx.transaction(async (sp) => {
					await sp.execute("INSERT INTO sp_x (id) VALUES (8)", []);
					throw new Error("boom");
				}),
			).rejects.toThrow("boom");
			await trx.commit();
			const rows = await c.query<{ id: number }>(
				"SELECT id FROM sp_x ORDER BY id",
				[],
			);
			expect(rows.map((r) => Number(r.id))).toEqual([7]);
		});
	});
}

suite("SQLite", "sqlite::memory:", "sqlite", "INTEGER PRIMARY KEY");
suite(
	"PG",
	process.env.ATLAS_TEST_PG_URL ?? "",
	"postgres",
	"INTEGER PRIMARY KEY",
);
suite(
	"MySQL",
	process.env.ATLAS_TEST_MYSQL_URL ?? "",
	"mysql",
	"INT PRIMARY KEY",
);
