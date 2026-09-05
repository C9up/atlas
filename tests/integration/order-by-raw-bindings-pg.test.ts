/**
 * `orderByRaw` and `groupByRaw` bind their placeholders.
 *
 * `whereRaw` and `havingRaw` took bindings; these two took only a string. So a
 * fragment like `word_similarity(?, name) DESC` reached the server with its
 * `?` intact and was refused — and the only way to rank by a score computed
 * from a value was to interpolate that value into the SQL, which is the one
 * thing a query builder exists to make unnecessary.
 *
 *   ATLAS_TEST_PG_URL=postgres://postgres:secret@localhost:5432/postgres \
 *     pnpm exec vitest run tests/integration/order-by-raw-bindings-pg.test.ts
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import db, { clearDb, setDb } from "../../src/services/db.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

describePg("atlas > orderByRaw / groupByRaw bind their placeholders", () => {
	let conn: AsyncDatabaseConnection;

	beforeAll(async () => {
		conn = await createNapiConnection(PG_URL, 1, 1);
		await conn.execute("DROP TABLE IF EXISTS ranked");
		await conn.execute(
			"CREATE TABLE ranked (id INTEGER PRIMARY KEY, name TEXT, team TEXT, score INTEGER)",
		);
		await conn.execute(
			"INSERT INTO ranked VALUES (1,'ada','x',5),(2,'adam','x',9),(3,'bob','y',1)",
		);
		setDb(conn);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS ranked");
		clearDb(conn);
		await conn.close();
	});

	it("ranks by a score computed from a bound value", async () => {
		// The reported call, verbatim in shape: a similarity ordering.
		const rows = await db
			.from("ranked")
			.select("name")
			.orderByRaw("position(? in name) DESC, name ASC", ["ada"]);

		expect(rows).toEqual([{ name: "ada" }, { name: "adam" }, { name: "bob" }]);
	});

	it("keeps a raw term in its place among the plain ones", async () => {
		const { sql } = db
			.from("ranked")
			.orderBy("team")
			.orderByRaw("score * ? DESC", [2])
			.orderBy("name")
			.toSQL();

		expect(sql.indexOf("score")).toBeGreaterThan(sql.indexOf('"team"'));
		expect(sql.indexOf('"name"')).toBeGreaterThan(sql.indexOf("score"));
	});

	it("numbers a raw ordering's parameters after the ones before it", async () => {
		// The clause is compiled after WHERE, so its placeholder is not $1.
		const rows = await db
			.from("ranked")
			.select("name")
			.where("team", "x")
			.orderByRaw("position(? in name) DESC", ["adam"]);

		expect(rows).toEqual([{ name: "adam" }, { name: "ada" }]);
	});

	it("binds a groupByRaw fragment too", async () => {
		const rows = await db
			.from("ranked")
			.select(db.raw("count(*)::int as n"))
			.groupByRaw("left(name, ?)", [1]);

		expect(rows).toEqual([{ n: 2 }, { n: 1 }]);
	});

	it("passes a value that would be SQL if it were interpolated", async () => {
		// The reason the bindings matter: this string is compared, never parsed.
		const rows = await db
			.from("ranked")
			.select("name")
			.orderByRaw("position(? in name) DESC, id ASC", [
				"'; DROP TABLE ranked; --",
			]);

		expect(rows).toHaveLength(3);
		const alive = await db.rawQuery("select count(*)::int as n from ranked");
		expect(alive).toEqual([{ n: 3 }]);
	});

	it("still takes a fragment with no bindings at all", async () => {
		const rows = await db.from("ranked").select("name").orderByRaw("id DESC");
		expect(rows).toEqual([{ name: "bob" }, { name: "adam" }, { name: "ada" }]);
	});
});
