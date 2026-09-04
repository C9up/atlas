/**
 * `db.rawQuery` against a REAL PostgreSQL, gated on ATLAS_TEST_PG_URL.
 *
 * This is the only dialect where the placeholder the caller writes and the one
 * the driver expects differ, and sqlx hands the statement to the server
 * verbatim. Every sqlite suite passed while all four forms below failed on
 * Postgres: `?` reached the server as a syntax error, and `$1` reached it with
 * the bindings array silently dropped — a signature promising bound parameters
 * and binding none, which is what pushes a caller to interpolate values by hand.
 *
 *   ATLAS_TEST_PG_URL=postgres://postgres:secret@localhost:5432/postgres \
 *     pnpm exec vitest run tests/integration/raw-query-pg.test.ts
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

describePg("atlas > db.rawQuery on PostgreSQL", () => {
	let conn: AsyncDatabaseConnection;

	beforeAll(async () => {
		conn = await createNapiConnection(PG_URL, 1, 1);
		await conn.execute("DROP TABLE IF EXISTS raw_probe");
		await conn.execute(
			"CREATE TABLE raw_probe (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		);
		await conn.execute(
			"INSERT INTO raw_probe VALUES (1,'ada',36),(2,'bob',18)",
		);
		setDb(conn);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS raw_probe");
		clearDb(conn);
		await conn.close();
	});

	it("binds a positional `?`", async () => {
		const rows = await db.rawQuery<{ name: string }>(
			"select name from raw_probe where id = ?",
			[1],
		);
		expect(rows).toEqual([{ name: "ada" }]);
	});

	it("binds a named `:id`", async () => {
		const rows = await db.rawQuery<{ name: string }>(
			"select name from raw_probe where id = :id",
			{ id: 2 },
		);
		expect(rows).toEqual([{ name: "bob" }]);
	});

	it("binds SQL already written with `$1`", async () => {
		// What a Postgres user writes by reflex. Rewriting only `?` left these
		// bindings behind and the server refused the statement for want of a
		// parameter — after the signature had promised one was bound.
		const rows = await db.rawQuery<{ name: string }>(
			"select name from raw_probe where id = $1",
			[1],
		);
		expect(rows).toEqual([{ name: "ada" }]);
	});

	it("binds around an alias, where the placeholder is not the last token", async () => {
		const rows = await db.rawQuery<{ who: string }>(
			"select name AS who from raw_probe where age > ?",
			[20],
		);
		expect(rows).toEqual([{ who: "ada" }]);
	});

	it("binds several parameters in occurrence order", async () => {
		const rows = await db.rawQuery<{ name: string }>(
			"select name from raw_probe where age > ? and id < ? order by id",
			[10, 3],
		);
		expect(rows).toEqual([{ name: "ada" }, { name: "bob" }]);
	});

	it("keeps a `::` cast out of the named-binding syntax", async () => {
		const rows = await db.rawQuery<{ data: unknown }>(
			"select :payload::jsonb as data",
			{ payload: '{"a":1}' },
		);
		expect(rows).toEqual([{ data: { a: 1 } }]);
	});

	it("passes a value that would be SQL if it were interpolated", async () => {
		// The whole point of a bound parameter: this string is compared, never
		// parsed. Interpolated, it would end the statement and start another.
		const rows = await db.rawQuery<{ name: string }>(
			"select name from raw_probe where name = ?",
			["ada'; DROP TABLE raw_probe; --"],
		);
		expect(rows).toEqual([]);
		// Still there, which is the assertion that matters.
		const all = await db.rawQuery("select count(*)::int as n from raw_probe");
		expect(all).toEqual([{ n: 2 }]);
	});

	it("binds a `db.raw()` fragment placed in the SELECT list", async () => {
		// The third path that takes SQL a caller typed. `whereRaw` and `havingRaw`
		// each carried their own copy of the `?` rewriting, so this one never got
		// it: the fragment reached Postgres with its `?` intact.
		const rows = await db
			.from("raw_probe")
			.select(db.raw("age + ? as bumped", [4]))
			.where("id", 1);
		expect(rows).toEqual([{ bumped: 40 }]);
	});

	it("does not add `*` beside a raw the caller selected", async () => {
		// A raw fragment IS a named column, so it replaces the `*` fallback
		// rather than joining it — otherwise asking for one computed column
		// hands back the whole row with it.
		const q = db.from("raw_probe").select(db.raw("age + ? as bumped", [4]));
		expect(q.toSQL().toNative().sql).not.toContain("*");
	});

	it("reports the statement the way every other builder does", () => {
		const q = db.rawQuery("select name from raw_probe where id = ?", [1]);
		// `.sql` normalised to `?` on every dialect; `.toNative()` carrying the
		// `$N` the server is actually sent.
		expect(q.toSQL().sql).toBe("select name from raw_probe where id = ?");
		expect(q.toSQL().toNative()).toEqual({
			sql: "select name from raw_probe where id = $1",
			bindings: [1],
		});
		expect(q.toQuery()).toBe("select name from raw_probe where id = 1");
	});
});
