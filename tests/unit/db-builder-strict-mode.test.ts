/**
 * Atlas strict mode reaches the connection-level builder.
 *
 * `db.from()` / `db.table()` / `trx.from()` hand out a DatabaseQueryBuilder,
 * which never consulted the switch: with strict mode on, `whereRaw('1=1 --')`
 * compiled, and `update({ n: db.raw('(select 1) or 1=1 --') })` commented out
 * the WHERE of the UPDATE. Every raw entry now goes through the same gate as
 * the model query; a `db.ref()` identifier is not raw SQL and still passes.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { setAtlasStrictMode } from "../../src/index.js";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../../src/query/DatabaseQueryBuilder.js";
import { RawSql, RefSql } from "../../src/query/QueryBuilder.js";

const exec: QueryExecutor = {
	query: async () => [],
	execute: async () => ({ rowsAffected: 0 }),
};

const users = (): DatabaseQueryBuilder =>
	new DatabaseQueryBuilder(exec, "sqlite", "users");
const payload = new RawSql("(select 1) or 1=1 --");

beforeEach(() => setAtlasStrictMode(true));
afterEach(() => setAtlasStrictMode(false));

const ENTRIES: Array<[string, () => unknown]> = [
	["whereRaw", () => users().whereRaw("1=1 -- x")],
	["andWhereRaw", () => users().andWhereRaw("1=1")],
	["orWhereRaw", () => users().orWhereRaw("1=1")],
	["whereNotRaw", () => users().whereNotRaw("1=0")],
	["orWhereNotRaw", () => users().orWhereNotRaw("1=0")],
	["orderByRaw", () => users().orderByRaw("(select password)")],
	["groupByRaw", () => users().groupByRaw("1")],
	["havingRaw", () => users().havingRaw("1=1")],
	["joinRaw", () => users().joinRaw("JOIN secrets ON 1=1")],
	["select(db.raw)", () => users().select(payload)],
	["orderBy(db.raw)", () => users().orderBy(payload)],
	["where(col, db.raw)", () => users().where("id", payload)],
	["where(col, op, db.raw)", () => users().where("id", "=", payload)],
	["having(col, op, db.raw)", () => users().having("id", ">", payload)],
	[
		"update(db.raw)",
		() => users().where("id", 1).update({ n: payload }).toSQL(),
	],
	["insert(db.raw)", () => users().insert({ n: payload }).toSQL()],
	["multiInsert(db.raw)", () => users().multiInsert([{ n: payload }])],
	[
		"merge(db.raw)",
		() => users().insert({ id: 1 }).onConflict("id").merge({ n: payload }),
	],
];

describe("atlas > strict mode on db.from()/db.table()", () => {
	it.each(ENTRIES)("%s is refused", (_method, call) => {
		expect(call).toThrow(/disabled in Atlas strict mode/);
	});

	it("still accepts a db.ref() identifier and structured clauses", () => {
		const sql = users()
			.select("id")
			.where("id", 1)
			.orderBy(new RefSql('"users"."id"'))
			.toSQL().sql;
		expect(sql).toContain('ORDER BY "users"."id"');
	});

	it("is inert when strict mode is off", () => {
		setAtlasStrictMode(false);
		expect(users().whereRaw("1 = 1").toSQL().sql).toContain("(1 = 1)");
	});
});
