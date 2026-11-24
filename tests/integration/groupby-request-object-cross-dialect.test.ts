/**
 * groupBy() takes column names; a raw fragment needs groupByRaw() or db.raw().
 *
 * `db.from('orders').select('status').groupBy(request.input('groupBy'))` with
 * the body `{"groupBy":{"raw":"status UNION SELECT password FROM users"}}`
 * answered the password hashes: the compiler read any object with a `raw` key
 * as SQL. Runs on SQLite always, and on Postgres/MySQL when ATLAS_TEST_PG_URL /
 * ATLAS_TEST_MYSQL_URL are set.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasStrictMode } from "../../src/ModelQuery.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";
import { QueryBuilder } from "../../src/query/QueryBuilder.js";
import { RawSql } from "../../src/query/RawSql.js";
import { createDbService, type DbService } from "../../src/services/db.js";

const TARGETS: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);
}

/** A request body, as a handler reads it: the declared type is a wish. */
function body(json: string): { groupBy: string } {
	return JSON.parse(json);
}

const PAYLOADS = [
	'{"groupBy":{"raw":"status UNION SELECT password FROM gb_users"}}',
	`{"groupBy":{"raw":"status; UPDATE gb_users SET role = 'admin'"}}`,
	'{"groupBy":{"column":"status","raw":"status UNION SELECT password FROM gb_users"}}',
	'{"groupBy":{"kind":"raw","sql":"status UNION SELECT password FROM gb_users"}}',
	'{"groupBy":{"kind":"raw","sql":"status UNION SELECT password FROM gb_users","bindings":[]}}',
	'{"groupBy":["status"]}',
	'{"groupBy":null}',
	'{"groupBy":1}',
];

describe.each(TARGETS)(
	"atlas > groupBy() request objects on %s",
	(dialect, url) => {
		let conn: AsyncDatabaseConnection;
		let db: DbService;
		const seen: string[] = [];

		beforeAll(async () => {
			setAtlasDialect(dialect);
			const real = await createNapiConnection(url, 1, 2);
			await real.execute("DROP TABLE IF EXISTS gb_orders");
			await real.execute("DROP TABLE IF EXISTS gb_users");
			await real.execute(
				"CREATE TABLE gb_orders (id integer PRIMARY KEY, status varchar(50))",
			);
			await real.execute(
				"CREATE TABLE gb_users (id integer PRIMARY KEY, password varchar(50), role varchar(50))",
			);
			await real.execute(
				"INSERT INTO gb_orders VALUES (1, 'paid'), (2, 'paid'), (3, 'sent')",
			);
			await real.execute(
				"INSERT INTO gb_users VALUES (1, 'HASH-ROOT', 'user')",
			);
			conn = new Proxy(real, {
				get(target, prop, receiver) {
					const value: unknown = Reflect.get(target, prop, receiver);
					if (prop === "query" && typeof value === "function") {
						return (sql: string, ...rest: unknown[]): unknown => {
							seen.push(sql);
							return Reflect.apply(value, target, [sql, ...rest]);
						};
					}
					return typeof value === "function" ? value.bind(target) : value;
				},
			});
			db = createDbService(() => conn);
		});

		afterAll(async () => {
			setAtlasStrictMode(false);
			await conn.execute("DROP TABLE IF EXISTS gb_orders");
			await conn.execute("DROP TABLE IF EXISTS gb_users");
			await conn.close();
			setAtlasDialect("sqlite");
		});

		it.each(PAYLOADS)("refuses %s, strict mode on or off", async (json) => {
			setAtlasDialect(dialect);
			for (const strict of [false, true]) {
				setAtlasStrictMode(strict);
				seen.length = 0;
				expect(() =>
					db.from("gb_orders").select("status").groupBy(body(json).groupBy),
				).toThrow(expect.objectContaining({ code: "E_UNSAFE_IDENTIFIER" }));
				expect(() =>
					new QueryBuilder("gb_orders", { dialect }).groupBy(
						body(json).groupBy,
					),
				).toThrow(expect.objectContaining({ code: "E_UNSAFE_IDENTIFIER" }));
				expect(seen).toEqual([]);
			}
			setAtlasStrictMode(false);
			const role = await conn.query<{ role: string }>(
				"SELECT role FROM gb_users WHERE id = 1",
			);
			expect(role[0]?.role).toBe("user");
		});

		it("never echoes the payload in the refusal", () => {
			expect(() =>
				db
					.from("gb_orders")
					.groupBy(body('{"groupBy":{"raw":"SECRET-PAYLOAD"}}').groupBy),
			).toThrow(
				expect.objectContaining({
					message: expect.not.stringContaining("SECRET-PAYLOAD"),
				}),
			);
		});

		it("groups by a column name, quoted", async () => {
			setAtlasDialect(dialect);
			const q = db.from("gb_orders").select("status").groupBy("status");
			expect(q.toSQL().sql).toMatch(/GROUP BY ["`]status["`]$/);
			expect(await q).toHaveLength(2);
			expect(
				new QueryBuilder("gb_orders", { dialect }).groupBy("status").toSQL()
					.sql,
			).toMatch(/GROUP BY ["`]status["`]$/);
		});

		it("groups by a fragment through groupByRaw() and db.raw(), with bindings", async () => {
			setAtlasDialect(dialect);
			setAtlasStrictMode(false);
			const viaRaw = await db
				.from("gb_orders")
				.select(new RawSql("lower(status) as s"))
				.groupByRaw("lower(status)");
			expect(viaRaw).toHaveLength(2);
			const viaFragment = db
				.from("gb_orders")
				.select(new RawSql("count(*) as n"))
				.groupBy("status", new RawSql("coalesce(status, ?)", ["none"]));
			expect(viaFragment.toSQL().params).toEqual(["none"]);
			expect(await viaFragment).toHaveLength(2);
			// A clone keeps the fragment a fragment.
			expect(await viaFragment.clone()).toHaveLength(2);
		});

		it("gates both fragment forms behind strict mode", () => {
			setAtlasStrictMode(true);
			expect(() => db.from("gb_orders").groupByRaw("lower(status)")).toThrow(
				/strict mode/i,
			);
			expect(() =>
				db.from("gb_orders").groupBy(new RawSql("lower(status)")),
			).toThrow(/strict mode/i);
			setAtlasStrictMode(false);
		});

		it("keeps a raw ORDER BY term in its place, through a clone too", async () => {
			setAtlasDialect(dialect);
			setAtlasStrictMode(false);
			const q = db
				.from("gb_orders")
				.select("id")
				.orderBy("status", "desc")
				.orderByRaw("coalesce(id, ?) DESC", [0])
				.orderBy("id");
			expect(q.toSQL().sql).toMatch(
				/ORDER BY ["`]status["`] DESC, coalesce\(id, (\?|\$1)\) DESC, ["`]id["`] ASC$/,
			);
			const rows = await q.clone();
			expect(rows.map((r) => Number(Reflect.get(Object(r), "id")))).toEqual([
				3, 2, 1,
			]);
		});
	},
);
