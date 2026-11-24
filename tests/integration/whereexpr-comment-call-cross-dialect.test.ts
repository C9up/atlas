/**
 * whereExpr()'s extra expression never calls a function hidden behind a
 * comment, on any engine.
 *
 * With a comment between a function name and its parenthesis, the name is a
 * bare word to a check that looks for `name(` and a call to the server. Under
 * strict mode, on PostgreSQL, `pg_sleep` held a connection that way, and
 * `query_to_xml` ran a complete SELECT on another table. Runs on SQLite
 * always, and on Postgres/MySQL when ATLAS_TEST_PG_URL / ATLAS_TEST_MYSQL_URL
 * are set.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { setAtlasStrictMode } from "../../src/ModelQuery.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Order extends BaseModel {
	static override table = "wx_orders";
	@PrimaryKey() declare id: number;
	@Column() declare total: number;
	@Column() declare tenantId: number;
}

/** What a handler builds from a client-chosen adjustment. */
const handler = (adjust: string) =>
	Order.query().where("tenantId", 1).whereExpr("total", adjust, ">", 0);

/** A string built without a quote: concat(chr(…), chr(…), …), calls hidden. */
const text = (s: string) =>
	`concat/**/(${[...s].map((c) => `chr/**/(${c.charCodeAt(0)})`).join(",")})`;

/** The functions each engine would have run, hidden behind a comment. */
const HIDDEN_CALLS: Record<AtlasDialect, string[]> = {
	sqlite: [
		"+ length(sqlite_version/**/())",
		"* 0 + length(randomblob/**/(1048576))",
	],
	postgres: [
		"+ length(concat/**/(pg_sleep/**/(2)))",
		"+ pg_sleep /* x */ (1)",
		`* 0 + length(concat/**/(query_to_xml/**/(${text("select 1 from wx_users")}, isfinite/**/(now()), isfinite/**/(now()), ${text(" ")}))) - 1`,
	],
	mysql: ["+ sleep/**/(2)", "* 0 + length(version/**/())"],
};

const TARGETS: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);
}

describe.each(TARGETS)(
	"atlas > whereExpr() hidden calls on %s",
	(dialect, url) => {
		let conn: AsyncDatabaseConnection;
		const seen: string[] = [];

		beforeAll(async () => {
			setAtlasDialect(dialect);
			const real = await createNapiConnection(url, 1, 2);
			await real.execute("DROP TABLE IF EXISTS wx_orders");
			await real.execute("DROP TABLE IF EXISTS wx_users");
			await real.execute(
				"CREATE TABLE wx_orders (id integer PRIMARY KEY, total integer, tenant_id integer)",
			);
			await real.execute(
				"CREATE TABLE wx_users (id integer PRIMARY KEY, password varchar(50))",
			);
			await real.execute("INSERT INTO wx_orders VALUES (1, 10, 1)");
			await real.execute("INSERT INTO wx_users VALUES (1, 'S3cr3t')");
			// Every statement that reaches the driver is recorded.
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
			setDb(conn);
			setAtlasStrictMode(true);
		});

		afterAll(async () => {
			setAtlasStrictMode(false);
			await conn.execute("DROP TABLE IF EXISTS wx_orders");
			await conn.execute("DROP TABLE IF EXISTS wx_users");
			clearDb(conn);
			await conn.close();
			setAtlasDialect("sqlite");
		});

		it.each(HIDDEN_CALLS[dialect])(
			"refuses %s before anything is sent",
			(adjust) => {
				setAtlasDialect(dialect);
				seen.length = 0;
				expect(() => handler(adjust)).toThrow(
					expect.objectContaining({ code: "E_ATLAS_INVALID_EXPRESSION" }),
				);
				expect(seen).toEqual([]);
			},
		);

		it("still runs plain arithmetic against the real engine", async () => {
			setAtlasDialect(dialect);
			expect(await handler("* 2 / 3").exec()).toHaveLength(1);
			expect(await handler("+ abs(tenant_id) - 100").exec()).toHaveLength(0);
			expect(await handler("* (1 + 0)").exec()).toHaveLength(1);
		});
	},
);
