/**
 * A select() / having() / aggregate expression never reaches another table.
 *
 * The expression filter was a word filter: `from`, `where`, `is`, `distinct`
 * were accepted at depth zero, and `join`, `on`, `table`, `limit` or a table
 * name all "looked like an identifier". A field list taken from the request,
 * `?fields=(password) as title from users where 1 is distinct`, returned the
 * password hashes as article titles; `(table api_keys limit 1)` is a
 * sub-select without the word; a bare `current_user` returned the database
 * role. The expression is now parsed against a grammar. Runs on SQLite always,
 * and on Postgres/MySQL when ATLAS_TEST_PG_URL / ATLAS_TEST_MYSQL_URL are set.
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
import {
	clearDb,
	createDbService,
	type DbService,
	setDb,
} from "../../src/services/db.js";

class Article extends BaseModel {
	static override table = "se_articles";
	@PrimaryKey() declare id: number;
	@Column() declare title: string;
}

const TARGETS: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);
}

/** What a client sends as `?fields=…`. No comma: the model builder splits on them. */
const FIELDS = [
	"(password) as title from se_users where 1 is distinct",
	"(password) as title from se_users join se_articles on not se_articles is distinct",
	"(table se_api_keys limit 1) as title",
	"(table se_api_keys limit 1)",
	"(current_user) as title",
	"(session_user) as title",
	"length(current_user) as title",
	"(password) as title from se_users",
	"count(*) from se_users",
	"(values (1)) as title",
	"(title) is distinct",
	"upper(title/**/) as title",
];

describe.each(TARGETS)("atlas > expression grammar on %s", (dialect, url) => {
	let conn: AsyncDatabaseConnection;
	let db: DbService;
	const seen: string[] = [];

	beforeAll(async () => {
		setAtlasDialect(dialect);
		const real = await createNapiConnection(url, 1, 2);
		for (const t of ["se_articles", "se_users", "se_api_keys"]) {
			await real.execute(`DROP TABLE IF EXISTS ${t}`);
		}
		await real.execute(
			"CREATE TABLE se_articles (id integer PRIMARY KEY, title varchar(200))",
		);
		await real.execute(
			"CREATE TABLE se_users (uid integer PRIMARY KEY, email varchar(200), password varchar(200))",
		);
		await real.execute("CREATE TABLE se_api_keys (token varchar(200))");
		await real.execute(
			"INSERT INTO se_articles VALUES (1, 'hello'), (2, 'world')",
		);
		await real.execute(
			"INSERT INTO se_users VALUES (1, 'root@x.io', 'HASH-ROOT')",
		);
		await real.execute("INSERT INTO se_api_keys VALUES ('sk_live_SECRET')");
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
		setDb(conn);
		setAtlasStrictMode(true);
	});

	afterAll(async () => {
		setAtlasStrictMode(false);
		for (const t of ["se_articles", "se_users", "se_api_keys"]) {
			await conn.execute(`DROP TABLE IF EXISTS ${t}`);
		}
		clearDb(conn);
		await conn.close();
		setAtlasDialect("sqlite");
	});

	it.each(FIELDS)(
		"select(%j) is refused and nothing is sent",
		async (fields) => {
			setAtlasDialect(dialect);
			seen.length = 0;
			const refusal = expect.objectContaining({
				code: expect.stringMatching(
					/^E_(UNSAFE_EXPRESSION|INJECTION_PATTERN)$/,
				),
			});
			await expect(Article.query().select(fields).exec()).rejects.toEqual(
				refusal,
			);
			await expect(
				Promise.resolve().then(() => db.from("se_articles").select(fields)),
			).rejects.toEqual(refusal);
			expect(seen).toEqual([]);
		},
	);

	it("the { alias: expression } form goes through the same grammar", async () => {
		setAtlasDialect(dialect);
		seen.length = 0;
		for (const expression of [
			"(table se_api_keys limit 1)",
			"(current_user)",
			"(password) from se_users where 1 is distinct",
		]) {
			await expect(
				Article.query().select({ title: expression }).exec(),
			).rejects.toEqual(
				expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
			);
			await expect(
				Promise.resolve().then(() =>
					db.from("se_articles").select({ title: expression }),
				),
			).rejects.toEqual(
				expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
			);
		}
		expect(seen).toEqual([]);
	});

	it("an aggregate takes a column, never the rest of an expression", () => {
		const probe =
			"id) + (case when true = ((1, 'root@x.io', 'H') <= (table se_users order by uid limit 1)) then 1000 else 0 end";
		for (const column of [probe, `${probe} as total`, "id) + (1", "id + 1"]) {
			expect(() => db.from("se_articles").sum(column)).toThrow(
				expect.objectContaining({ code: "E_UNSAFE_IDENTIFIER" }),
			);
			expect(() => db.from("se_articles").countDistinct(column)).toThrow(
				expect.objectContaining({ code: "E_UNSAFE_IDENTIFIER" }),
			);
		}
		expect(() => db.from("se_articles").sum("id as (table se_users)")).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_ALIAS" }),
		);
	});

	it("having(): the column is no oracle on another table", async () => {
		setAtlasDialect(dialect);
		seen.length = 0;
		for (const column of [
			"(table se_api_keys limit 1)",
			"count(*) from se_users",
			"(current_user)",
		]) {
			await expect(
				Promise.resolve().then(() =>
					db
						.from("se_articles")
						.select("id")
						.groupBy("id")
						.having(column, ">=", "sk"),
				),
			).rejects.toEqual(
				expect.objectContaining({ code: "E_UNSAFE_EXPRESSION" }),
			);
		}
		expect(seen).toEqual([]);
	});

	it("still runs the expressions the guides show", async () => {
		setAtlasDialect(dialect);
		const num = (row: unknown, key: string): number =>
			Number(Reflect.get(Object(row), key));

		const counted = await db.from("se_articles").select("count(*) as n");
		expect(num(counted[0], "n")).toBe(2);

		const summed = await db
			.from("se_articles")
			.select("coalesce(sum(id), 0) as total", "max(id) - min(id) as spread");
		expect(num(summed[0], "total")).toBe(3);
		expect(num(summed[0], "spread")).toBe(1);

		const cased = await db
			.from("se_articles")
			.select(
				"id",
				"(case when id > 1 then 'late' else 'early' end) as slot",
				"upper(title) as shout",
				"length(title) * 2 as twice",
			)
			.orderBy("id");
		expect(cased.map((r) => Reflect.get(Object(r), "slot"))).toEqual([
			"early",
			"late",
		]);
		expect(Reflect.get(Object(cased[0]), "shout")).toBe("HELLO");
		expect(num(cased[0], "twice")).toBe(10);

		const filtered = await db
			.from("se_articles")
			.select("title", "count(*) as n")
			.groupBy("title")
			.having("count(*)", ">", 0)
			.having("sum(id) / count(*)", ">=", 2);
		expect(filtered.map((r) => Reflect.get(Object(r), "title"))).toEqual([
			"world",
		]);

		const aggregated = await db.from("se_articles").sum("id as total");
		expect(num(aggregated[0], "total")).toBe(3);

		const mapped = await db
			.from("se_articles")
			.select({ n: "count(distinct title)" });
		expect(num(mapped[0], "n")).toBe(2);

		const models = await Article.query()
			.select(["id", "lower(title) as title"])
			.orderBy("id")
			.exec();
		expect(models.map((m) => m.title)).toEqual(["hello", "world"]);
	});

	it("reads a quoted keyword as a column of that name", () => {
		const quoted = dialect === "mysql" ? "`current_user`" : '"current_user"';
		expect(
			db.from("se_articles").select(`length(${quoted}) as n`).toSQL().sql,
		).toContain(`length(${quoted}) as n`);
	});
});
