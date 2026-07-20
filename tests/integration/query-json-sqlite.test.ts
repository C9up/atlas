/**
 * JSON where predicates (whereJsonPath / whereJsonSupersetOf / whereJsonSubsetOf
 * + and/or/not) — none of which existed in either layer.
 *
 * whereJsonPath runs for real against SQLite's json_extract, which is the point:
 * a query that compiles but extracts the wrong node would still pass a string
 * assertion. Containment is Postgres/MySQL syntax (SQLite has no operator for
 * it), so those cases are asserted at the SQL level and the SQLite refusal is
 * executed.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("docs")
class Doc extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare data: string;
}

function repo(
	conn: AsyncDatabaseConnection,
	dialect: "sqlite" | "postgres" | "mysql" = "sqlite",
) {
	return new BaseRepository(Doc, conn, { dialect });
}

describe("whereJsonPath (sqlite, executed)", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)",
			[],
		);
		for (const [id, data] of [
			[1, '{"name":"ada","address":{"city":"London"},"tags":["a","b"]}'],
			[2, '{"name":"bob","address":{"city":"Paris"},"tags":["b"]}'],
			[3, '{"name":"cid","age":40}'],
		] as const) {
			await conn.execute("INSERT INTO docs (id, data) VALUES (?, ?)", [
				id,
				data,
			]);
		}
	});

	it("matches a nested string value", async () => {
		const rows = await repo(conn)
			.query()
			.whereJsonPath("data", "$.address.city", "=", "Paris")
			.exec();
		expect(rows.map((d) => d.id)).toEqual([2]);
	});

	it("matches a numeric value with a comparison operator", async () => {
		const rows = await repo(conn)
			.query()
			.whereJsonPath("data", "$.age", ">", 30)
			.exec();
		expect(rows.map((d) => d.id)).toEqual([3]);
	});

	it("indexes into an array", async () => {
		const rows = await repo(conn)
			.query()
			.whereJsonPath("data", "$.tags[0]", "=", "a")
			.exec();
		expect(rows.map((d) => d.id)).toEqual([1]);
	});

	it("composes with OR and normal predicates", async () => {
		const rows = await repo(conn)
			.query()
			.where("id", 1)
			.orWhereJsonPath("data", "$.name", "=", "bob")
			.exec();
		expect(rows.map((d) => d.id).sort()).toEqual([1, 2]);
	});

	it("binds the value — it never lands in the SQL string", () => {
		const { sql, params } = repo(conn)
			.query()
			.whereJsonPath("data", "$.name", "=", "ada")
			.toSQL();
		expect(sql).not.toContain("ada");
		expect(sql).not.toContain("$.name");
		expect(params).toEqual(["$.name", "ada"]);
	});

	it("rejects a path that doesn't start at the root", () => {
		expect(() =>
			repo(conn).query().whereJsonPath("data", "name", "=", "x"),
		).toThrow(/must start with '\$'/);
	});

	it("resolves the column through the entity map", () => {
		// A column typo is caught like any other entry point.
		expect(() =>
			repo(conn).query().whereJsonPath("nope", "$.a", "=", 1),
		).toThrow(/does not exist/);
	});
});

describe("JSON containment (SQL per dialect)", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)",
			[],
		);
	});

	it("emits @> / <@ on postgres", () => {
		const sup = repo(conn, "postgres")
			.query()
			.whereJsonSupersetOf("data", ["a"])
			.toSQL();
		expect(sup.sql).toContain('"data"::jsonb @> $1::jsonb');

		const sub = repo(conn, "postgres")
			.query()
			.whereJsonSubsetOf("data", ["a", "b"])
			.toSQL().sql;
		expect(sub).toContain('"data"::jsonb <@ $1::jsonb');
	});

	it("emits JSON_CONTAINS on mysql, flipping args for subset", () => {
		expect(
			repo(conn, "mysql").query().whereJsonSupersetOf("data", ["a"]).toSQL()
				.sql,
		).toContain("JSON_CONTAINS(`data`, ?)");
		expect(
			repo(conn, "mysql").query().whereJsonSubsetOf("data", ["a"]).toSQL().sql,
		).toContain("JSON_CONTAINS(?, `data`)");
	});

	it("wraps the negated forms in NOT (...)", () => {
		expect(
			repo(conn, "postgres")
				.query()
				.whereNotJsonSupersetOf("data", ["a"])
				.toSQL().sql,
		).toContain('NOT ("data"::jsonb @> $1::jsonb)');
	});

	it("refuses containment on SQLite, which has no operator for it", () => {
		expect(() =>
			repo(conn, "sqlite").query().whereJsonSupersetOf("data", ["a"]).toSQL(),
		).toThrow(/E_UNSUPPORTED/);
		expect(() =>
			repo(conn, "sqlite").query().whereJsonSubsetOf("data", ["a"]).toSQL(),
		).toThrow(/E_UNSUPPORTED/);
	});
});
