/**
 * ModelQuery source-compat surface added for Lucid parity: `query.model`,
 * `sideload` (replace by default, merge with the 2nd arg) + preload propagation,
 * `ifDialect`/`unlessDialect`, `comment`, `toNative`, and `toSQL().bindings`.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, HasMany, PrimaryKey } from "../../src/index.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { RawSql } from "../../src/query/QueryBuilder.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Post extends BaseModel {
	@PrimaryKey() declare id: number;
	@Column() declare authorId: number;
	@Column() declare title: string;
}

class Author extends BaseModel {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@HasMany(() => Post, { foreignKey: "author_id" })
	declare posts: Post[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)",
	);
	await conn.execute(
		"INSERT INTO authors VALUES (1, 'Ada'), (2, 'Bob'), (3, 'Cy')",
	);
	await conn.execute("INSERT INTO posts VALUES (1, 1, 'Engines')");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > ModelQuery surface (Lucid)", () => {
	it("orderBy(array) / distinct(cols) / union(callback) / having helpers (Lucid)", async () => {
		// orderBy(array) with mixed forms.
		const ordered = await Author.query().orderBy([
			{ column: "name", order: "desc" },
		]);
		expect(ordered.map((a) => a.name)).toEqual(["Cy", "Bob", "Ada"]);

		// distinct(cols) sets DISTINCT + projects the columns.
		expect(Author.query().distinct("name").toSQL().sql).toContain(
			'SELECT DISTINCT "name"',
		);

		// union(callback) builds the branch on the same model.
		const u = await Author.query()
			.where("id", 1)
			.union((q) => q.where("id", 2));
		expect(u.map((a) => a.id).sort()).toEqual([1, 2]);

		// having helpers compile IN / BETWEEN.
		const sql = Author.query()
			.select("name")
			.groupBy("name")
			.havingBetween("COUNT(*)", [1, 5])
			.havingIn("name", ["Ada", "Bob"])
			.toSQL().sql;
		expect(sql).toContain("BETWEEN ? AND ?");
		expect(sql).toContain("IN (?, ?)");
	});

	it("query.model exposes the model class", () => {
		expect(Author.query().model).toBe(Author);
	});

	it("toSQL() returns bindings + params; toNative() returns { sql, bindings }", () => {
		const q = Author.query().where("id", 1);
		const sql = q.toSQL();
		expect(sql.bindings).toEqual([1]);
		expect(sql.params).toEqual([1]);
		expect(q.toNative()).toEqual({ sql: sql.sql, bindings: [1] });
	});

	it("comment() prefixes a /* */ SQL comment; rejects the terminator", () => {
		expect(Author.query().comment("report").toSQL().sql).toContain(
			"/* report */",
		);
		expect(() => Author.query().comment("x */ y")).toThrow(/\*\//);
	});

	it("ifDialect/unlessDialect gate by the active dialect", () => {
		const sql = Author.query()
			.ifDialect("sqlite", (q) => q.orderBy("id", "desc"))
			.unlessDialect("sqlite", (q) => q.orderBy("name"))
			.toSQL().sql;
		expect(sql).toContain('ORDER BY "id" DESC');
		expect(sql).not.toContain('"name"');
	});

	it("sideload replaces by default, merges with the 2nd arg, and reaches preloads", async () => {
		// Replace (default): the second sideload wins wholesale.
		const [a1] = await Author.query()
			.sideload({ a: 1 })
			.sideload({ b: 2 })
			.exec();
		expect(a1.$sideloaded).toEqual({ b: 2 });

		// Merge (2nd arg true) + propagation to preloaded posts.
		const [a2] = await Author.query()
			.sideload({ tenant: 7 })
			.sideload({ role: "admin" }, true)
			.preload("posts")
			.exec();
		expect(a2.$sideloaded).toEqual({ tenant: 7, role: "admin" });
		expect(a2.posts[0]?.$sideloaded).toEqual({ tenant: 7, role: "admin" });
	});
});

describe("atlas > ModelQuery — DB-builder surface parity (Lucid)", () => {
	it("select: multiple args, object map, raw fragment, named subquery", () => {
		// Multiple string args + object alias map.
		expect(
			Author.query().select("id", "name", { label: "name" }).toSQL().sql,
		).toContain('"name" AS "label"');
		// Raw fragment with its own bindings.
		const raw = Author.query()
			.select("id")
			.select(new RawSql("? AS marker", ["x"]));
		expect(raw.toSQL()).toMatchObject({ params: ["x"] });
		// Named subquery column — select(subquery.as('alias')).
		const withSub = Author.query()
			.select("name")
			.select(Post.query().select("COUNT(*)").as("post_count"));
		expect(withSub.toSQL().sql).toContain(') AS "post_count"');
	});

	it("join(4-arg) + join()/leftOuterJoin aliases", () => {
		expect(
			Author.query().join("posts", "authors.id", "=", "posts.author_id").toSQL()
				.sql,
		).toContain('INNER JOIN "posts" ON "authors"."id" = "posts"."author_id"');
		expect(
			Author.query()
				.leftOuterJoin("posts", "authors.id", "posts.author_id")
				.toSQL().sql,
		).toContain('LEFT JOIN "posts"');
	});

	it("join callback ON family: onIn/onNotNull/onBetween/onExists", () => {
		const sql = Author.query()
			.leftJoin("posts", (j) =>
				j
					.on("authors.id", "posts.author_id")
					.onIn("posts.id", [1, 2])
					.onNotNull("posts.title")
					.onBetween("posts.id", [1, 10])
					.onExists((q) => q.where("id", 1)),
			)
			.toSQL();
		expect(sql.sql).toContain('AND "posts"."id" IN (?, ?)');
		expect(sql.sql).toContain('AND "posts"."title" IS NOT NULL');
		expect(sql.sql).toContain('AND "posts"."id" BETWEEN ? AND ?');
		expect(sql.sql).toContain("AND EXISTS (");
	});

	it("CTE with(callback) + withRecursive(callback, columns)", () => {
		expect(
			Author.query()
				.with("recent", (q) => q.where("id", ">", 1))
				.toSQL().sql,
		).toContain('WITH "recent" AS (');
		expect(
			Author.query()
				.withRecursive("tree", (q) => q.where("id", 1), ["id"])
				.toSQL().sql,
		).toContain('WITH RECURSIVE "tree"("id") AS (');
	});

	it("carries a CTE onto a ModelQuery DML (with().delete()) without breaking params", async () => {
		// Non-destructive: id 999 doesn't exist. Proves the CTE + its param (99)
		// are threaded before the DELETE WHERE param without corrupting the query.
		const affected = await Author.query()
			.with("recent", (q) => q.where("id", 99))
			.where("id", 999)
			.delete();
		expect(affected).toBe(0);
	});

	it("timeout(ms) keeps a fast query working; timeout() clears it", async () => {
		const rows = await Author.query().timeout(5000).orderBy("id");
		expect(rows).toHaveLength(3);
		expect(await Author.query().timeout().count()).toBe(3);
	});

	it("lazy DML: Model.query().update(...).toSQL() + .returning() after (Lucid)", async () => {
		// Inspect without executing.
		const q = Author.query().where("id", 1).update({ name: "Zed" });
		expect(q.toSQL().sql).toContain('UPDATE "authors" SET');
		expect(await Author.query().where("id", 1).count()).toBe(1); // not run yet
		// returning() chains AFTER update (Lucid order); run then revert.
		const rows = await Author.query()
			.where("id", 1)
			.update({ name: "Zed" })
			.returning(["id", "name"]);
		expect(rows).toEqual([{ id: 1, name: "Zed" }]);
		await Author.query().where("id", 1).update({ name: "Ada" }); // revert
	});
});
