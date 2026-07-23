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
