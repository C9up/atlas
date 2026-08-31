/**
 * has-many `groupLimit`/`groupOrderBy` — top-N related rows PER PARENT in a
 * preload, via a `ROW_NUMBER() OVER (PARTITION BY fk ORDER BY …)` window
 * (Lucid parity). SQLite always runs; Postgres/MySQL run when their URL is set.
 * A plain `.limit()` in a preload caps the WHOLE result set — the contrast test
 * pins that difference.
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

class GPost extends BaseModel {
	@PrimaryKey() declare id: number;
	@Column() declare authorId: number;
	@Column() declare title: string;
}
class GAuthor extends BaseModel {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@HasMany(() => GPost, { foreignKey: "author_id" }) declare posts: GPost[];
}

function suite(
	label: string,
	url: string,
	d: "sqlite" | "postgres" | "mysql",
	pk: string,
) {
	(url ? describe : describe.skip)(`groupLimit ${label}`, () => {
		let c: AsyncDatabaseConnection;
		beforeAll(async () => {
			setAtlasDialect(d);
			c = await createNapiConnection(url, 1, 2);
			await c.execute("DROP TABLE IF EXISTS g_posts");
			await c.execute("DROP TABLE IF EXISTS g_authors");
			await c.execute(`CREATE TABLE g_authors (id ${pk}, name VARCHAR(50))`);
			await c.execute(
				`CREATE TABLE g_posts (id ${pk}, author_id INT, title VARCHAR(50))`,
			);
			await c.execute(
				"INSERT INTO g_authors (id,name) VALUES (1,'Ada'),(2,'Bo')",
			);
			await c.execute(
				"INSERT INTO g_posts (id,author_id,title) VALUES (1,1,'p1'),(2,1,'p2'),(3,1,'p3'),(4,1,'p4'),(5,2,'p5'),(6,2,'p6'),(7,2,'p7')",
			);
			setDb(c);
		});
		afterAll(async () => {
			await c?.execute("DROP TABLE IF EXISTS g_posts");
			await c?.execute("DROP TABLE IF EXISTS g_authors");
			clearDb(c);
			await c?.close();
		});
		it("groupLimit(2).groupOrderBy('id','desc') → top-2 per parent", async () => {
			const authors = await GAuthor.query()
				.preload("posts", (q) => q.groupLimit(2).groupOrderBy("id", "desc"))
				.orderBy("id");
			const ada = authors.find((a) => a.id === 1);
			const bo = authors.find((a) => a.id === 2);
			expect(ada?.posts.map((p) => p.id)).toEqual([4, 3]);
			expect(bo?.posts.map((p) => p.id)).toEqual([7, 6]);
		});
		it("without groupLimit, a plain .limit() caps globally (contrast)", async () => {
			const authors = await GAuthor.query()
				.preload("posts", (q) => q.limit(2).orderBy("id"))
				.orderBy("id");
			const total = authors.reduce((n, a) => n + a.posts.length, 0);
			expect(total).toBe(2); // global cap, not per-parent
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
