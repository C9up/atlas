/**
 * Real relation-loader coverage — boots an in-memory SQLite DB via the napi
 * driver (no Docker) and exercises preload + lazy hasMany/belongsTo/manyToMany
 * end-to-end. This is the path the unit mocks can't reach: the bulk of
 * ModelQuery/BaseRepository's untested branches are the relation resolvers.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseEntity,
	BaseRepository,
	BelongsTo,
	Column,
	Entity,
	HasMany,
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";

@Entity("rel_authors")
class Author extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => Book, { foreignKey: "author_id" })
	declare books: Book[];
}

@Entity("rel_books")
class Book extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare authorId: string;
	@Column() declare title: string;
	@BelongsTo(() => Author, { foreignKey: "author_id", ownerKey: "id" })
	declare author: Author;
	@ManyToMany(() => Tag, {
		pivotTable: "rel_book_tag",
		foreignKey: "book_id",
		otherKey: "tag_id",
	})
	declare tags: Tag[];
}

@Entity("rel_tags")
class Tag extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare label: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE rel_authors (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE rel_books (id TEXT PRIMARY KEY, author_id TEXT, title TEXT)",
	);
	await conn.execute("CREATE TABLE rel_tags (id TEXT PRIMARY KEY, label TEXT)");
	await conn.execute(
		"CREATE TABLE rel_book_tag (book_id TEXT, tag_id TEXT)",
	);
	await conn.execute("INSERT INTO rel_authors VALUES ('a1', 'Ada'), ('a2', 'Linus')");
	await conn.execute(
		"INSERT INTO rel_books VALUES ('b1', 'a1', 'Engines'), ('b2', 'a1', 'Looms'), ('b3', 'a2', 'Kernels')",
	);
	await conn.execute("INSERT INTO rel_tags VALUES ('t1', 'classic'), ('t2', 'tech')");
	await conn.execute(
		"INSERT INTO rel_book_tag VALUES ('b1', 't1'), ('b1', 't2'), ('b3', 't2')",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > relations against real SQLite", () => {
	it("preloads a hasMany relation (no N+1)", async () => {
		const authors = await new BaseRepository(Author, conn).query().preload("books").exec();
		const ada = authors.find((a) => a.id === "a1");
		const linus = authors.find((a) => a.id === "a2");
		expect(ada?.books.map((b) => b.title).sort()).toEqual(["Engines", "Looms"]);
		expect(linus?.books.map((b) => b.title)).toEqual(["Kernels"]);
	});

	it("preloads a belongsTo relation", async () => {
		const books = await new BaseRepository(Book, conn).query().preload("author").exec();
		const engines = books.find((b) => b.id === "b1");
		expect(engines?.author?.name).toBe("Ada");
	});

	it("preloads a manyToMany relation through the pivot", async () => {
		const books = await new BaseRepository(Book, conn).query().preload("tags").exec();
		const engines = books.find((b) => b.id === "b1");
		const looms = books.find((b) => b.id === "b2");
		expect(engines?.tags.map((t) => t.label).sort()).toEqual(["classic", "tech"]);
		expect(looms?.tags).toEqual([]);
	});

	it("withCount counts a hasMany relation", async () => {
		const authors = await new BaseRepository(Author, conn)
			.query()
			.withCount("books")
			.exec();
		const ada = authors.find((a) => a.id === "a1");
		expect(Number(ada?.getExtra("books_count"))).toBe(2);
	});

	it("whereHas filters parents by relation existence", async () => {
		// Only authors that have at least one book titled like 'K%'.
		const authors = await new BaseRepository(Author, conn)
			.query()
			.whereHas("books", (q) => q.whereLike("title", "K%"))
			.exec();
		expect(authors.map((a) => a.id)).toEqual(["a2"]);
	});

	it("lazy-loads a relation via find + entity.load()", async () => {
		const repo = new BaseRepository(Author, conn);
		const ada = await repo.find("a1");
		expect(ada).not.toBeNull();
		if (ada) await ada.load("books");
		expect(ada?.books.map((b) => b.title).sort()).toEqual(["Engines", "Looms"]);
	});
});
