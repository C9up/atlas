/**
 * Factory `.relation()` / `.with()` against real in-memory SQLite. Persists
 * parents with related rows through the relation proxies and asserts the FK /
 * pivot wiring on disk — hasMany, belongsTo, and manyToMany.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseEntity,
	BelongsTo,
	Column,
	Entity,
	HasMany,
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { factory } from "../../src/testing/Factory.js";

@Entity("f_authors")
class FAuthor extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@HasMany(() => FBook, { foreignKey: "author_id" })
	declare books: FBook[];
}

@Entity("f_books")
class FBook extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare authorId: number | null;
	@Column() declare title: string;
	@Column() declare published: boolean;
	@BelongsTo(() => FAuthor, { foreignKey: "author_id" })
	declare author: FAuthor;
	@ManyToMany(() => FTag, {
		pivotTable: "f_book_tag",
		foreignKey: "book_id",
		otherKey: "tag_id",
	})
	declare tags: FTag[];
}

@Entity("f_tags")
class FTag extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
}

const TagFactory = factory(FTag, ({ faker }) => ({
	label: faker.lorem.word(),
}));
const BookFactory = factory(FBook, ({ faker }) => ({
	title: faker.lorem.words(3),
	published: false,
}))
	.relation("author", () => AuthorFactory)
	.relation("tags", () => TagFactory);
const AuthorFactory = factory(FAuthor, ({ faker }) => ({
	name: faker.person.fullName(),
})).relation("books", () => BookFactory);

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE f_authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE f_books (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, title TEXT, published INTEGER)",
	);
	await conn.execute(
		"CREATE TABLE f_tags (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT)",
	);
	await conn.execute(
		"CREATE TABLE f_book_tag (book_id INTEGER, tag_id INTEGER)",
	);
});

afterEach(async () => {
	await conn?.close();
});

async function count(table: string, where = ""): Promise<number> {
	const rows = await conn.query<{ n: number }>(
		`SELECT COUNT(*) AS n FROM ${table} ${where}`,
	);
	return rows[0]?.n ?? 0;
}

describe("atlas > factory relations > hasMany", () => {
	it("creates N children with the parent FK wired", async () => {
		const author = await AuthorFactory.with("books", 3).create(conn);

		expect(await count("f_books")).toBe(3);
		// Every book points back at the author.
		expect(await count("f_books", `WHERE author_id = ${author.id}`)).toBe(3);
	});

	it("applies the callback's merge to every child", async () => {
		await AuthorFactory.with("books", 2, (book) =>
			book.merge({ published: true }),
		).create(conn);

		expect(await count("f_books", "WHERE published = 1")).toBe(2);
	});

	it("gives each created parent its own children in createMany", async () => {
		const authors = await AuthorFactory.with("books", 2).createMany(3, conn);
		expect(authors).toHaveLength(3);
		expect(await count("f_books")).toBe(6);
		for (const a of authors) {
			expect(await count("f_books", `WHERE author_id = ${a.id}`)).toBe(2);
		}
	});
});

describe("atlas > factory relations > belongsTo", () => {
	it("creates the owner and associates the FK on the parent", async () => {
		const book = await BookFactory.with("author").create(conn);

		expect(await count("f_authors")).toBe(1);
		const rows = await conn.query<{ author_id: number | null }>(
			`SELECT author_id FROM f_books WHERE id = ${book.id}`,
		);
		expect(rows[0]?.author_id).not.toBeNull();
	});
});

describe("atlas > factory relations > manyToMany", () => {
	it("creates related rows and the pivot links", async () => {
		const book = await BookFactory.with("tags", 2).create(conn);

		expect(await count("f_tags")).toBe(2);
		expect(await count("f_book_tag", `WHERE book_id = ${book.id}`)).toBe(2);
	});
});

describe("atlas > factory relations > errors", () => {
	it("throws when .with() names an undeclared relation", async () => {
		await expect(AuthorFactory.with("ghosts", 1).create(conn)).rejects.toThrow(
			/is not a declared relation/,
		);
	});

	it("throws when a relation has no registered factory", async () => {
		const bare = factory(FAuthor, () => ({ name: "x" }));
		await expect(bare.with("books", 1).create(conn)).rejects.toThrow(
			/no related factory/,
		);
	});

	it("a .with() consumed by make() does not leak into the next create()", async () => {
		// make() ignores relations — and must also CLEAR the queue, so the next
		// create() doesn't get surprise children.
		AuthorFactory.with("books", 3).make();
		const author = await AuthorFactory.create(conn);

		expect(await count("f_books", `WHERE author_id = ${author.id}`)).toBe(0);
		expect(await count("f_books")).toBe(0);
	});

	it("before/after('create') hooks run around the INSERT (Lucid)", async () => {
		const log: string[] = [];
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		}))
			.before("create", (_, m) => {
				m.name = "Hooked";
				log.push("before");
			})
			.after("create", (_, m) => {
				log.push(`after:${m.$isPersisted}`);
			});

		const author = await f.create(conn);
		expect(author.name).toBe("Hooked");
		expect(log).toEqual(["before", "after:true"]);
		// The before-hook value was the one persisted.
		const rows = await conn.query<{ name: string }>(
			`SELECT name FROM f_authors WHERE id = ${author.id}`,
		);
		expect(rows[0]?.name).toBe("Hooked");
	});
});
