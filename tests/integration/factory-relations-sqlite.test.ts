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
import { Factory, factory } from "../../src/testing/Factory.js";

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
		pivotColumns: ["featured"],
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
		"CREATE TABLE f_book_tag (book_id INTEGER, tag_id INTEGER, featured INTEGER)",
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

describe("atlas > factory relations > nested with", () => {
	it("applies a nested .with() inside the callback (Lucid nested factories)", async () => {
		// Author → 2 books, and EACH book → 3 tags (nested with).
		const author = await AuthorFactory.with("books", 2, (book) =>
			book.with("tags", 3),
		).create(conn);

		expect(await count("f_books", `WHERE author_id = ${author.id}`)).toBe(2);
		// 2 books × 3 tags = 6 tag rows and 6 pivot links.
		expect(await count("f_tags")).toBe(6);
		expect(await count("f_book_tag")).toBe(6);
		// Each book got exactly its own 3 pivot links.
		const books = await conn.query<{ id: number }>(
			`SELECT id FROM f_books WHERE author_id = ${author.id}`,
		);
		for (const b of books) {
			expect(await count("f_book_tag", `WHERE book_id = ${b.id}`)).toBe(3);
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

	it("pivotAttributes() writes extra pivot columns (Lucid)", async () => {
		const book = await BookFactory.with("tags", 2, (tag) =>
			tag.pivotAttributes({ featured: 1 }),
		).create(conn);

		// Both pivot rows carry the pivot attribute.
		expect(
			await count("f_book_tag", `WHERE book_id = ${book.id} AND featured = 1`),
		).toBe(2);
	});

	it("pivotAttributes(array) sets different values per row (Lucid)", async () => {
		const book = await BookFactory.with("tags", 2, (tag) =>
			tag.pivotAttributes([{ featured: 1 }, { featured: 0 }]),
		).create(conn);

		expect(
			await count("f_book_tag", `WHERE book_id = ${book.id} AND featured = 1`),
		).toBe(1);
		expect(
			await count("f_book_tag", `WHERE book_id = ${book.id} AND featured = 0`),
		).toBe(1);
	});

	it("pivotAttributes(array) rejects a length mismatch (Lucid strict)", async () => {
		// Too short: 1 pivot entry for 2 rows.
		await expect(
			BookFactory.with("tags", 2, (tag) =>
				tag.pivotAttributes([{ featured: 1 }]),
			).create(conn),
		).rejects.toThrow(/length \(1\) must match the related-row count \(2\)/);

		// Too long: 3 pivot entries for 2 rows.
		await expect(
			BookFactory.with("tags", 2, (tag) =>
				tag.pivotAttributes([
					{ featured: 1 },
					{ featured: 0 },
					{ featured: 1 },
				]),
			).create(conn),
		).rejects.toThrow(/length \(3\) must match the related-row count \(2\)/);
	});
});

describe("atlas > factory relations > atomicity", () => {
	it("rolls the parent back when a related write fails (Lucid managed trx)", async () => {
		// A tag factory that fails on persist — the whole graph must roll back.
		const BoomTags = factory(FTag, ({ faker }) => ({
			label: faker.lorem.word(),
		})).after("make", () => {
			throw new Error("boom");
		});
		const AuthorWithBadBook = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		})).relation("books", () => BadBookFactory);
		const BadBookFactory = factory(FBook, ({ faker }) => ({
			title: faker.lorem.words(2),
			published: false,
		})).relation("tags", () => BoomTags);

		await expect(
			AuthorWithBadBook.with("books", 1, (b) => b.with("tags", 1)).create(conn),
		).rejects.toThrow("boom");

		// Parent author + book both rolled back — nothing persisted.
		expect(await count("f_authors")).toBe(0);
		expect(await count("f_books")).toBe(0);
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

	it("tap receives (model, ctx, builder); newUp replaces instantiation (Lucid)", async () => {
		const args: Array<{
			name: string;
			stubbed: boolean;
			sameBuilder: boolean;
		}> = [];
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		}))
			// newUp: custom instantiation — force a known name regardless of defaults.
			.newUp((attrs) => {
				const a = new FAuthor();
				a.name = "NewUpped";
				a.id = typeof attrs.id === "number" ? attrs.id : 0;
				return a;
			})
			.tap((m, ctx, b) => {
				args.push({
					name: m.name,
					stubbed: ctx.isStubbed,
					sameBuilder: b === f,
				});
			});

		const made = f.make();
		expect(made.name).toBe("NewUpped");
		expect(args).toEqual([
			{ name: "NewUpped", stubbed: false, sameBuilder: true },
		]);
	});

	it("merge(callback) receives (model, attributes, ctx) on the instance (Lucid)", () => {
		let seenModel: unknown;
		let seenAttrs: Record<string, unknown> | undefined;
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		})).merge((model, attributes, ctx) => {
			seenModel = model;
			seenAttrs = attributes;
			expect(ctx.isStubbed).toBe(false);
			model.name = "Merged";
		});

		const a = f.make();
		expect(seenModel).toBeInstanceOf(FAuthor); // the model INSTANCE, not data
		expect(typeof seenAttrs?.name).toBe("string"); // the resolved attributes
		expect(a.name).toBe("Merged");
	});

	it("merge(array) overrides per row on makeMany (Lucid)", () => {
		const authors = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		}))
			.merge([{ name: "First" }, { name: "Second" }])
			.makeMany(3);
		// index 0 → First, index 1 → Second, index 2 → plain default (a name).
		expect(authors[0].name).toBe("First");
		expect(authors[1].name).toBe("Second");
		expect(typeof authors[2].name).toBe("string");
		expect(authors[2].name).not.toBe("First");
	});

	it("state() receives the model INSTANCE, not a data object (Lucid)", () => {
		let received: unknown;
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		})).state("renamed", (model) => {
			received = model;
			model.name = "Renamed";
		});

		const a = f.apply("renamed").make();
		expect(a).toBeInstanceOf(FAuthor);
		expect(received).toBeInstanceOf(FAuthor); // the callback got the instance
		expect(a.name).toBe("Renamed");
	});

	it("create(db) exposes the explicit connection as ctx.$trx (Lucid)", async () => {
		let seen: unknown;
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		})).before("create", (_, _m, ctx) => {
			seen = ctx.$trx;
		});
		// No .client() bind — the connection passed to create() must reach ctx.$trx.
		await f.create(conn);
		expect(seen).toBe(conn);
	});

	it("Factory.query({ client }) binds the connection (Lucid)", async () => {
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		}));
		// No explicit db arg — the client bound via query() is used.
		const a = await f.query({ client: conn }).create();
		expect(await count("f_authors", `WHERE id = ${a.id}`)).toBe(1);
	});

	it("Factory.stubId overrides the stubbed primary key generator (Lucid)", () => {
		Factory.stubId((counter) => `stub-${counter}`);
		try {
			const a = factory(FAuthor, () => ({ name: "x" })).makeStubbed();
			expect(String(a.id)).toMatch(/^stub-\d+$/);
		} finally {
			// Restore the default so other tests keep integer stub ids.
			Factory.stubId();
		}
	});

	it("after('make') fires on an un-persisted make()/makeMany() build (Lucid)", async () => {
		const seen: Array<{ persisted: boolean; stubbed: boolean }> = [];
		const f = factory(FAuthor, ({ faker }) => ({
			name: faker.person.fullName(),
		})).after("make", (_, m, ctx) => {
			seen.push({ persisted: m.$isPersisted, stubbed: ctx.isStubbed });
		});

		const one = f.make();
		expect(one.$isPersisted).toBe(false);
		f.makeMany(2);
		// Fired once per built instance; make() is not stubbed and not persisted.
		expect(seen).toEqual([
			{ persisted: false, stubbed: false },
			{ persisted: false, stubbed: false },
			{ persisted: false, stubbed: false },
		]);
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
