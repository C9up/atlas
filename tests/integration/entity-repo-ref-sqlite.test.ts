/**
 * A persisted instance returned by create/save/createMany/saveMany must carry
 * its repo back-reference — so `entity.related()` / `refresh()` / `load*()` work
 * without re-fetching (AdonisJS Lucid parity: a model you just created knows its
 * query client). Before the fix only find/query (#hydrate) wired it, so
 * `(await User.create(...)).related('x')` threw "requires the entity to be
 * hydrated".
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, HasMany, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

/** The row at `index`, which the test expects the result to contain. */
function at<T>(rows: readonly T[], index: number): T {
	const row = rows[index];
	if (row === undefined) throw new Error(`expected a row at index ${index}`);
	return row;
}

/** The first row of a result the query is expected to return at least one of. */
function first<T>(rows: readonly T[]): T {
	const [row] = rows;
	if (row === undefined) throw new Error("expected at least one row");
	return row;
}

class RrBook extends BaseModel {
	static override table = "rr_books";
	@PrimaryKey() declare id: string;
	@Column() declare authorId: string;
}

class RrAuthor extends BaseModel {
	static override table = "rr_authors";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => RrBook, { foreignKey: "author_id" })
	declare books: RrBook[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE rr_authors (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE rr_books (id TEXT PRIMARY KEY, author_id TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM rr_books");
	await conn.execute("DELETE FROM rr_authors");
});

describe("atlas > persisted instances carry the repo back-ref (Lucid parity)", () => {
	it("create(): related() + refresh() work on the returned instance", async () => {
		const author = await RrAuthor.create({ id: "a1", name: "Ada" });
		// related() no longer throws — and it actually works end-to-end.
		await author.related("books").create({ id: "b1" });
		expect((await RrBook.find("b1"))?.authorId).toBe("a1");
		// refresh() re-reads through the same back-ref.
		await author.refresh();
		expect(author.name).toBe("Ada");
	});

	it("save() on a brand-new instance wires the back-ref", async () => {
		const author = new RrAuthor();
		author.id = "a2";
		author.name = "Linus";
		await RrAuthor.$repo().save(author);
		await author.related("books").create({ id: "b2" });
		expect((await RrBook.find("b2"))?.authorId).toBe("a2");
	});

	it("createMany() wires the back-ref on every instance", async () => {
		const pair = await RrAuthor.$repo().createMany([
			{ id: "a3", name: "Grace" },
			{ id: "a4", name: "Edsger" },
		]);
		const x = first(pair);
		const y = at(pair, 1);
		await x.related("books").create({ id: "b3" });
		await y.related("books").create({ id: "b4" });
		expect((await RrBook.find("b3"))?.authorId).toBe("a3");
		expect((await RrBook.find("b4"))?.authorId).toBe("a4");
	});

	it("saveMany() wires the back-ref on the returned instances", async () => {
		const a = new RrAuthor();
		a.id = "a5";
		a.name = "Barbara";
		const saved = first(await RrAuthor.$repo().saveMany([a]));
		await saved.related("books").create({ id: "b5" });
		expect((await RrBook.find("b5"))?.authorId).toBe("a5");
	});

	// firstOrCreate/updateOrCreate/*Many run in #inManagedTx — their result was
	// created on the trx-bound repo, so without the durable re-attach the returned
	// instance's related()/refresh() would hit "transaction already finished".
	it("firstOrCreate() wires the back-ref on the returned instance", async () => {
		const author = await RrAuthor.$repo().firstOrCreate(
			{ id: "a6" },
			{ name: "Ken" },
		);
		await author.related("books").create({ id: "b6" });
		expect((await RrBook.find("b6"))?.authorId).toBe("a6");
	});

	it("updateOrCreate() wires the back-ref on the returned instance", async () => {
		const author = await RrAuthor.$repo().updateOrCreate(
			{ id: "a7" },
			{ name: "Dennis" },
		);
		await author.related("books").create({ id: "b7" });
		expect((await RrBook.find("b7"))?.authorId).toBe("a7");
	});

	it("updateOrCreateMany() wires the back-ref on every returned instance", async () => {
		const x = first(
			await RrAuthor.$repo().updateOrCreateMany("id", [
				{ id: "a8", name: "Bjarne" },
			]),
		);
		await x.related("books").create({ id: "b8" });
		expect((await RrBook.find("b8"))?.authorId).toBe("a8");
	});
});
