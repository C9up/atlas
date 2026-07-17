import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	Column,
	Entity,
	HasMany,
	PrimaryKey,
	SoftDeletes,
	setAtlasStrictMode,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Order extends BaseModel {
	static override table = "orders";
	@PrimaryKey() declare id: string;
	@Column() declare userId: string;
	@Column() declare status: string;
	@Column() declare region: string;
}

// A soft-delete model — `restore()`'s #assertPersistedRow guard only runs on
// one (it early-returns as a no-op otherwise).
@SoftDeletes()
class SdOrder extends BaseModel {
	static override table = "sd_orders";
	@PrimaryKey() declare id: string;
	@Column() declare deletedAt: string | null;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, tier TEXT)");
	await conn.execute(
		"CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT, status TEXT, region TEXT)",
	);
	await conn.execute("INSERT INTO users VALUES ('u1','gold'),('u2','silver')");
	await conn.execute(
		"INSERT INTO orders VALUES ('o1','u1','paid','eu'),('o2','u1','pending','eu'),('o3','u2','paid','us')",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > join onVal binds a value end-to-end (NAPI param channel)", () => {
	it("onVal filters in the JOIN and its param is ordered before the WHERE param", async () => {
		// JOIN param 'paid' ($1) comes before WHERE param 'eu' ($2) — the whole
		// point of threading join params through the compiler before the WHERE.
		const rows = await Order.query()
			.innerJoin("users", (j) =>
				j.on("users.id", "orders.user_id").andOnVal("orders.status", "paid"),
			)
			.where("region", "eu")
			.exec();
		// Only o1 matches: paid (bound in the JOIN) AND region eu (bound in the
		// WHERE). o2 is pending, o3 is us. Exactly one row proves BOTH params bound
		// correctly and in the right order.
		expect(rows.length).toBe(1);
		// `id` is NO LONGER clobbered by users.id: with a join + default select the
		// projection is scoped to orders' own columns, so the model hydrates cleanly.
		expect(rows[0].id).toBe("o1");
		expect(rows[0].status).toBe("paid");
		expect(rows[0].region).toBe("eu");
		expect(rows[0].userId).toBe("u1");
	});

	it("select() including a joined table's id hydrates the base PK, not the joined one (P1)", async () => {
		// Explicitly select the JOINED table's `id` (users.id) alongside a partial
		// projection. Before the fix, `users.id` satisfied the PK-present check (its
		// leaf equals the PK name), so `orders.id` was NOT auto-added and the joined
		// `id` clobbered the entity → Order.id = 'u1' (a users row → a later save()
		// would UPDATE the wrong table's key). Now the base PK is appended last and
		// wins the duplicate result key.
		const rows = await Order.query()
			.innerJoin("users", (j) => j.on("users.id", "orders.user_id"))
			.select(["users.tier", "users.id"])
			.exec();
		// Every hydrated id is an ORDER id (o…), never a joined users id (u…).
		expect(rows.map((r) => r.id).sort()).toEqual(["o1", "o2", "o3"]);
	});

	it("onVal on a schema-qualified base table (short form) keeps the custom @Column adapter (P3a)", () => {
		class Widget extends BaseModel {
			static override table = "main.widgets";
			@PrimaryKey() declare id: string;
			@Column({ prepare: (v: unknown) => `P:${String(v)}` })
			declare code: string;
		}
		// `widgets.code` (short form) references the `main.widgets` model's OWN column.
		// It must be recognised as base — so the custom prepare adapter runs — even
		// though the stored table name is schema-qualified. Compile-only (no exec).
		const { params } = Widget.query()
			.innerJoin("x", (j) =>
				j.on("x.id", "main.widgets.id").andOnVal("widgets.code", "abc"),
			)
			.toSQL();
		expect(params).toContain("P:abc");
	});

	it("onVal on a schema-qualified FOREIGN table (unqualified root model) does NOT apply the root adapter (P2)", () => {
		@Entity("gadgets")
		class Gadget extends BaseModel {
			@PrimaryKey() declare id: string;
			@Column({ prepare: (v: unknown) => `P:${String(v)}` })
			declare code: string;
		}
		// Root model is UNQUALIFIED (`gadgets`). A ref that ADDS a schema
		// (`archive.gadgets.code`) names a different table the model never claimed —
		// it must stay foreign, so the root's prepare adapter is NOT applied.
		const { params } = Gadget.query()
			.innerJoin("x", (j) =>
				j.on("x.id", "gadgets.id").andOnVal("archive.gadgets.code", "abc"),
			)
			.toSQL();
		expect(params).toContain("abc");
		expect(params).not.toContain("P:abc");
	});

	it("raw() is disabled in strict mode and points at the break-glass (P2)", async () => {
		setAtlasStrictMode(true);
		try {
			await expect(Order.$repo().raw("SELECT 1")).rejects.toThrow(
				/strict mode/i,
			);
		} finally {
			setAtlasStrictMode(false);
		}
	});

	it("refresh() on an aggregate projection fails loud with a clear PK error (P3b)", async () => {
		// An aggregate/alias projection hydrates a persisted entity with no PK.
		// refresh() must not silently query WHERE pk IS NULL — it fails loud with
		// the missing-primary-key diagnostic (parity with save()).
		const [row] = await Order.query().select("COUNT(*) as n").exec();
		await expect(row.refresh()).rejects.toThrow(/primary key/i);
	});

	it("delete() on an aggregate projection fails loud BEFORE firing hooks (P1)", async () => {
		const [row] = await Order.query().select("COUNT(*) as n").exec();
		// Guard fires before beforeDelete, so no hook runs against a phantom row and
		// no `DELETE ... WHERE pk IS NULL` is issued.
		await expect(row.delete()).rejects.toThrow(/primary key/i);
	});

	it("delete() on an UNSAVED entity fails loud, even with a manual PK (P1)", async () => {
		// BaseModel.delete() resolves the static repo (no REPO_REF gate), so a local
		// instance reaches the repo. A row-premised op requires $isPersisted — a
		// hand-set PK on an unsaved instance is NOT a database row, so deleting off
		// it (which would silently DELETE an unrelated existing row) must be blocked.
		await expect(new Order().delete()).rejects.toThrow(/not persisted/i);
		const local = new Order();
		local.id = "o1"; // real row exists, but this instance was never loaded
		await expect(local.delete()).rejects.toThrow(/not persisted/i);
		// The real row is untouched.
		expect(await Order.find("o1")).not.toBeNull();
	});

	it("related().create() on a projection parent is blocked (no orphan FK) (P1)", async () => {
		@Entity("books")
		class Book extends BaseModel {
			@PrimaryKey() declare id: string;
			@Column() declare authorId: string;
		}
		@Entity("authors")
		class Author extends BaseModel {
			@PrimaryKey() declare id: string;
			@HasMany(() => Book, { foreignKey: "author_id" })
			declare books: Book[];
		}
		await conn.execute(
			"CREATE TABLE IF NOT EXISTS authors (id TEXT PRIMARY KEY)",
		);
		await conn.execute(
			"CREATE TABLE IF NOT EXISTS books (id TEXT PRIMARY KEY, author_id TEXT)",
		);
		await conn.execute("INSERT INTO authors VALUES ('a1')");
		// Project only an aggregate → the hydrated Author is persisted but has no id.
		const [author] = await Author.query().select("COUNT(*) as n").exec();
		// The proxy builds lazily; the missing-key error surfaces at write time (the
		// parent is persisted so it isn't re-saved, and there's no key to set as FK).
		await expect(author.related("books").create({ id: "b0" })).rejects.toThrow(
			/primary key/i,
		);
	});

	it("related().create() persists an UNSAVED parent first, then the child (Lucid parity, R7)", async () => {
		@Entity("po_books")
		class PoBook extends BaseModel {
			@PrimaryKey() declare id: string;
			@Column() declare authorId: string;
		}
		@Entity("po_authors")
		class PoAuthor extends BaseModel {
			@PrimaryKey() declare id: string;
			@HasMany(() => PoBook, { foreignKey: "author_id" })
			declare books: PoBook[];
		}
		await conn.execute("CREATE TABLE po_authors (id TEXT PRIMARY KEY)");
		await conn.execute(
			"CREATE TABLE po_books (id TEXT PRIMARY KEY, author_id TEXT)",
		);
		// An UNPERSISTED parent with a client-set key, reached via the repo (bypasses
		// the entity-level REPO_REF gate). Lucid saves the parent first in the txn.
		const repo = PoAuthor.$repo();
		const author = new PoAuthor();
		author.id = "pa1";
		expect(author.$isPersisted).toBe(false);
		const book = await repo.relatedProxy(author, "books").create({ id: "pb1" });
		// Parent got persisted, child created with the parent's FK — no orphan.
		expect(author.$isPersisted).toBe(true);
		expect(await PoAuthor.find("pa1")).not.toBeNull();
		expect(book.authorId).toBe("pa1");
		expect((await PoBook.find("pb1"))?.authorId).toBe("pa1");
	});

	// R7 P3 — the remaining row-premised ops share the one #assertPersistedRow
	// seam; these pin that fresh/forceDelete/restore/loadCount/load reject an
	// unpersisted (or keyless-projection) entity exactly like delete/refresh.
	// `restore()` no-ops on a non-soft-delete model (its guard is unreachable), so
	// its cases run against a @SoftDeletes model.
	it("forceDelete()/restore() reject an UNSAVED entity, even with a manual PK", async () => {
		await conn.execute(
			"CREATE TABLE IF NOT EXISTS sd_orders (id TEXT PRIMARY KEY, deleted_at TEXT)",
		);
		await expect(Order.$repo().forceDelete(new Order())).rejects.toThrow(
			/not persisted/i,
		);
		await expect(SdOrder.$repo().restore(new SdOrder())).rejects.toThrow(
			/not persisted/i,
		);
		const local = new Order();
		local.id = "o1"; // real row exists, but this instance was never loaded
		await expect(Order.$repo().forceDelete(local)).rejects.toThrow(
			/not persisted/i,
		);
		// The real row is untouched by the blocked op.
		expect(await Order.find("o1")).not.toBeNull();
	});

	it("fresh()/forceDelete()/restore() on a keyless projection fail loud with a PK error", async () => {
		await conn.execute(
			"CREATE TABLE IF NOT EXISTS sd_orders (id TEXT PRIMARY KEY, deleted_at TEXT)",
		);
		const [row] = await Order.query().select("COUNT(*) as n").exec();
		// Persisted (hydrated by the repo) but no PK → a row-premised op must not
		// silently target `WHERE pk IS NULL`.
		await expect(row.fresh()).rejects.toThrow(/primary key/i);
		await expect(Order.$repo().forceDelete(row)).rejects.toThrow(
			/primary key/i,
		);
		const [sdRow] = await SdOrder.query().select("COUNT(*) as n").exec();
		await expect(SdOrder.$repo().restore(sdRow)).rejects.toThrow(
			/primary key/i,
		);
	});

	it("loadCount()/load() on a keyless projection fail loud with a PK error", async () => {
		@Entity("p3_books")
		class P3Book extends BaseModel {
			@PrimaryKey() declare id: string;
			@Column() declare authorId: string;
		}
		@Entity("p3_authors")
		class P3Author extends BaseModel {
			@PrimaryKey() declare id: string;
			@HasMany(() => P3Book, { foreignKey: "author_id" })
			declare books: P3Book[];
		}
		await conn.execute("CREATE TABLE p3_authors (id TEXT PRIMARY KEY)");
		await conn.execute(
			"CREATE TABLE p3_books (id TEXT PRIMARY KEY, author_id TEXT)",
		);
		await conn.execute("INSERT INTO p3_authors VALUES ('a1')");
		const [author] = await P3Author.query().select("COUNT(*) as n").exec();
		await expect(author.loadCount("books")).rejects.toThrow(/primary key/i);
		await expect(author.load("books")).rejects.toThrow(/primary key/i);
	});
});
