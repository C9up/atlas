/**
 * belongsTo `associate()` persists an UNSAVED owner first, then saves the parent
 * with the FK set — all in ONE transaction. If the parent save fails AFTER the
 * owner was inserted, the DB rolls back, and the in-memory state must be restored
 * too (like withParentSaved): the owner reverts to $isNew, its REPO_REF re-points at
 * the durable repo, and its queued events are dropped. Before the fix associate()
 * registered only an after('commit') hook, so a rollback left the owner lying
 * $isPersisted with a dead trx ref and a stale event queue.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	BelongsTo,
	beforeCreate,
	beforeUpdate,
	Column,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class AsAuthor extends BaseModel {
	static override table = "as_authors";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@beforeCreate() static stamp(a: AsAuthor): void {
		a.addDomainEvent("author.created", { id: a.id });
	}
}

class AsPost extends BaseModel {
	static override table = "as_posts";
	@PrimaryKey() declare id: string;
	@Column() declare authorId: string | null;
	@BelongsTo(() => AsAuthor, { foreignKey: "author_id" })
	declare author: AsAuthor;
	@beforeUpdate() static guard(p: AsPost): void {
		// Poison the parent UPDATE that runs AFTER the owner was already inserted.
		if (p.authorId === "boom") throw new Error("boom-parent");
	}
}

// The FK references the owner's `code` (a non-PK ownerKey), so a savable owner whose
// `code` is unset passes the INSERT but fails the ownerKey check right after.
class OwAuthor extends BaseModel {
	static override table = "ow_authors";
	@PrimaryKey() declare id: string;
	@Column() declare code: string | null;
	@beforeCreate() static stamp(a: OwAuthor): void {
		a.addDomainEvent("owner.created", { id: a.id });
	}
}

class OwPost extends BaseModel {
	static override table = "ow_posts";
	@PrimaryKey() declare id: string;
	@Column() declare authorCode: string | null;
	@BelongsTo(() => OwAuthor, { foreignKey: "author_code", ownerKey: "code" })
	declare author: OwAuthor;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE as_authors (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE as_posts (id TEXT PRIMARY KEY, author_id TEXT)",
	);
	await conn.execute(
		"CREATE TABLE ow_authors (id TEXT PRIMARY KEY, code TEXT)",
	);
	await conn.execute(
		"CREATE TABLE ow_posts (id TEXT PRIMARY KEY, author_code TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM as_posts");
	await conn.execute("DELETE FROM as_authors");
	await conn.execute("DELETE FROM ow_posts");
	await conn.execute("DELETE FROM ow_authors");
});

describe("atlas > belongsTo associate() restores state on rollback", () => {
	it("reverts the freshly-saved owner and clears its events when the parent save fails", async () => {
		const authorRepo = AsAuthor.$repo();
		const published: string[] = [];
		// associate() dispatches BOTH the owner's and the parent's events through the
		// PARENT repo (the belongsTo owner is flushed via `parentRepo`), so the sink
		// lives on the post's repo.
		const postRepo = AsPost.$repo();
		postRepo.onDomainEvents = async (ev) => {
			published.push(...ev.map((e) => e.name));
		};

		const post = await postRepo.create({ id: "p1", authorId: null });
		const author = new AsAuthor();
		author.id = "boom"; // associating sets post.authorId = "boom" → parent UPDATE throws
		author.name = "Ada";
		expect(author.$isPersisted).toBe(false);

		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");

		await expect(rel.associate(author)).rejects.toThrow(/boom-parent/);

		// The owner insert rolled back with the failed parent update.
		expect(await authorRepo.find("boom")).toBeNull();
		expect(published).toEqual([]);

		// The owner was inserted on the rolled-back trx → it must report $isNew again,
		// with its "author.created" event dropped, else reusing it would skip the
		// re-insert and leave the parent FK pointing at a phantom owner row.
		expect(author.$isPersisted).toBe(false);
		expect(author.getDomainEvents()).toEqual([]);

		// The parent's row existed before, so it stays persisted (only fresh inserts
		// revert). And the owner is reusable: a clean associate re-inserts it + links.
		expect(post.$isPersisted).toBe(true);
		author.id = "ok";
		const rel2 = post.related("author");
		if (rel2.type !== "belongsTo") throw new Error("expected belongsTo");
		await rel2.associate(author);
		expect(await authorRepo.find("ok")).not.toBeNull();
		expect(post.authorId).toBe("ok");
		expect(published).toEqual(["author.created"]); // published exactly once
	});

	it("reverts a freshly-saved owner when the ownerKey check fails AFTER the save", async () => {
		const ownerRepo = OwAuthor.$repo();
		const post = await OwPost.$repo().create({ id: "p1", authorCode: null });

		// The owner is savable (has a PK) but its `code` — the ownerKey the FK
		// references — is unset. associate() INSERTs the owner, THEN reads `code`,
		// finds it absent, and throws E_MISSING_OWNER_KEY. The owner insert must roll
		// back AND the in-memory owner must be restored, even though the throw happens
		// between the save and the (now earlier-registered) rollback hook.
		const owner = new OwAuthor();
		owner.id = "o1";
		owner.code = null; // no ownerKey value
		expect(owner.$isPersisted).toBe(false);

		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");

		await expect(rel.associate(owner)).rejects.toThrow(/has no code/i);

		// The owner INSERT rolled back with the aborted associate.
		expect(await ownerRepo.find("o1")).toBeNull();

		// And its in-memory state was restored: $isNew again, events dropped — not left
		// lying $isPersisted with a ref bound to the finished transaction.
		expect(owner.$isPersisted).toBe(false);
		expect(owner.getDomainEvents()).toEqual([]);

		// Reusable: set the missing key and associate cleanly.
		owner.code = "C1";
		const rel2 = post.related("author");
		if (rel2.type !== "belongsTo") throw new Error("expected belongsTo");
		await rel2.associate(owner);
		expect(await ownerRepo.find("o1")).not.toBeNull();
		expect(post.authorCode).toBe("C1");
	});
});
