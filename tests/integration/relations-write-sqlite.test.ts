/**
 * Relation WRITE + soft-delete coverage against real in-memory SQLite — the
 * mutation side of the relation proxies (create-through, m2m attach/sync/detach)
 * and the soft-delete lifecycle, none of which the unit mocks reach.
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
	Column,
	Entity,
	HasMany,
	ManyToMany,
	PrimaryKey,
	SoftDeletes,
} from "../../src/index.js";

@Entity("w_users")
class WUser extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => WPost, { foreignKey: "wuser_id" })
	declare posts: WPost[];
}

@SoftDeletes()
@Entity("w_posts")
class WPost extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare wuserId: string;
	@Column() declare title: string;
	@Column() declare deletedAt: string | null;
	@ManyToMany(() => WTag, {
		pivotTable: "w_post_tag",
		foreignKey: "post_id",
		otherKey: "tag_id",
	})
	declare tags: WTag[];
}

@Entity("w_tags")
class WTag extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare label: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE w_users (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE w_posts (id TEXT PRIMARY KEY, wuser_id TEXT, title TEXT, deleted_at TEXT)",
	);
	await conn.execute("CREATE TABLE w_tags (id TEXT PRIMARY KEY, label TEXT)");
	await conn.execute("CREATE TABLE w_post_tag (post_id TEXT, tag_id TEXT)");
	await conn.execute("INSERT INTO w_users VALUES ('u1', 'Ada')");
	await conn.execute(
		"INSERT INTO w_tags VALUES ('t1', 'a'), ('t2', 'b'), ('t3', 'c')",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > relation writes against real SQLite", () => {
	it("creates a child through a hasMany proxy with the FK injected", async () => {
		const repo = new BaseRepository(WUser, conn);
		const ada = await repo.findOrFail("u1");
		await ada.related("posts").create({ id: "p1", title: "First" });

		const posts = await new BaseRepository(WPost, conn).where("wuserId", "u1");
		expect(posts.map((p) => p.title)).toEqual(["First"]);
		expect(posts[0].wuserId).toBe("u1");
	});

	it("attaches, syncs and detaches a manyToMany relation", async () => {
		const repo = new BaseRepository(WPost, conn);
		const post = await repo.findOrFail("p1");
		const tags = post.related("tags");
		if (tags.type !== "manyToMany")
			throw new Error("expected a manyToMany proxy");

		await tags.attach(["t1", "t2"]);
		let loaded = await repo.query().preload("tags").where("id", "p1");
		expect(loaded[0].tags.map((t) => t.id).sort()).toEqual(["t1", "t2"]);

		// sync replaces the set
		await tags.sync(["t2", "t3"]);
		loaded = await repo.query().preload("tags").where("id", "p1");
		expect(loaded[0].tags.map((t) => t.id).sort()).toEqual(["t2", "t3"]);

		// detach removes one
		await tags.detach(["t2"]);
		loaded = await repo.query().preload("tags").where("id", "p1");
		expect(loaded[0].tags.map((t) => t.id)).toEqual(["t3"]);
	});

	it("soft-deletes, hides by default, lists via onlyTrashed, then restores", async () => {
		const repo = new BaseRepository(WPost, conn);
		await repo.create({ id: "p2", wuserId: "u1", title: "Doomed" });

		const doomed = await repo.findOrFail("p2");
		await repo.delete(doomed);

		// Default queries exclude soft-deleted rows.
		expect((await repo.all()).map((p) => p.id)).not.toContain("p2");
		// onlyTrashed surfaces it.
		expect((await repo.onlyTrashed()).map((p) => p.id)).toContain("p2");
		// withTrashed includes both.
		expect((await repo.allWithTrashed()).map((p) => p.id)).toContain("p2");

		// restore brings it back.
		const trashed = (await repo.onlyTrashed()).find((p) => p.id === "p2");
		if (trashed) await repo.restore(trashed);
		expect((await repo.all()).map((p) => p.id)).toContain("p2");
	});

	it("forceDelete removes the row permanently", async () => {
		const repo = new BaseRepository(WPost, conn);
		await repo.create({ id: "p3", wuserId: "u1", title: "Gone" });
		const gone = await repo.findOrFail("p3");
		await repo.forceDelete(gone);
		expect((await repo.allWithTrashed()).map((p) => p.id)).not.toContain("p3");
	});

	it("cursor-paginates a result set", async () => {
		const repo = new BaseRepository(WUser, conn);
		await repo.create({ id: "u2", name: "Bob" });
		await repo.create({ id: "u3", name: "Cleo" });

		const page1 = await repo
			.query()
			.cursorPaginate({ limit: 2, orderBy: "id" });
		expect(page1.items.length).toBe(2);
		expect(page1.hasMore).toBe(true);

		const page2 = await repo.query().cursorPaginate({
			cursor: page1.nextCursor ?? undefined,
			limit: 2,
			orderBy: "id",
		});
		expect(page2.items.length).toBeGreaterThan(0);
	});
});
