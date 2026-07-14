import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	Column,
	column,
	HasMany,
	PrimaryKey,
	SoftDeletes,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// P1a — a related model whose column is a @column.dateTime.
class Blog extends BaseModel {
	static override table = "blogs";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => Post, { foreignKey: "blog_id" })
	declare posts: Post[];
}
class Post extends BaseModel {
	static override table = "posts";
	@PrimaryKey() declare id: string;
	@Column() declare blogId: string;
	@column.dateTime() declare publishedAt: DateTime | null;
}

// P2b — soft-delete column mapped to a legacy name via columnName.
@SoftDeletes()
class Ticket extends BaseModel {
	static override table = "tickets";
	@PrimaryKey() declare id: string;
	@Column() declare subject: string;
	@column.dateTime({ columnName: "removed_on" })
	declare deletedAt: DateTime | null;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE blogs (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE posts (id TEXT PRIMARY KEY, blog_id TEXT, published_at TEXT)",
	);
	await conn.execute(
		"CREATE TABLE tickets (id TEXT PRIMARY KEY, subject TEXT, removed_on TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > audit P1/P2 fixes", () => {
	it("P1a: preloaded relation hydrates @column.dateTime as a Chronos DateTime", async () => {
		await Blog.create({ id: "b1", name: "tech" });
		await Post.create({
			id: "p1",
			blogId: "b1",
			publishedAt: new DateTime("2026-06-09T12:00:00Z"),
		});

		const [blog] = await Blog.query().preload("posts").exec();
		const post = blog.posts[0];
		// Before the fix this was a raw ISO string, not a DateTime.
		expect(post.publishedAt).toBeInstanceOf(DateTime);
		expect(Date.parse(post.publishedAt?.toISO() ?? "")).toBe(
			Date.parse("2026-06-09T12:00:00Z"),
		);
	});

	it("P2b: soft delete writes/reads the columnName-overridden deletedAt column", async () => {
		const t = await Ticket.create({ id: "t1", subject: "help" });
		await t.delete();

		// The legacy column `removed_on` (not `deleted_at`) is stamped.
		const [raw] = await conn.query<Record<string, unknown>>(
			"SELECT removed_on FROM tickets WHERE id = 't1'",
		);
		expect(raw.removed_on).not.toBeNull();

		// Default scope excludes it; onlyTrashed finds it (read filter honors the override).
		expect(await Ticket.find("t1")).toBeNull();
		const trashed = await Ticket.query().onlyTrashed().exec();
		expect(trashed.map((x) => x.id)).toContain("t1");
	});
});
