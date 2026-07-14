import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	BelongsTo,
	Column,
	column,
	HasMany,
	PrimaryKey,
	SoftDeletes,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// D — a related model whose PK property is multi-word (postId → post_id).
class Article extends BaseModel {
	static override table = "articles";
	@PrimaryKey() declare postId: string;
	@Column() declare headline: string;
}
class Note extends BaseModel {
	static override table = "notes";
	@PrimaryKey() declare id: string;
	@Column() declare articleId: string;
	@BelongsTo(() => Article, { foreignKey: "article_id", ownerKey: "postId" })
	declare article: Article;
}

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
	await conn.execute(
		"CREATE TABLE articles (post_id TEXT PRIMARY KEY, headline TEXT)",
	);
	await conn.execute(
		"CREATE TABLE notes (id TEXT PRIMARY KEY, article_id TEXT)",
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

	it("D: belongsTo preload resolves a multi-word related PK (postId → post_id)", async () => {
		await Article.create({ postId: "a1", headline: "hello" });
		await Note.create({ id: "n1", articleId: "a1" });

		const [note] = await Note.query().preload("article").exec();
		// Before the fix the resolver used `WHERE postId IN (...)` / row['postId'],
		// which is `no such column` / undefined against the real `post_id` column.
		expect(note.article).toBeDefined();
		expect(note.article.headline).toBe("hello");
	});

	it("P1: a preload callback constraint resolves columns + prepares DateTime", async () => {
		await Blog.create({ id: "b2", name: "cal" });
		await Post.create({
			id: "old",
			blogId: "b2",
			publishedAt: new DateTime("2020-01-01T00:00:00Z"),
		});
		await Post.create({
			id: "new",
			blogId: "b2",
			publishedAt: new DateTime("2026-06-09T12:00:00Z"),
		});

		// The DateTime bound in the preload callback must be prepared to ISO and
		// `blogId`/`publishedAt` resolved — else this throws or filters wrong.
		const [blog] = await Blog.query()
			.where("id", "b2")
			.preload("posts", (q) =>
				q.where("publishedAt", ">", new DateTime("2025-01-01T00:00:00Z")),
			)
			.exec();
		expect(blog.posts.map((p) => p.id)).toEqual(["new"]);
	});

	it("P1: a nested where(q => ...) group prepares a DateTime value", async () => {
		const rows = await Post.query()
			.where((q) =>
				q.where("publishedAt", new DateTime("2026-06-09T12:00:00Z")),
			)
			.exec();
		expect(rows.map((p) => p.id)).toContain("new");
	});

	it("P1c: whereHas resolves a multi-word owner key in the correlated join", async () => {
		// The join is notes.article_id = articles.post_id (ownerKey postId → post_id).
		const notes = await Note.query()
			.whereHas("article", (q) => q.where("headline", "hello"))
			.exec();
		expect(notes.map((n) => n.id)).toContain("n1");
	});
});
