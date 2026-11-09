/**
 * A preload query is a fetch of the related model, so that model's
 * `beforeFetch` / `afterFetch` hooks run on it — for every relation kind,
 * nested preloads and `load()` included.
 *
 * They used to run on the root query only: a filter an application put in a
 * `@beforeFetch` (tenant, visibility) applied to `Post.query()` and not to
 * `user.posts` preloaded, which handed back every tenant's rows.
 *
 * SQLite always runs; Postgres runs when ATLAS_TEST_PG_URL is set.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import {
	BelongsTo,
	Column,
	Entity,
	HasMany,
	HasManyThrough,
	HasOne,
	ManyToMany,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { afterFetch, beforeFetch } from "../../src/decorators/hooks.js";
import type { ModelQuery } from "../../src/ModelQuery.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";

/** What the hooks saw, in order. */
const log: string[] = [];
/** The tenant every hook filters on. */
let tenant = 1;

@Entity("ph_comments")
class Comment extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare postId: number;
	@Column() declare tenantId: number;
	@Column() declare body: string;

	@beforeFetch()
	static scopeComments(q: ModelQuery<BaseEntity>): void {
		log.push("comments:before");
		q.where("tenant_id", tenant);
	}
	@afterFetch()
	static seenComments(rows: BaseEntity[]): void {
		log.push(`comments:after:${rows.length}`);
	}
}

@Entity("ph_posts")
class Post extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare authorId: number;
	@Column() declare tenantId: number;
	@Column() declare body: string;
	@HasMany(() => Comment) declare comments: Comment[];
	@BelongsTo(() => Author) declare author: Author | null;

	@beforeFetch()
	static scopePosts(q: ModelQuery<BaseEntity>): void {
		log.push("posts:before");
		q.where("tenant_id", tenant);
	}
	@afterFetch()
	static seenPosts(rows: BaseEntity[]): void {
		// The rows arrive with their own preloads already in place.
		const nested = rows.map((r) =>
			Array.isArray(r.comments) ? r.comments.length : "-",
		);
		log.push(`posts:after:${rows.length}:${nested.join(",")}`);
	}
}

@Entity("ph_profiles")
class Profile extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare authorId: number;
	@Column() declare tenantId: number;

	@beforeFetch()
	static scopeProfiles(q: ModelQuery<BaseEntity>): void {
		q.where("tenant_id", tenant);
	}
}

@Entity("ph_badges")
class Badge extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare tenantId: number;
	@Column() declare label: string;

	@beforeFetch()
	static scopeBadges(q: ModelQuery<BaseEntity>): void {
		log.push("badges:before");
		q.where("tenant_id", tenant);
	}
}

@Entity("ph_authors")
class Author extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare tenantId: number;
	@Column() declare clubId: number;
	@HasMany(() => Post) declare posts: Post[];
	@HasOne(() => Profile) declare profile: Profile | null;
	@ManyToMany(() => Badge, {
		pivotTable: "ph_author_badges",
		foreignKey: "author_id",
		otherKey: "badge_id",
	})
	declare badges: Badge[];

	@beforeFetch()
	static scopeAuthors(q: ModelQuery<BaseEntity>): void {
		log.push("authors:before");
		q.where("tenant_id", tenant);
	}
}

@Entity("ph_clubs")
class Club extends BaseEntity {
	@PrimaryKey() declare id: number;
	@HasMany(() => Author) declare authors: Author[];
	@HasManyThrough(
		() => Post,
		() => Author,
	)
	declare posts: Post[];
}

function suite(label: string, url: string, dialect: AtlasDialect): void {
	(url ? describe : describe.skip)(`preload fetch hooks ${label}`, () => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			setAtlasDialect(dialect);
			conn = await createNapiConnection(url, 1, 2);
			const tables = [
				"ph_comments",
				"ph_posts",
				"ph_profiles",
				"ph_author_badges",
				"ph_badges",
				"ph_authors",
				"ph_clubs",
			];
			for (const t of tables) await conn.execute(`DROP TABLE IF EXISTS ${t}`);
			await conn.execute("CREATE TABLE ph_clubs (id INTEGER PRIMARY KEY)");
			await conn.execute(
				"CREATE TABLE ph_authors (id INTEGER PRIMARY KEY, tenant_id INTEGER, club_id INTEGER)",
			);
			await conn.execute(
				"CREATE TABLE ph_posts (id INTEGER PRIMARY KEY, author_id INTEGER, tenant_id INTEGER, body VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE ph_comments (id INTEGER PRIMARY KEY, post_id INTEGER, tenant_id INTEGER, body VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE ph_profiles (id INTEGER PRIMARY KEY, author_id INTEGER, tenant_id INTEGER)",
			);
			await conn.execute(
				"CREATE TABLE ph_badges (id INTEGER PRIMARY KEY, tenant_id INTEGER, label VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE ph_author_badges (author_id INTEGER, badge_id INTEGER)",
			);
			await conn.execute("INSERT INTO ph_clubs (id) VALUES (1)");
			// Author 2 is tenant 2's, in the same club.
			await conn.execute(
				"INSERT INTO ph_authors (id, tenant_id, club_id) VALUES (1, 1, 1), (2, 2, 1)",
			);
			// Post 2 is tenant 2's but points at tenant 1's author.
			await conn.execute(
				"INSERT INTO ph_posts (id, author_id, tenant_id, body) VALUES " +
					"(1, 1, 1, 'mine'), (2, 1, 2, 'theirs'), (3, 2, 2, 'their author')",
			);
			await conn.execute(
				"INSERT INTO ph_comments (id, post_id, tenant_id, body) VALUES (1, 1, 1, 'ok'), (2, 1, 2, 'leak')",
			);
			await conn.execute(
				"INSERT INTO ph_profiles (id, author_id, tenant_id) VALUES (1, 1, 2)",
			);
			await conn.execute(
				"INSERT INTO ph_badges (id, tenant_id, label) VALUES (1, 1, 'gold'), (2, 2, 'red')",
			);
			await conn.execute(
				"INSERT INTO ph_author_badges (author_id, badge_id) VALUES (1, 1), (1, 2)",
			);
		});

		beforeEach(() => {
			setAtlasDialect(dialect);
			tenant = 1;
			log.length = 0;
		});

		afterAll(async () => {
			await conn?.close();
		});

		const authors = () => new BaseRepository(Author, conn);

		it("runs the related model's hooks on a hasMany preload, nested ones first", async () => {
			const [author] = await authors()
				.query()
				.where("id", 1)
				.preload("posts", (q) => q.preload("comments"))
				.exec();

			expect(author?.posts.map((p) => p.body)).toEqual(["mine"]);
			expect(author?.posts[0]?.comments.map((c) => c.body)).toEqual(["ok"]);
			// Upstream's order: each query's beforeFetch, its rows' own preloads,
			// then its afterFetch, which sees them loaded.
			expect(log).toEqual([
				"authors:before",
				"posts:before",
				"comments:before",
				"comments:after:1",
				"posts:after:1:1",
			]);
		});

		it("runs them on hasOne, belongsTo and many-to-many preloads", async () => {
			const [author] = await authors()
				.query()
				.where("id", 1)
				.preload("profile")
				.preload("badges")
				.exec();
			expect(author?.profile).toBeNull();
			expect(author?.badges.map((b) => b.label)).toEqual(["gold"]);

			tenant = 2;
			const posts = await new BaseRepository(Post, conn)
				.query()
				.preload("author")
				.orderBy("id")
				.exec();
			// Post 2 (tenant 2) points at tenant 1's author: not visible.
			expect(posts.map((p) => [p.body, p.author?.id ?? null])).toEqual([
				["theirs", null],
				["their author", 2],
			]);
		});

		it("runs them on a through preload", async () => {
			const [club] = await new BaseRepository(Club, conn)
				.query()
				.preload("posts")
				.preload("authors")
				.exec();
			expect(club?.authors.map((a) => a.id)).toEqual([1]);
			expect(club?.posts.map((p) => p.body)).toEqual(["mine"]);
		});

		it("runs them on load()", async () => {
			const author = await authors().findOrFail(1);
			log.length = 0;
			await author.load("posts");
			expect(author.posts.map((p) => p.body)).toEqual(["mine"]);
			expect(log).toContain("posts:before");
			expect(log).toContain("posts:after:1:-");
		});

		it("applies a groupLimit to the rows the hook lets through", async () => {
			const [author] = await authors()
				.query()
				.where("id", 1)
				.preload("posts", (q) => q.groupLimit(1).groupOrderBy("id", "desc"))
				.exec();
			// Without the hook in the ranked query, post 2 (tenant 2) would take
			// the one slot and the filter would then leave nothing.
			expect(author?.posts.map((p) => p.body)).toEqual(["mine"]);
		});

		it("fires nothing for a relation no query was made for", async () => {
			const rows = await authors()
				.query()
				.where("id", 99)
				.preload("posts")
				.exec();
			expect(rows).toEqual([]);
			expect(log).toEqual(["authors:before"]);
		});
	});
}

suite("SQLite", "sqlite::memory:", "sqlite");
suite("PG", process.env.ATLAS_TEST_PG_URL ?? "", "postgres");
