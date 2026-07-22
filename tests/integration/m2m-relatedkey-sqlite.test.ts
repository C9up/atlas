/**
 * Adonis Lucid `relatedKey`: a many-to-many pivot whose otherKey references a
 * NON-PK column of the related model (here `slug`). attach writes that column's
 * value, and preload filters/joins the related rows by it — not the PK.
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
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";

@Entity("rk_tags")
class Tag extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare slug: string;
	@Column() declare name: string;
}

@Entity("rk_articles")
class Article extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare title: string;

	@ManyToMany(() => Tag, {
		pivotTable: "rk_article_tag",
		foreignKey: "article_id",
		otherKey: "tag_slug",
		// The pivot's tag_slug references Tag.slug, NOT Tag.id.
		relatedKey: "slug",
	})
	declare tags: Tag[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE rk_tags (id TEXT PRIMARY KEY, slug TEXT, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE rk_articles (id TEXT PRIMARY KEY, title TEXT)",
	);
	await conn.execute(
		"CREATE TABLE rk_article_tag (article_id TEXT, tag_slug TEXT)",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > m2m relatedKey (non-PK related column)", () => {
	it("attaches by relatedKey value and preloads by it", async () => {
		const articleRepo = new BaseRepository(Article, conn);
		const tagRepo = new BaseRepository(Tag, conn);
		await tagRepo.create({ id: "t1", slug: "js", name: "JavaScript" });
		await tagRepo.create({ id: "t2", slug: "rs", name: "Rust" });
		const article = await articleRepo.create({ id: "a1", title: "Hello" });

		// attach by the relatedKey (slug) values — written into pivot.tag_slug.
		const tagsRel = article.related("tags");
		if (tagsRel.type !== "manyToMany") throw new Error("expected m2m proxy");
		await tagsRel.attach(["js", "rs"]);

		const pivot = await conn.query<{ tag_slug: string }>(
			"SELECT tag_slug FROM rk_article_tag WHERE article_id = 'a1' ORDER BY tag_slug",
		);
		expect(pivot.map((r) => r.tag_slug)).toEqual(["js", "rs"]);

		// preload resolves the related rows by slug (relatedKey), not id.
		const loaded = await articleRepo
			.query()
			.where("id", "a1")
			.preload("tags")
			.first();
		expect((loaded?.tags ?? []).map((t) => t.name).sort()).toEqual([
			"JavaScript",
			"Rust",
		]);
	});
});
