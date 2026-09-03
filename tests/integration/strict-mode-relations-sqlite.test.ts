/**
 * Relations must keep working with Atlas strict mode ON.
 *
 * Strict mode disables the raw WHERE/JOIN entry points. Only the BLOCKING
 * direction was covered — that `repo.raw()` throws — so nothing proved the
 * framework's own relation paths still resolve under it. `ModelQuery` even
 * carries an unused `runWithAtlasInternalBypass` escape hatch, written for
 * internal code that "legitimately needs to call whereRaw/joinRaw": if any
 * internal path still did, enabling strict mode would break relations for the
 * apps most likely to enable it.
 */
import "reflect-metadata";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
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
	setAtlasStrictMode,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

@Entity("sm_tags")
class SmTag extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare label: string;
}

@Entity("sm_posts")
class SmPost extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare author_id: string;
	@Column() declare title: string;
}

@Entity("sm_authors")
class SmAuthor extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@HasMany(() => SmPost, { foreignKey: "author_id" })
	declare posts: SmPost[];

	@ManyToMany(() => SmTag, {
		pivotTable: "sm_author_tags",
		foreignKey: "author_id",
		otherKey: "tag_id",
	})
	declare tags: SmTag[];
}

/** The lazy relation proxy hands back `unknown`; this narrows it honestly. */
function isTagList(value: unknown): value is SmTag[] {
	return Array.isArray(value) && value.every((v) => v instanceof SmTag);
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE sm_authors (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE sm_posts (id TEXT PRIMARY KEY, author_id TEXT, title TEXT)",
	);
	await conn.execute("CREATE TABLE sm_tags (id TEXT PRIMARY KEY, label TEXT)");
	await conn.execute(
		"CREATE TABLE sm_author_tags (author_id TEXT, tag_id TEXT)",
	);
	await conn.execute("INSERT INTO sm_authors VALUES ('a1', 'Ada')");
	await conn.execute("INSERT INTO sm_posts VALUES ('p1', 'a1', 'First')");
	await conn.execute("INSERT INTO sm_posts VALUES ('p2', 'a1', 'Second')");
	await conn.execute("INSERT INTO sm_tags VALUES ('t1', 'rust')");
	await conn.execute("INSERT INTO sm_author_tags VALUES ('a1', 't1')");
	setDb(conn);
});

afterEach(() => setAtlasStrictMode(false));

afterAll(async () => {
	clearDb(conn);
	await conn.close();
});

describe("atlas > relations under strict mode", () => {
	it("preloads a hasMany", async () => {
		setAtlasStrictMode(true);

		const author = await new BaseRepository(SmAuthor, conn)
			.query()
			.preload("posts")
			.first();

		expect(author?.posts.map((p) => p.title).sort()).toEqual([
			"First",
			"Second",
		]);
	});

	it("preloads a manyToMany — the pivot EXISTS subquery is the path the escape hatch was written for", async () => {
		setAtlasStrictMode(true);

		const author = await new BaseRepository(SmAuthor, conn)
			.query()
			.preload("tags")
			.first();

		expect(author?.tags.map((t) => t.label)).toEqual(["rust"]);
	});

	it("resolves a lazy manyToMany through related()", async () => {
		setAtlasStrictMode(true);
		const author = await new BaseRepository(SmAuthor, conn).find("a1");

		if (!author) throw new Error("author a1 should exist");
		const rel = author.related("tags");
		if (rel.type !== "manyToMany") throw new Error("expected an m2m proxy");
		const tags = await rel.query();

		// The proxy's query() is untyped (the relation kinds are a union), so the
		// shape is proven rather than asserted.
		expect(isTagList(tags)).toBe(true);
		if (isTagList(tags)) expect(tags.map((t) => t.label)).toEqual(["rust"]);
	});

	it("still blocks an application whereRaw, which is the point", async () => {
		setAtlasStrictMode(true);

		expect(() =>
			new BaseRepository(SmAuthor, conn).query().whereRaw("1 = 1"),
		).toThrow(/strict mode/i);
	});
});
