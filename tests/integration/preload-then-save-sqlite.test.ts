/**
 * A preloaded or lazily loaded relation is not an attribute.
 *
 * The relation was set on the instance after the persisted snapshot, so it
 * counted as dirty: an untouched preloaded model reported `$isDirty`, and
 * save() wrote the relation into the UPDATE (`SET "posts" = ?`, refused as an
 * array, or a related instance bound as JSON into a column of that name).
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
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("pts_users")
class PtsUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@HasMany(() => PtsPost, { foreignKey: "user_id" })
	declare posts: PtsPost[];
}

@Entity("pts_posts")
class PtsPost extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare userId: number;
	@Column() declare title: string;
	@Column() declare author: string;
	@BelongsTo(() => PtsUser, { foreignKey: "user_id" })
	declare user: PtsUser;
}

let conn: AsyncDatabaseConnection;
let users: BaseRepository<PtsUser>;
let posts: BaseRepository<PtsPost>;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE pts_users (id INTEGER PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE pts_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, author TEXT)",
	);
	users = new BaseRepository(PtsUser, conn);
	posts = new BaseRepository(PtsPost, conn);
});

afterAll(async () => {
	await conn.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM pts_posts");
	await conn.execute("DELETE FROM pts_users");
	await conn.execute("INSERT INTO pts_users VALUES (1, 'carol')");
	await conn.execute(
		"INSERT INTO pts_posts VALUES (1, 1, 'hello', 'carol'), (2, 1, 'again', 'carol')",
	);
});

describe("atlas > save() after preload()/load()", () => {
	it("a hasMany preload leaves the model clean and save() writes only the change", async () => {
		const user = await users.query().preload("posts").where("id", 1).first();
		if (!user) throw new Error("no user");
		expect(user.posts).toHaveLength(2);
		expect(user.$isDirty).toBe(false);
		expect(user.isDirty("posts")).toBe(false);

		user.name = "carol2";
		expect(Object.keys(user.$dirty)).toEqual(["name"]);
		await users.save(user);
		expect(await conn.query("SELECT name FROM pts_users")).toEqual([
			{ name: "carol2" },
		]);
	});

	it("a belongsTo preload does not reach the UPDATE", async () => {
		const post = await posts.query().preload("user").where("id", 1).first();
		if (!post) throw new Error("no post");
		expect(post.user.name).toBe("carol");
		post.title = "edited";
		await posts.save(post);
		expect(
			await conn.query("SELECT title, author FROM pts_posts WHERE id = 1"),
		).toEqual([{ title: "edited", author: "carol" }]);
	});

	it("a lazily loaded relation does not reach the UPDATE either", async () => {
		const user = await users.find(1);
		if (!user) throw new Error("no user");
		await users.loadRelation(user, "posts");
		expect(user.posts).toHaveLength(2);
		expect(user.$isDirty).toBe(false);
		user.name = "dave";
		await users.save(user);
		expect(await conn.query("SELECT name FROM pts_users")).toEqual([
			{ name: "dave" },
		]);
	});
});
