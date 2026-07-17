/**
 * `related()` on a @HasManyThrough / @HasOneThrough relation must behave like Lucid:
 *  - query() TRAVERSES the intermediate ("through") table — it does NOT read the
 *    related table by a direct foreign key.
 *  - the relation is READ-ONLY: create/save/createMany/saveMany throw (persist via
 *    the intermediate model instead).
 * Before the fix a through relation fell through to the hasMany proxy, so reads
 * bypassed the through table and writes hit the wrong table with a bogus FK.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	Column,
	HasManyThrough,
	PrimaryKey,
} from "../../src/index.js";
import { ModelQuery } from "../../src/ModelQuery.js";
import { clearDb, setDb } from "../../src/services/db.js";

class ThPost extends BaseModel {
	static override table = "th_posts";
	@PrimaryKey() declare id: string;
	@Column() declare userId: string;
}

class ThUser extends BaseModel {
	static override table = "th_users";
	@PrimaryKey() declare id: string;
	@Column() declare countryId: string;
}

class ThCountry extends BaseModel {
	static override table = "th_countries";
	@PrimaryKey() declare id: string;
	// countries → (users.country_id) → users → (posts.user_id) → posts
	@HasManyThrough(
		() => ThPost,
		() => ThUser,
		{
			firstKey: "country_id", // th_users.country_id → th_countries.id
			secondKey: "user_id", // th_posts.user_id → th_users.id
		},
	)
	declare posts: ThPost[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE th_countries (id TEXT PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE th_users (id TEXT PRIMARY KEY, country_id TEXT)",
	);
	await conn.execute(
		"CREATE TABLE th_posts (id TEXT PRIMARY KEY, user_id TEXT)",
	);
	// Country c1 → users u1,u2 → posts p1,p2,p3. Country c2 → user u9 → post p9.
	await conn.execute("INSERT INTO th_countries (id) VALUES ('c1'), ('c2')");
	await conn.execute(
		"INSERT INTO th_users (id, country_id) VALUES ('u1','c1'), ('u2','c1'), ('u9','c2')",
	);
	await conn.execute(
		"INSERT INTO th_posts (id, user_id) VALUES ('p1','u1'), ('p2','u2'), ('p3','u1'), ('p9','u9')",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > @HasManyThrough related() (Lucid parity)", () => {
	it("query() traverses the through table (not a direct FK)", async () => {
		const country = await ThCountry.$repo().findOrFail("c1");
		const rel = country.related("posts");
		if (rel.type !== "hasManyThrough")
			throw new Error("expected hasManyThrough");

		const q = rel.query();
		if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
		const posts = await q.orderBy("id").exec();
		// c1's posts reached via its users u1,u2 — NOT c2's p9.
		expect(posts.map((p) => p.id)).toEqual(["p1", "p2", "p3"]);
	});

	it("stays chainable like Lucid (orderBy/limit on the traversed query)", async () => {
		const country = await ThCountry.$repo().findOrFail("c1");
		const rel = country.related("posts");
		if (rel.type !== "hasManyThrough")
			throw new Error("expected hasManyThrough");

		const q = rel.query();
		if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
		const posts = await q.orderBy("id", "desc").limit(2).exec();
		expect(posts.map((p) => p.id)).toEqual(["p3", "p2"]);
	});

	it("is READ-ONLY: writes throw (persist via the intermediate model)", async () => {
		const country = await ThCountry.$repo().findOrFail("c1");
		const rel = country.related("posts");
		if (rel.type !== "hasManyThrough")
			throw new Error("expected hasManyThrough");

		await expect(rel.create({ id: "px", userId: "u1" })).rejects.toThrow(
			/READ-ONLY|not supported/i,
		);
		await expect(rel.save(new ThPost())).rejects.toThrow(
			/READ-ONLY|not supported/i,
		);
		// No stray row was written to the related table.
		expect(await ThPost.$repo().find("px")).toBeNull();
	});
});
