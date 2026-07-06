import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	HasManyThrough,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("th_users")
class ThUser extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare countryId: string;
}

@Entity("th_posts")
class ThPost extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare userId: string;
}

@Entity("th_countries")
class ThCountry extends BaseEntity {
	@PrimaryKey() declare id: string;
	@HasManyThrough(
		() => ThPost,
		() => ThUser,
	)
	declare posts: ThPost[];
}

function db() {
	return wrapPrepareMock({
		prepare() {
			return { run: () => ({ changes: 0 }), all: () => [] };
		},
	});
}

describe("atlas > whereHas / withCount on hasManyThrough (2-hop EXISTS)", () => {
	it("whereHas emits a correlated 2-hop EXISTS", () => {
		const sql = new BaseRepository(ThCountry, db())
			.query()
			.whereHas("posts")
			.toSQL().sql;
		expect(sql).toContain('EXISTS (SELECT * FROM "th_posts"');
		// related.secondKey IN (SELECT through.secondLocal FROM through WHERE
		// through.firstKey = parent.pk). Default keys derive from the CLASS names
		// (ThUser → th_user_id, ThCountry → th_country_id), through PK = "id".
		expect(sql).toContain(
			'"th_posts"."th_user_id" IN (SELECT "id" FROM "th_users" WHERE "th_users"."th_country_id" = "th_countries"."id")',
		);
	});

	it("whereHas callback constrains the related side", () => {
		const { sql, params } = new BaseRepository(ThCountry, db())
			.query()
			.whereHas("posts", (q) => q.where("id", ">", 5))
			.toSQL();
		expect(sql).toContain('"id" > ?');
		expect(params).toContain(5);
	});

	it("withCount projects a correlated 2-hop COUNT(*)", () => {
		const sql = new BaseRepository(ThCountry, db())
			.query()
			.withCount("posts")
			.toSQL().sql;
		expect(sql).toContain('(SELECT COUNT(*) FROM "th_posts"');
		expect(sql).toContain('AS "posts_count"');
	});
});
