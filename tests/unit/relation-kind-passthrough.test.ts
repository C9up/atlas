import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	BelongsTo,
	CamelCaseNamingStrategy,
	Column,
	Entity,
	HasMany,
	HasManyThrough,
	HasOne,
	PrimaryKey,
	type RelationKind,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

/**
 * Every relation must tell the naming strategy WHICH kind it is.
 *
 * The default strategy ignores the argument, so a call site passing `hasMany`
 * for a `belongsTo` — or flattening `hasOne` and the `*Through` kinds into
 * `hasMany` — was invisible: the derived column came out the same either way.
 * A custom strategy that answers per kind, which is what upstream's own does
 * for `belongsTo`, then got the wrong answer and looked for a column no
 * migration had created.
 *
 * This strategy encodes the kind INTO the column, so the SQL says which one
 * each path actually passed.
 */
class KindNamingStrategy extends CamelCaseNamingStrategy {
	override relationForeignKey(
		kind: RelationKind,
		parentClass: string,
		_parentPk: string,
	): string {
		// An ATTRIBUTE, per the contract; `columnName()` makes it a column.
		const tag = kind.charAt(0).toUpperCase() + kind.slice(1);
		return `${parentClass.toLowerCase()}${tag}`;
	}
}

@Entity("k_users")
class KUser extends BaseEntity {
	static namingStrategy = new KindNamingStrategy();
	@PrimaryKey() declare id: string;
	@Column() declare countryId: string;
	@HasMany(() => KPost) declare posts: KPost[];
	@HasOne(() => KProfile) declare profile: KProfile;
	@BelongsTo(() => KCountry) declare country: KCountry;
}

@Entity("k_posts")
class KPost extends BaseEntity {
	static namingStrategy = new KindNamingStrategy();
	@PrimaryKey() declare id: string;
	@Column() declare userId: string;
}

@Entity("k_profiles")
class KProfile extends BaseEntity {
	static namingStrategy = new KindNamingStrategy();
	@PrimaryKey() declare id: string;
}

@Entity("k_countries")
class KCountry extends BaseEntity {
	static namingStrategy = new KindNamingStrategy();
	@PrimaryKey() declare id: string;
	@HasManyThrough(
		() => KPost,
		() => KUser,
	)
	declare posts: KPost[];
}

function db() {
	return wrapPrepareMock({
		prepare() {
			return { run: () => ({ changes: 0 }), all: () => [] };
		},
	});
}

const sqlFor = (
	build: (q: ReturnType<BaseRepository<KUser>["query"]>) => unknown,
): string => {
	const q = new BaseRepository(KUser, db()).query();
	build(q);
	return q.toSQL().sql;
};

describe("atlas > every relation passes its own kind", () => {
	it("hasMany asks as hasMany", () => {
		expect(sqlFor((q) => q.whereHas("posts"))).toContain("kuser_has_many");
	});

	it("hasOne asks as hasOne, not as hasMany", () => {
		// The `hasOne` and `hasMany` branches shared one call site, so `hasOne`
		// arrived as `hasMany` and a strategy distinguishing them was overruled.
		const sql = sqlFor((q) => q.whereHas("profile"));
		expect(sql).toContain("kuser_has_one");
		expect(sql).not.toContain("kuser_has_many");
	});

	it("belongsTo asks as belongsTo", () => {
		const sql = sqlFor((q) => q.whereHas("country"));
		expect(sql).toContain("kcountry_belongs_to");
		expect(sql).not.toContain("k_country_has_many");
	});

	it("hasManyThrough asks as hasManyThrough on BOTH hops", () => {
		// Two derived keys, and both used to be requested as `hasMany`.
		const sql = new BaseRepository(KCountry, db())
			.query()
			.whereHas("posts")
			.toSQL().sql;
		expect(sql).toContain("kcountry_has_many_through");
		expect(sql).toContain("kuser_has_many_through");
		// Not the flattened `hasMany` these two hops used to ask for.
		expect(sql).not.toMatch(/kcountry_has_many"/);
	});
});
