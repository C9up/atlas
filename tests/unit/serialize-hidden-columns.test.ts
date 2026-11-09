/**
 * A column hidden with `serializeAs: null` stays hidden when a relation is
 * trimmed with `serialize({ relations: { x: { fields } } })` and in a
 * `Paginator.serialize({ fields })`.
 *
 * Both paths used to read the raw entity — the related instance, the paginator
 * row — so a password hash, `$extras` and `$original` came back to whoever
 * asked for them by name or by omission.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import {
	BelongsTo,
	Column,
	HasMany,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { Paginator } from "../../src/ModelQuery.js";

class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column({ serializeAs: null }) declare password: string;
	@Column({ serializeAs: "display_name" }) declare displayName: string;
	@HasMany(() => Post) declare posts: Post[];
}

class Post extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare userId: number;
	@Column({ serializeAs: null }) declare secretNote: string;
	@BelongsTo(() => User) declare author: User;
}

function makeUser(): User {
	const user = new User();
	user.id = 1;
	user.email = "a@b.c";
	user.password = "$argon2id$hash";
	user.displayName = "Ada";
	return user;
}

function makePost(id: number, author: User): Post {
	const post = new Post();
	post.id = id;
	post.userId = author.id;
	post.secretNote = "internal";
	post.author = author;
	return post;
}

const LEAKS = ["password", "secretNote", "$extras", "$original", "$sideloaded"];

function assertNoLeak(value: unknown): void {
	const text = JSON.stringify(value);
	for (const key of LEAKS) expect(text).not.toContain(`"${key}"`);
	expect(text).not.toContain("$argon2id$hash");
	expect(text).not.toContain("internal");
}

describe("atlas > serializeAs: null survives every serialization path", () => {
	it("a belongsTo relation picked by name cannot select a hidden column", () => {
		const post = makePost(9, makeUser());
		const out = post.serialize({
			relations: { author: { fields: ["id", "password"] } },
		});
		expect(out.author).toEqual({ id: 1 });
		assertNoLeak(out);
	});

	it("a relation trimmed by omission exposes neither hidden columns nor internals", () => {
		const post = makePost(9, makeUser());
		const out = post.serialize({
			relations: { author: { fields: { omit: ["email"] } } },
		});
		expect(out.author).toEqual({ id: 1, display_name: "Ada" });
		assertNoLeak(out);
	});

	it("a hasMany relation and a nested relation two levels deep stay serialized", () => {
		const user = makeUser();
		user.posts = [makePost(1, makeUser()), makePost(2, makeUser())];
		const out = user.serialize({
			fields: { omit: ["email"] },
			relations: {
				posts: {
					fields: { omit: ["userId"] },
					relations: { author: { fields: { pick: ["display_name"] } } },
				},
			},
		});
		expect(out.posts).toEqual([
			{ id: 1, author: { display_name: "Ada" } },
			{ id: 2, author: { display_name: "Ada" } },
		]);
		assertNoLeak(out);
	});

	it("Paginator.serialize({ fields }) goes through the entity serializer", () => {
		const paginator = new Paginator([makeUser()], {
			total: 1,
			perPage: 10,
			currentPage: 1,
		});
		const picked = paginator.serialize({ fields: ["id", "password"] });
		expect(picked.data).toEqual([{ id: 1 }]);
		const renamed = paginator.serialize({ fields: ["display_name"] });
		expect(renamed.data).toEqual([{ display_name: "Ada" }]);
		const whole = paginator.serialize();
		assertNoLeak(whole);
		expect(whole.data).toEqual([
			{ id: 1, email: "a@b.c", display_name: "Ada" },
		]);
	});

	it("Paginator.serialize takes relations too", () => {
		const user = makeUser();
		const post = new Post();
		post.id = 3;
		post.userId = 1;
		post.secretNote = "internal";
		user.posts = [post];
		const paginator = new Paginator([user], {
			total: 1,
			perPage: 10,
			currentPage: 1,
		});
		const out = paginator.serialize({
			fields: ["id"],
			relations: { posts: { fields: { omit: ["secretNote"] } } },
		});
		expect(out.data).toEqual([{ id: 1, posts: [{ id: 3, userId: 1 }] }]);
		assertNoLeak(out);
	});

	it("plain rows in a paginator are still picked", () => {
		const paginator = new Paginator([{ id: 1, name: "x", other: 2 }], {
			total: 1,
			perPage: 10,
			currentPage: 1,
		});
		expect(paginator.serialize({ fields: ["id", "name"] }).data).toEqual([
			{ id: 1, name: "x" },
		]);
	});
});
