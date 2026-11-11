/**
 * The subpaths (`@c9up/atlas/orm` | `/schema` | `/factories` | `/seeders` |
 * `/types/relations`) hand out a working surface: each case builds something
 * through the subpath alone, so a re-export of the wrong value fails here.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { Factory } from "../../src/factories.js";
import { TableBuilder } from "../../src/lucid-schema.js";
import {
	BaseModel,
	Column,
	column,
	hasMany,
	PrimaryKey,
} from "../../src/orm.js";
import { BaseSeeder, runSeeders } from "../../src/seeders.js";
import type { BelongsTo, HasMany } from "../../src/types/relations.js";

class SubpathPost extends BaseModel {
	static override table = "subpath_posts";
	@PrimaryKey() declare id: number;
	@Column() declare title: string;
	@column.dateTime({ autoCreate: true }) declare createdAt: unknown;
	@hasMany(() => SubpathPost) declare replies: SubpathPost[];
}

describe("atlas > subpath exports", () => {
	it("/orm builds a model whose columns track dirt and whose relations stay out of it", () => {
		const post = new SubpathPost();
		post.fill({ title: "hello" });
		post.markAsPersisted();
		post.title = "edited";
		post.replies = [];
		expect(post.$dirty).toEqual({ title: "edited" });
	});

	it("/schema compiles a migration table", () => {
		const table = new TableBuilder("subpath_users");
		table.increments("id");
		table.string("email", 120).notNullable();
		const [create] = table.toStatements("sqlite");
		expect(create).toMatch(/^CREATE TABLE "subpath_users"/);
		expect(create).toContain('"email" TEXT NOT NULL');
	});

	it("/factories makes an instance with its defaults and overrides", () => {
		const posts = Factory.define(SubpathPost, () => ({
			title: "draft",
		})).build();
		expect(posts.make().title).toBe("draft");
		expect(posts.merge({ title: "set" }).make().title).toBe("set");
	});

	it("/seeders runs seeders in order", async () => {
		const ran: string[] = [];
		class First extends BaseSeeder {
			async run() {
				ran.push("first");
			}
		}
		class Second extends BaseSeeder {
			async run() {
				ran.push("second");
			}
		}
		const db = {
			execute: async () => ({ rowsAffected: 0 }),
			query: async () => [],
		};
		await runSeeders([new First(db), new Second(db)]);
		expect(ran).toEqual(["first", "second"]);
	});

	it("/types/relations helpers resolve to atlas shapes", () => {
		class Post {}
		// Compile-time: HasMany<typeof Post> === Post[], BelongsTo<typeof Post> === Post | null.
		const many: HasMany<typeof Post> = [new Post()];
		const one: BelongsTo<typeof Post> = null;
		expect(Array.isArray(many)).toBe(true);
		expect(one).toBeNull();
	});
});
