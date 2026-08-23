/**
 * Lucid surface that atlas was missing: clearing clauses on a shared builder,
 * HAVING EXISTS, self-assigned primary keys, the AdonisJS pivot option names,
 * and cherry-picked serialization including relations.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import { DatabaseQueryBuilder } from "../../src/query/DatabaseQueryBuilder.js";

const builder = () =>
	new DatabaseQueryBuilder(
		{
			query: async () => [],
			execute: async () => ({ rowsAffected: 0 }),
		} as never,
		"sqlite",
	).from("users");

describe("atlas > clearing clauses", () => {
	it("drops each clause independently", () => {
		const q = builder()
			.select("id", "email")
			.where("active", true)
			.orderBy("id")
			.limit(10)
			.offset(20);

		expect(q.toSQL().sql).toContain("LIMIT");
		q.clearSelect().clearWhere().clearOrder().clearLimit().clearOffset();

		const sql = q.toSQL().sql;
		expect(sql).not.toContain("WHERE");
		expect(sql).not.toContain("ORDER BY");
		expect(sql).not.toContain("LIMIT");
		expect(sql).not.toContain("OFFSET");
		// Back to selecting everything.
		expect(sql).toContain("*");
	});

	it("clears HAVING without touching WHERE", () => {
		const q = builder()
			.where("active", true)
			.groupBy("role")
			.having("total", ">", 2);
		q.clearHaving();
		const sql = q.toSQL().sql;
		expect(sql).toContain("WHERE");
		expect(sql).not.toContain("HAVING");
	});
});

describe("atlas > HAVING EXISTS", () => {
	it("filters groups by a correlated subquery", () => {
		const q = builder()
			.select("role")
			.groupBy("role")
			.havingExists((sub) =>
				sub.from("orders").whereRaw("orders.role = users.role"),
			);
		const sql = q.toSQL().sql;
		expect(sql).toContain("HAVING");
		expect(sql).toContain("EXISTS");
		expect(sql).toContain("orders");
	});

	it("negates and ORs", () => {
		const sql = builder()
			.groupBy("role")
			.havingNotExists((sub) => sub.from("bans"))
			.orHavingExists((sub) => sub.from("orders"))
			.toSQL().sql;
		expect(sql).toContain("NOT EXISTS");
		expect(sql).toContain("OR");
	});
});

class Post extends BaseEntity {
	declare id: number;
	declare title: string;
	declare secret: string;
}

describe("atlas > cherry-picked serialization", () => {
	const post = () => {
		const p = new Post();
		p.id = 1;
		p.title = "Hello";
		p.secret = "s3cret";
		return p;
	};

	it("picks a list of fields", () => {
		expect(post().serialize({ fields: ["id", "title"] })).toEqual({
			id: 1,
			title: "Hello",
		});
	});

	it("omits what should never travel", () => {
		const out = post().serialize({ fields: { omit: ["secret"] } });
		expect(out).not.toHaveProperty("secret");
		expect(out).toMatchObject({ id: 1, title: "Hello" });
	});

	it("trims relations in the same pass", () => {
		const author = new Post();
		author.id = 9;
		author.title = "Ada";
		author.secret = "nope";
		const p = post();
		// A preloaded relation, as a serialized graph carries it.
		(p as unknown as Record<string, unknown>).author = author;

		const out = p.serialize({
			fields: ["id", "author"],
			relations: { author: { fields: ["id"] } },
		});
		expect(out).toEqual({ id: 1, author: { id: 9 } });
	});

	it("trims a has-many relation element by element", () => {
		const p = post();
		const child = new Post();
		child.id = 2;
		child.title = "Child";
		child.secret = "x";
		(p as unknown as Record<string, unknown>).comments = [child];

		const out = p.serialize({
			fields: ["id", "comments"],
			relations: { comments: { fields: ["id", "title"] } },
		});
		expect(out).toEqual({ id: 1, comments: [{ id: 2, title: "Child" }] });
	});

	it("returns everything when nothing is asked", () => {
		expect(post().serialize()).toMatchObject({ id: 1, secret: "s3cret" });
	});
});
