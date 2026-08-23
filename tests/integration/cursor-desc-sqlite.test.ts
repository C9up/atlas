/**
 * `cursorPaginate` hardcoded `asc`, so a newest-first feed — the common reason
 * to reach for a cursor at all — could not be paginated, and an `orderBy` set
 * on the query was silently overwritten.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Post extends BaseModel {
	static override table = "posts";
	@PrimaryKey() declare id: number;
	@Column() declare title: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)");
	for (let i = 1; i <= 5; i++) {
		await conn.execute("INSERT INTO posts VALUES (?, ?)", [i, `post ${i}`]);
	}
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > cursorPaginate direction", () => {
	it("walks newest-first when asked", async () => {
		const first = await Post.query().cursorPaginate({
			limit: 2,
			orderBy: "id",
			direction: "desc",
		});
		expect(first.items.map((p) => p.id)).toEqual([5, 4]);
		expect(first.hasMore).toBe(true);

		const second = await Post.query().cursorPaginate({
			limit: 2,
			orderBy: "id",
			direction: "desc",
			cursor: first.nextCursor ?? undefined,
		});
		expect(second.items.map((p) => p.id)).toEqual([3, 2]);
	});

	it("still walks ascending by default", async () => {
		const page = await Post.query().cursorPaginate({ limit: 2, orderBy: "id" });
		expect(page.items.map((p) => p.id)).toEqual([1, 2]);
	});

	it("reaches the end without offering another cursor", async () => {
		const page = await Post.query().cursorPaginate({
			limit: 10,
			orderBy: "id",
			direction: "desc",
		});
		expect(page.items.map((p) => p.id)).toEqual([5, 4, 3, 2, 1]);
		expect(page.hasMore).toBe(false);
		expect(page.nextCursor).toBeNull();
	});
});
