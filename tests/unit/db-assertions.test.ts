/**
 * The `db()` helix plugin (AdonisJS Lucid database-assertions parity): calling
 * the plugin registers a `db` assertion surface on the context. Exercised
 * against an in-memory SQLite connection via a mock PluginApi.
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";
import { createDbAssertions, db } from "../../src/testing/DbAssertions.js";

let conn: Awaited<ReturnType<typeof createNapiConnection>>;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, active INTEGER)",
	);
	await conn.execute("INSERT INTO users (email, active) VALUES (?, ?)", [
		"a@b.c",
		1,
	]);
	await conn.execute("INSERT INTO users (email, active) VALUES (?, ?)", [
		"d@e.f",
		0,
	]);
});

afterAll(async () => {
	await conn.close();
});

describe("helix plugin > db()", () => {
	it("the plugin registers `db` on the context", async () => {
		let registered: unknown;
		const api = {
			context: {
				macro(name: string, value: unknown) {
					if (name === "db") registered = value;
				},
				getter() {},
			},
		};
		await db(conn)(api);
		expect(registered).toBeDefined();
	});

	it("assertHas / assertMissing match on bound params", async () => {
		const a = createDbAssertions(conn);
		await a.assertHas("users", { email: "a@b.c" });
		await a.assertHas("users", { email: "a@b.c", active: 1 });
		await a.assertMissing("users", { email: "nobody@x.y" });
		await expect(
			a.assertHas("users", { email: "nobody@x.y" }),
		).rejects.toThrow();
		await expect(
			a.assertMissing("users", { email: "a@b.c" }),
		).rejects.toThrow();
	});

	it("assertCount (total + scoped) and assertEmpty", async () => {
		const a = createDbAssertions(conn);
		await a.assertCount("users", 2);
		await a.assertCount("users", 1, { active: 1 });
		await expect(a.assertCount("users", 5)).rejects.toThrow();
		await expect(a.assertEmpty("users")).rejects.toThrow();
	});

	it("rejects an unsafe identifier instead of injecting", async () => {
		const a = createDbAssertions(conn);
		await expect(a.assertHas("users; DROP TABLE users", {})).rejects.toThrow(
			/unsafe identifier/,
		);
	});
});
