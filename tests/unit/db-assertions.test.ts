/**
 * The `db()` helix plugin (AdonisJS Lucid database-assertions parity): calling
 * the plugin registers a `db` assertion surface on the context. Exercised
 * against an in-memory SQLite connection.
 */

import { Emitter, type PluginApi, Runner } from "@c9up/helix/runtime";
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
		// A REAL PluginApi, not a partial one: the plugin takes Japa's shape and
		// a stand-in missing half of it only typechecks by lying.
		const api: PluginApi = {
			config: {},
			cliArgs: {},
			runner: new Runner(new Emitter()),
			emitter: new Emitter(),
			context: {
				macro(name: string, value: unknown) {
					if (name === "db") registered = value;
				},
				getter() {},
			},
			cleanup() {},
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

/**
 * The SQL the assertions emit, per dialect. fluveo hit both defects within
 * minutes of first use against Postgres and had to shim them app-side:
 * `?` markers gave "syntax error at or near AND", and an unmarked uuid column
 * gave "operator does not exist: uuid = text".
 */
describe("db assertions > dialect-aware SQL", () => {
	interface Recorded {
		sql: string;
		params: unknown[];
	}

	function recorder(dialect: string, catalog: Array<[string, string]> = []) {
		const calls: Recorded[] = [];
		const conn = {
			dialect,
			async query(sql: string, params: unknown[] = []) {
				calls.push({ sql, params });
				if (sql.includes("information_schema.columns")) {
					return catalog.map(([column_name, data_type]) => ({
						column_name,
						data_type,
					}));
				}
				return [{ c: 1 }];
			},
		};
		return { conn, calls };
	}

	it("numbers its placeholders on Postgres", async () => {
		const { conn, calls } = recorder("postgres");
		await createDbAssertions(conn).assertHas("users", {
			email: "a@b.c",
			status: "invited",
		});

		const count = calls.at(-1);
		expect(count?.sql).toContain('"email" = $1');
		expect(count?.sql).toContain('"status" = $2');
		expect(count?.sql).not.toContain("?");
		expect(count?.params).toEqual(["a@b.c", "invited"]);
	});

	it("keeps `?` on SQLite and MySQL", async () => {
		for (const dialect of ["sqlite", "mysql"]) {
			const { conn, calls } = recorder(dialect);
			await createDbAssertions(conn).assertHas("users", { email: "a@b.c" });
			expect(calls.at(-1)?.sql).toContain('"email" = ?');
			expect(calls.at(-1)?.sql).not.toContain("$1");
		}
	});

	it("casts a Postgres uuid column, because a text-bound param will not compare", async () => {
		const { conn, calls } = recorder("postgres", [
			["id", "uuid"],
			["company_id", "uuid"],
			["status", "character varying"],
		]);
		await createDbAssertions(conn).assertHas("users", {
			id: "2f1c…",
			company_id: "9a0b…",
			status: "invited",
		});

		const count = calls.at(-1);
		expect(count?.sql).toContain('"id" = $1::uuid');
		expect(count?.sql).toContain('"company_id" = $2::uuid');
		// A varchar column needs no cast: text binds compare fine.
		expect(count?.sql).toContain('"status" = $3');
		expect(count?.sql).not.toContain("$3::");
	});

	it("derives the cast from the COLUMN, never from the value", async () => {
		// A text column holding a UUID-shaped string must keep comparing as text;
		// guessing from the value would break it.
		const { conn, calls } = recorder("postgres", [["ref", "text"]]);
		await createDbAssertions(conn).assertHas("audit", {
			ref: "123e4567-e89b-12d3-a456-426614174000",
		});
		expect(calls.at(-1)?.sql).toContain('"ref" = $1');
		expect(calls.at(-1)?.sql).not.toContain("::uuid");
	});

	it("emits no cast when the catalog cannot be read", async () => {
		const conn = {
			dialect: "postgres",
			async query(sql: string) {
				if (sql.includes("information_schema")) throw new Error("no catalog");
				return [{ c: 1 }];
			},
		};
		// Failing introspection must not turn a passing assertion into an error.
		await expect(
			createDbAssertions(conn).assertHas("users", { id: "x" }),
		).resolves.toBeUndefined();
	});

	it("reads the catalog once per table", async () => {
		const { conn, calls } = recorder("postgres", [["id", "uuid"]]);
		const assertions = createDbAssertions(conn);
		await assertions.assertHas("users", { id: "a" });
		await assertions.assertHas("users", { id: "b" });

		const catalogReads = calls.filter((c) =>
			c.sql.includes("information_schema"),
		).length;
		expect(catalogReads).toBe(1);
	});
});
