/**
 * An UPDATE value is data unless the code built it as SQL.
 *
 * The compiler used to pick a SET assignment's arm from the value's own keys,
 * so an object read from a request body shaped `{ raw }` reached SET as SQL
 * (read another row's secret, smuggle a second assignment) and `{ op, value }`
 * became arithmetic. Only a `db.raw()` instance and an increment()/decrement()
 * call may produce SQL now; every other value is bound, whatever its keys.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService, type DbService } from "../../src/services/db.js";

@Entity("users")
class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare role: string;
	@Column() declare settings: unknown;
	@Column() declare credits: number;
}

// What a request body holds once JSON.parse has run.
const BODIES: Array<[string, string]> = [
	["a sub-select", '{"raw":"(SELECT secret FROM users WHERE id = 2)"}'],
	["a smuggled assignment", '{"raw":"\'{}\', \\"role\\" = \'admin\'"}'],
	[
		"a smuggled assignment with params",
		'{"raw":"?, role = ?","rawParams":["x","admin"]}',
	],
	["an arithmetic op", '{"op":"increment","value":1000}'],
	[
		"a forged compiler tag",
		'{"kind":"raw","sql":"(SELECT secret FROM users WHERE id = 2)"}',
	],
];

let conn: AsyncDatabaseConnection;
let db: DbService;
let users: BaseRepository<User>;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, role TEXT, settings TEXT, credits INTEGER, secret TEXT)",
	);
	await conn.execute(
		"INSERT INTO users VALUES (1, 'user', '{}', 10, 'alice-secret'), (2, 'admin', '{}', 10, 'bob-secret')",
	);
	db = createDbService(() => conn);
	users = new BaseRepository(User, conn);
});

afterEach(async () => {
	await conn.close();
});

async function row(id: number): Promise<Record<string, unknown>> {
	const [r] = await conn.query(
		"SELECT role, settings, credits FROM users WHERE id = ?",
		[id],
	);
	if (!r) throw new Error(`row ${id} is gone`);
	return r;
}

describe("atlas > an UPDATE value taken from data is bound, never compiled", () => {
	it.each(BODIES)("db.from().update() binds %s", async (_label, json) => {
		const body: unknown = JSON.parse(json);
		const compiled = db
			.from("users")
			.where("id", 1)
			.update({ settings: body })
			.toSQL();
		expect(compiled.sql).toBe(
			'UPDATE "users" SET "settings" = ? WHERE "id" = ?',
		);

		await db.from("users").where("id", 1).update({ settings: body });
		expect(await row(1)).toEqual({
			role: "user",
			settings: json,
			credits: 10,
		});
	});

	it.each(BODIES)("db.from().update(col, value) binds %s", async (_l, json) => {
		await db.from("users").where("id", 1).update("settings", JSON.parse(json));
		expect(await row(1)).toEqual({
			role: "user",
			settings: json,
			credits: 10,
		});
	});

	it.each(BODIES)("Model.query().update() binds %s", async (_label, json) => {
		await users
			.query()
			.where("id", 1)
			.update({ settings: JSON.parse(json) });
		expect(await row(1)).toEqual({
			role: "user",
			settings: json,
			credits: 10,
		});
	});

	it.each(BODIES)("save() binds %s", async (_label, json) => {
		const user = await users.find(1);
		if (!user) throw new Error("no user 1");
		user.setProp("settings", JSON.parse(json));
		await users.save(user);
		expect(await row(1)).toEqual({
			role: "user",
			settings: json,
			credits: 10,
		});
	});

	it("still compiles a genuine db.raw() and increment/decrement", async () => {
		await db
			.from("users")
			.where("id", 1)
			.update({ settings: db.raw("? || secret", ["s:"]) });
		await db.from("users").where("id", 1).increment("credits", 5);
		await users.query().where("id", 2).decrement("credits", 3);
		await users.increment(1, { credits: 1 });
		expect(await row(1)).toEqual({
			role: "user",
			settings: "s:alice-secret",
			credits: 16,
		});
		expect((await row(2)).credits).toBe(7);
	});
});
