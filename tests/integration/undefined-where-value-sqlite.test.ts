/**
 * A comparison whose value is `undefined` is refused, never dropped.
 *
 * `JSON.stringify` omits an `undefined` key, so the clause reached the
 * compiler without a value and vanished: `findBy('token', header)` with a
 * missing header returned the first user, and `where('token', undefined)
 * .delete()` emptied the table. The 3-argument form `where('id', '=',
 * undefined)` was read as `where('id', '=')`, comparing the column with '='.
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
	@Column() declare name: string;
	@Column() declare token: string;
	@Column() declare role: string;
}

let conn: AsyncDatabaseConnection;
let db: DbService;
let users: BaseRepository<User>;

// Values the type system cannot see are missing: a header, a body field.
const missing: string | undefined = JSON.parse("{}").token;
const absentId: number = JSON.parse("{}").id;
const absentList: Array<string | number> = JSON.parse("{}").ids;
const notAList: unknown[] = JSON.parse('"1, 2"');

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, token TEXT, role TEXT)",
	);
	await conn.execute(
		"INSERT INTO users VALUES (1, 'alice', 'ta', 'user'), (2, 'bob', 'tb', 'user')",
	);
	db = createDbService(() => conn);
	users = new BaseRepository(User, conn);
});

afterEach(async () => {
	await conn.close();
});

async function snapshot(): Promise<unknown[]> {
	return conn.query("SELECT id, name, role FROM users ORDER BY id");
}

const UNTOUCHED = [
	{ id: 1, name: "alice", role: "user" },
	{ id: 2, name: "bob", role: "user" },
];

const ATTACKS: Array<[string, () => PromiseLike<unknown>]> = [
	["repo.findBy(col, undefined)", () => users.findBy("token", missing)],
	["repo.findBy({ col: undefined })", () => users.findBy({ token: missing })],
	[
		"repo.findManyBy(col, undefined)",
		() => users.findManyBy("token", absentList),
	],
	[
		"repo.updateById(undefined)",
		() => users.updateById(absentId, { name: "pwned" }),
	],
	[
		"repo.updateWhere(col, undefined)",
		() => users.updateWhere("token", missing, { name: "pwned" }),
	],
	[
		"Model.query().where(col, undefined).delete()",
		() => users.query().where("token", missing).delete(),
	],
	[
		"Model.query().where(col, '=', undefined).delete()",
		() => users.query().where("id", "=", missing).delete(),
	],
	[
		"Model.query().whereNot(col, '=', undefined).delete()",
		() => users.query().whereNot("id", "=", missing).delete(),
	],
	[
		"Model.query().orWhere(col, undefined).update()",
		() =>
			users.query().where("id", 0).orWhere("token", missing).update({
				role: "admin",
			}),
	],
	[
		"Model.query().andWhere(col, '>', undefined).first()",
		() => users.query().andWhere("id", ">", missing).first(),
	],
	[
		"Model.query().whereIn(col, [undefined]).delete()",
		() => users.query().whereIn("id", [missing]).delete(),
	],
	[
		"db.from().where(col, undefined).delete()",
		() => db.from("users").where("token", missing).delete(),
	],
	[
		"db.from().where({ col: undefined }).delete()",
		() => db.from("users").where({ tenant_id: missing }).delete(),
	],
	[
		"db.from().where(col, '=', undefined).delete()",
		() => db.from("users").where("id", "=", missing).delete(),
	],
	[
		"db.from().whereNot(col, '=', undefined).delete()",
		() => db.from("users").whereNot("id", "=", missing).delete(),
	],
	[
		"db.from().whereIn(col, undefined).update()",
		() => db.from("users").whereIn("id", absentList).update({ role: "admin" }),
	],
	[
		"db.from().whereIn(col, non-array).update()",
		() => db.from("users").whereIn("id", notAList).update({ role: "admin" }),
	],
	[
		"db.from().having(col, '>', undefined)",
		() =>
			db
				.from("users")
				.select("role")
				.groupBy("role")
				.having("role", ">", missing),
	],
];

describe("atlas > an undefined WHERE value is refused", () => {
	it.each(ATTACKS)("%s throws and touches nothing", async (_label, run) => {
		const error: unknown = await Promise.resolve()
			.then(run)
			.then(
				() => undefined,
				(e: unknown) => e,
			);
		expect(error).toBeInstanceOf(Error);
		const code = error instanceof Error ? Reflect.get(error, "code") : "";
		const message = error instanceof Error ? error.message : "";
		expect(`${String(code)} ${message}`).toContain("E_ATLAS_UNDEFINED_BINDING");
		expect(await snapshot()).toEqual(UNTOUCHED);
	});

	it("names the code on the error", async () => {
		await expect(users.findBy("token", missing)).rejects.toMatchObject({
			code: "E_ATLAS_UNDEFINED_BINDING",
		});
	});

	it("still treats where(col, null) as IS NULL and a defined value as a filter", async () => {
		await conn.execute("UPDATE users SET token = NULL WHERE id = 2");
		const nulls = await users.query().where("token", null).exec();
		expect(nulls.map((u) => u.id)).toEqual([2]);
		const found = await users.findBy("token", "ta");
		expect(found?.id).toBe(1);
		expect(await db.from("users").where("id", "=", 1).delete()).toBe(1);
		expect(await snapshot()).toEqual([UNTOUCHED[1]]);
	});
});
