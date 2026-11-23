/**
 * Every refusal atlas raises carries a stable `code`, so a caller branches on
 * `error.code` instead of parsing the message. Each case below triggers the
 * real refusal and checks the code it carries.
 */

import "reflect-metadata";
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Exception } from "@c9up/flare";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { openFromConfig } from "../../src/ConnectionManager.js";
import {
	BelongsTo,
	Column,
	Entity,
	HasMany,
	HasOne,
	ManyToMany,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { addHook } from "../../src/decorators/hooks.js";
import { errors } from "../../src/index.js";
import { QueryBuilder } from "../../src/query/QueryBuilder.js";
import { verifySchema } from "../../src/schema/SchemaCheck.js";
import { readSchemaDumpManifest } from "../../src/schema/SchemaDumper.js";
import db, { createDbService } from "../../src/services/db.js";
import { createDbAssertions } from "../../src/testing/DbAssertions.js";
import { factory } from "../../src/testing/Factory.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("ec_users")
class EcUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@HasOne(() => EcProfile, { foreignKey: "userId" }) declare profile: unknown;
	@HasMany(() => EcPost, { foreignKey: "userId" }) declare posts: unknown[];
	@ManyToMany(() => EcTag, {
		pivotTable: "ec_user_tags",
		foreignKey: "user_id",
		otherKey: "tag_id",
	})
	declare tags: unknown[];
	static scopes = {
		named: (q: { where: (c: string, v: string) => unknown }) =>
			q.where("name", "a"),
	};
}

@Entity("ec_profiles")
class EcProfile extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare userId: number;
}

@Entity("ec_posts")
class EcPost extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare userId: number;
	@BelongsTo(() => EcUser, { foreignKey: "userId" }) declare user: unknown;
}

@Entity("ec_tags")
class EcTag extends BaseEntity {
	@PrimaryKey() declare id: number;
}

@Entity("ec_guarded")
class EcBoth extends BaseEntity {
	static fillable = ["name"];
	static guarded = ["id"];
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

let conn: Awaited<ReturnType<typeof createNapiConnection>>;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE ec_users (id INTEGER PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE ec_profiles (id INTEGER PRIMARY KEY, user_id INTEGER)",
	);
	await conn.execute(
		"CREATE TABLE ec_posts (id INTEGER PRIMARY KEY, user_id INTEGER)",
	);
	await conn.execute("CREATE TABLE ec_tags (id INTEGER PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE ec_user_tags (user_id INTEGER, tag_id INTEGER)",
	);
	await conn.execute(
		"INSERT INTO ec_users (id, name) VALUES (1, 'a'), (2, 'a')",
	);
	await conn.execute(
		"INSERT INTO ec_profiles (id, user_id) VALUES (1, 1), (2, 1)",
	);
	await conn.execute("INSERT INTO ec_posts (id, user_id) VALUES (1, 1)");
});

afterAll(async () => {
	await conn.close();
});

function users() {
	return new BaseRepository(EcUser, conn, { dialect: "sqlite" });
}

describe("atlas > error codes > connections and services", () => {
	it("E_ATLAS_NO_URL: a connection config without a URL", async () => {
		await expect(openFromConfig("main", {})).rejects.toMatchObject({
			code: "E_ATLAS_NO_URL",
		});
	});

	it("E_ATLAS_NOT_BOOTED: the db service before the provider booted", () => {
		expect(() => db.from("ec_users")).toThrow(
			expect.objectContaining({ code: "E_ATLAS_NOT_BOOTED" }),
		);
	});

	it("E_ATLAS_READ_ONLY: a write on a read-mode builder", () => {
		const reader = createDbService(() => conn, true);
		expect(() => reader.from("ec_users").where("id", 1).delete()).toThrow(
			expect.objectContaining({ code: "E_ATLAS_READ_ONLY" }),
		);
	});

	it("E_ATLAS_INVALID_PRAGMA: a sqlite pragma key outside the safe alphabet", async () => {
		await expect(
			createNapiConnection("sqlite::memory:", 1, 1, { "journal_mode; --": 1 }),
		).rejects.toMatchObject({ code: "E_ATLAS_INVALID_PRAGMA" });
	});
});

describe("atlas > error codes > entities", () => {
	it("E_ATLAS_NO_REPOSITORY: refresh() on an entity no repository hydrated", async () => {
		await expect(new EcUser().refresh()).rejects.toMatchObject({
			code: "E_ATLAS_NO_REPOSITORY",
		});
	});

	it("E_ATLAS_FILLABLE_AND_GUARDED: a class declaring both lists", () => {
		expect(() => new EcBoth().fill({ name: "x" })).toThrow(
			expect.objectContaining({ code: "E_ATLAS_FILLABLE_AND_GUARDED" }),
		);
	});

	it("E_ATLAS_INVALID_HOOK: a runtime hook that is not a function", () => {
		expect(() => addHook(EcUser, "beforeSave", Reflect.get({}, "x"))).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_HOOK" }),
		);
	});
});

describe("atlas > error codes > model queries", () => {
	it("E_ATLAS_MULTIPLE_ROWS: sole() matching two rows", async () => {
		await expect(
			users().query().where("name", "a").sole(),
		).rejects.toMatchObject({ code: "E_ATLAS_MULTIPLE_ROWS" });
	});

	it("E_ATLAS_HAS_ONE_MULTIPLE_ROWS: a @HasOne preload finding two rows", async () => {
		await expect(
			users().query().where("id", 1).preload("profile").exec(),
		).rejects.toMatchObject({ code: "E_ATLAS_HAS_ONE_MULTIPLE_ROWS" });
	});

	it("E_UNDEFINED_RELATIONSHIP: withCount() on a relation the model lacks", () => {
		expect(() => users().query().withCount("ghost").toSQL()).toThrow(
			expect.objectContaining({ code: "E_UNDEFINED_RELATIONSHIP" }),
		);
	});

	it("E_ATLAS_INVALID_AGGREGATE: withAggregate() whose callback sets none", () => {
		expect(() =>
			users()
				.query()
				.withAggregate("posts", (q) => q)
				.toSQL(),
		).toThrow(expect.objectContaining({ code: "E_ATLAS_INVALID_AGGREGATE" }));
	});

	it("E_ATLAS_INVALID_EXPRESSION: whereExpr() with forbidden characters", () => {
		expect(() => users().query().whereExpr("id", "id; DROP", ">", 1)).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_EXPRESSION" }),
		);
	});

	it("E_ATLAS_INVALID_JSON_PATH: whereJsonPath() with a path not rooted at $", () => {
		expect(() => users().query().whereJsonPath("name", "a.b", 1)).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_JSON_PATH" }),
		);
	});

	it("E_ATLAS_INVALID_GROUP_LIMIT: a negative groupLimit()", () => {
		expect(() => users().query().groupLimit(-1)).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_GROUP_LIMIT" }),
		);
	});

	it("E_ATLAS_INVALID_LOCK: skipLocked() without a base lock", () => {
		const pg = new BaseRepository(EcUser, conn, { dialect: "postgres" });
		expect(() => pg.query().skipLocked().toSQL()).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_LOCK" }),
		);
	});

	it("E_ATLAS_INVALID_COMMENT: a comment that would close the SQL comment", () => {
		expect(() => users().query().comment("x */ DROP")).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_COMMENT" }),
		);
		expect(() => fromUsers().comment("x */ DROP")).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_COMMENT" }),
		);
	});

	it("E_ATLAS_INVALID_ALIAS: a subquery alias that is not an identifier", () => {
		const sub = users().query().select("id").as("bad alias");
		expect(() => users().query().select(sub).toSQL()).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_ALIAS" }),
		);
	});

	it("E_ATLAS_SUBQUERY_ALIAS: a subquery selected without .as()", () => {
		const sub = users().query().select("id");
		expect(() => users().query().select(sub).toSQL()).toThrow(
			expect.objectContaining({ code: "E_ATLAS_SUBQUERY_ALIAS" }),
		);
	});

	it("E_ATLAS_UNKNOWN_SCOPE: apply() reaching a scope the model lacks", () => {
		expect(() =>
			users()
				.query()
				.apply((scopes) => Reflect.get(scopes, "ghost")),
		).toThrow(expect.objectContaining({ code: "E_ATLAS_UNKNOWN_SCOPE" }));
	});

	it("E_ATLAS_CURSOR_ORDER_REQUIRED: cursorPaginate() without an order column", async () => {
		await expect(
			users().query().cursorPaginate({ limit: 10, orderBy: [] }),
		).rejects.toMatchObject({ code: "E_ATLAS_CURSOR_ORDER_REQUIRED" });
	});

	it("E_ATLAS_INVALID_CURSOR: a cursor that does not decode", async () => {
		await expect(
			users()
				.query()
				.cursorPaginate({ cursor: "!!!", limit: 10, orderBy: ["id"] }),
		).rejects.toMatchObject({ code: "E_ATLAS_INVALID_CURSOR" });
	});

	it("E_ATLAS_EMPTY_UPDATE: update() with an empty payload", () => {
		expect(() => users().query().update({})).toThrow(
			expect.objectContaining({ code: "E_ATLAS_EMPTY_UPDATE" }),
		);
	});

	it("E_ATLAS_INVALID_PLUCK: pluck() reading an object value", async () => {
		const repo = new BaseRepository(
			EcUser,
			wrapPrepareMock({
				prepare() {
					return {
						run: () => ({ changes: 0 }),
						all: () => [{ name: { a: 1 } }],
					};
				},
			}),
			{ dialect: "sqlite" },
		);
		await expect(repo.query().pluck("name")).rejects.toMatchObject({
			code: "E_ATLAS_INVALID_PLUCK",
		});
	});

	it("E_ATLAS_INVALID_JOIN: a string join missing its right operand", () => {
		const query = users().query();
		expect(() => Reflect.apply(query.join, query, ["ec_posts", "id"])).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_JOIN" }),
		);
	});

	it("E_ATLAS_QUERY_TIMEOUT: a query outrunning timeout()", async () => {
		await expect(
			createDbService(() => conn)
				.rawQuery(
					"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3000000) SELECT count(*) AS n FROM c",
				)
				.timeout(1),
		).rejects.toMatchObject({ code: "E_ATLAS_QUERY_TIMEOUT" });
	});
});

describe("atlas > error codes > relations", () => {
	it("E_ATLAS_RELATION_UNSUPPORTED_OPERATION: create() through a @BelongsTo", async () => {
		const post = await new BaseRepository(EcPost, conn, {
			dialect: "sqlite",
		}).findOrFail(1);
		await expect(
			post.related("user").create({ name: "x" }),
		).rejects.toMatchObject({ code: "E_ATLAS_RELATION_UNSUPPORTED_OPERATION" });
	});

	it("E_ATLAS_ASSOCIATE_NULL: associate() given no model", async () => {
		const post = await new BaseRepository(EcPost, conn, {
			dialect: "sqlite",
		}).findOrFail(1);
		const related = post.related("user");
		if (!("associate" in related)) throw new Error("expected a @BelongsTo");
		await expect(
			Reflect.apply(related.associate, related, [null]),
		).rejects.toMatchObject({ code: "E_ATLAS_ASSOCIATE_NULL" });
	});

	it("E_ATLAS_PIVOT_KEY_COLLISION: attach() extras naming a pivot key", async () => {
		const user = await users().findOrFail(1);
		const tags = user.related("tags");
		if (!("attach" in tags)) throw new Error("expected a @ManyToMany");
		await expect(tags.attach({ 7: { tag_id: 9 } })).rejects.toMatchObject({
			code: "E_ATLAS_PIVOT_KEY_COLLISION",
		});
	});
});

describe("atlas > error codes > testing helpers", () => {
	it("E_ATLAS_DB_ASSERTION: a database assertion that does not hold", async () => {
		await expect(
			createDbAssertions(conn).assertHas("ec_users", { name: "nobody" }),
		).rejects.toMatchObject({ code: "E_ATLAS_DB_ASSERTION" });
	});

	it("E_ATLAS_FACTORY_NO_CONNECTION: create() with no connection to write to", async () => {
		await expect(
			factory(EcUser, () => ({ name: "x" })).create(),
		).rejects.toMatchObject({ code: "E_ATLAS_FACTORY_NO_CONNECTION" });
	});

	it("E_ATLAS_FACTORY_STATE: a state the factory does not define", () => {
		expect(() =>
			factory(EcUser, () => ({ name: "x" }))
				.apply("ghost")
				.make(),
		).toThrow(expect.objectContaining({ code: "E_ATLAS_FACTORY_STATE" }));
	});

	it("E_ATLAS_FACTORY_UNKNOWN_RELATION: with() naming no declared relation", async () => {
		await expect(
			factory(EcUser, () => ({ name: "x" }))
				.with("ghost")
				.create(conn),
		).rejects.toMatchObject({ code: "E_ATLAS_FACTORY_UNKNOWN_RELATION" });
	});
});

describe("atlas > error codes > schema tooling", () => {
	it("E_ATLAS_SCHEMA_MISMATCH: verifySchema() finding drift", async () => {
		@Entity("ec_missing_table")
		class Missing extends BaseEntity {
			@PrimaryKey() declare id: number;
		}
		await expect(verifySchema([Missing], conn, "sqlite")).rejects.toMatchObject(
			{ code: "E_ATLAS_SCHEMA_MISMATCH" },
		);
	});

	it("E_ATLAS_SCHEMA_DUMP_MANIFEST: a malformed schema-dump manifest", async () => {
		const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-ec-"));
		try {
			await fsp.writeFile(
				path.join(dir, "schema.meta.json"),
				JSON.stringify({ version: 2 }),
			);
			await expect(
				readSchemaDumpManifest(path.join(dir, "schema.sql")),
			).rejects.toMatchObject({ code: "E_ATLAS_SCHEMA_DUMP_MANIFEST" });
		} finally {
			await fsp.rm(dir, { recursive: true, force: true });
		}
	});
});

describe("atlas > error codes > class", () => {
	it("the entry exports the errors namespace, each a flare Exception", () => {
		let caught: unknown;
		try {
			users().query().groupLimit(-1);
		} catch (err) {
			caught = err;
		}
		expect(caught).toBeInstanceOf(errors.E_ATLAS_INVALID_GROUP_LIMIT);
		expect(caught).toBeInstanceOf(Exception);
		expect(caught).toMatchObject({
			code: "E_ATLAS_INVALID_GROUP_LIMIT",
			status: 500,
		});
	});

	it("a Rust compiler refusal becomes the class its code names", () => {
		let caught: unknown;
		try {
			new QueryBuilder('orders"; DROP TABLE orders--').toSQL();
		} catch (err) {
			caught = err;
		}
		expect(caught).toBeInstanceOf(errors.E_UNSAFE_IDENTIFIER);
		expect(caught).toMatchObject({ code: "E_UNSAFE_IDENTIFIER", status: 500 });
		if (!(caught instanceof Error)) throw caught;
		expect(caught.message).not.toMatch(/^E_UNSAFE_IDENTIFIER/);
		expect(caught.cause).toBeInstanceOf(Error);
		expect(String(Reflect.get(Object(caught.cause), "message"))).toMatch(
			/^E_UNSAFE_IDENTIFIER: /,
		);
	});

	it("E_ROW_NOT_FOUND answers 404, keeps the model and criteria, and says neither", () => {
		class User {}
		const error = new errors.E_ROW_NOT_FOUND(User, { email: "a@b.test" });
		expect(error).toBeInstanceOf(Exception);
		expect(error).toMatchObject({
			code: "E_ROW_NOT_FOUND",
			status: 404,
			criteria: { email: "a@b.test" },
			message: "Row not found",
		});
		expect(error.model).toBe(User);
	});

	it("words the upstream codes as upstream does", () => {
		expect(new errors.E_MODEL_DELETED().message).toBe(
			"Cannot mutate delete model instance",
		);
		expect(new errors.E_UNMANAGED_DB_CONNECTION(["replica"]).message).toBe(
			"Cannot connect to unregistered connection replica",
		);
		expect(new errors.E_UNDEFINED_RELATIONSHIP(["posts", "User"]).message).toBe(
			'"posts" is not defined as a relationship on "User" model',
		);
		expect(
			new errors.E_INVALID_DATE_COLUMN_VALUE(["User.bornAt", "Not a date."])
				.message,
		).toBe('Invalid value for "User.bornAt". Not a date.');
		expect(new errors.E_UNABLE_ACQUIRE_LOCK().message).toBe(
			"Unable to acquire lock. Concurrent migrations are not allowed",
		);
	});
});

function fromUsers() {
	return createDbService(() => conn).from("ec_users");
}
