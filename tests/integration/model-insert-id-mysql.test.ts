/**
 * The model write path on MySQL receives the AUTO_INCREMENT id, against a
 * REAL MySQL, gated on ATLAS_TEST_MYSQL_URL.
 *
 * MySQL has no RETURNING, and the repository dropped the id the driver
 * reported: a created entity was marked persisted with `id` undefined, so a
 * later save()/delete() threw E_MISSING_PRIMARY_KEY.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

const MYSQL_URL = process.env.ATLAS_TEST_MYSQL_URL ?? "";
const describeMysql = MYSQL_URL ? describe : describe.skip;

@Entity("insert_id_users")
class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

@Entity("insert_id_tags")
class Tag extends BaseEntity {
	@PrimaryKey() declare code: string;
	@Column() declare label: string;
}

describeMysql("atlas > MySQL model inserts receive the generated id", () => {
	let conn: AsyncDatabaseConnection;
	let users: BaseRepository<User>;
	let tags: BaseRepository<Tag>;

	beforeAll(async () => {
		setAtlasDialect("mysql");
		conn = await createNapiConnection(MYSQL_URL, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS insert_id_users");
		await conn.execute("DROP TABLE IF EXISTS insert_id_tags");
		await conn.execute(
			"CREATE TABLE insert_id_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)",
		);
		await conn.execute(
			"CREATE TABLE insert_id_tags (code VARCHAR(20) PRIMARY KEY, label VARCHAR(50) NOT NULL)",
		);
		users = new BaseRepository(User, conn);
		tags = new BaseRepository(Tag, conn);
	});

	beforeEach(async () => {
		await conn.execute("TRUNCATE TABLE insert_id_users");
		await conn.execute("TRUNCATE TABLE insert_id_tags");
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS insert_id_users");
		await conn.execute("DROP TABLE IF EXISTS insert_id_tags");
		await conn.close();
		setAtlasDialect("sqlite");
	});

	it("create() hydrates the id, and the entity can be saved and deleted", async () => {
		await users.create({ name: "first" });
		const user = await users.create({ name: "second" });
		expect(user.id).toBe(2);

		user.name = "renamed";
		await users.save(user);
		const rows = await conn.query(
			"SELECT id, name FROM insert_id_users ORDER BY id",
		);
		expect(rows).toEqual([
			{ id: 1, name: "first" },
			{ id: 2, name: "renamed" },
		]);

		await users.delete(user);
		expect(await conn.query("SELECT id FROM insert_id_users")).toEqual([
			{ id: 1 },
		]);
	});

	it("createMany() hydrates every id", async () => {
		const created = await users.createMany([{ name: "a" }, { name: "b" }]);
		expect(created.map((u) => u.id)).toEqual([1, 2]);
	});

	it("a key the app provides is kept, and no id is invented for a table without AUTO_INCREMENT", async () => {
		const tag = await tags.create({ code: "news", label: "News" });
		expect(tag.code).toBe("news");
		tag.label = "Latest";
		await tags.save(tag);
		expect(await conn.query("SELECT code, label FROM insert_id_tags")).toEqual([
			{ code: "news", label: "Latest" },
		]);
	});
});
