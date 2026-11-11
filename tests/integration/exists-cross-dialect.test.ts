/**
 * Model.query().exists() selects the literal 1, not a column named "1".
 *
 * The literal went through the column quoting and compiled to `SELECT "1"`:
 * SQLite reads an unknown quoted identifier as a string, so the suite passed,
 * while Postgres answered `column "1" does not exist` and MySQL `Unknown
 * column '1'`. Runs on SQLite always, and on Postgres/MySQL when
 * ATLAS_TEST_PG_URL / ATLAS_TEST_MYSQL_URL are set.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";

@Entity("exists_items")
class Item extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

const TARGETS: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);
}

describe.each(TARGETS)("atlas > exists() on %s", (dialect, url) => {
	let conn: AsyncDatabaseConnection;
	let items: BaseRepository<Item>;

	beforeAll(async () => {
		setAtlasDialect(dialect);
		conn = await createNapiConnection(url, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS exists_items");
		await conn.execute(
			"CREATE TABLE exists_items (id INTEGER PRIMARY KEY, name VARCHAR(20))",
		);
		await conn.execute("INSERT INTO exists_items VALUES (1, 'a')");
		items = new BaseRepository(Item, conn);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS exists_items");
		await conn.close();
		setAtlasDialect("sqlite");
	});

	it("answers true and false against the real engine", async () => {
		setAtlasDialect(dialect);
		expect(await items.query().where("id", 1).exists()).toBe(true);
		expect(await items.query().where("id", 2).exists()).toBe(false);
		expect(await items.query().where("id", 2).doesntExist()).toBe(true);
	});
});
