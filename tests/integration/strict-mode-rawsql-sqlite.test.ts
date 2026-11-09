/**
 * Strict mode covers every entry point of the model query that takes a
 * `db.raw()` fragment, not only the `*Raw` methods.
 *
 * `select(db.raw(...))`, `orderBy(db.raw(...))`, `update({ col: db.raw(...) })`
 * and a raw where value spliced SQL with strict mode on — an interpolated
 * `--` commented out the WHERE of an UPDATE. `db.ref()` stays allowed: it is
 * a validated, quoted identifier, not SQL.
 */
import "reflect-metadata";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasStrictMode } from "../../src/ModelQuery.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService, type DbService } from "../../src/services/db.js";

@Entity("w")
class W extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

const INPUT = "1) OR 1=1 --";

let conn: AsyncDatabaseConnection;
let db: DbService;
let repo: BaseRepository<W>;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE w (id INTEGER PRIMARY KEY, name TEXT)");
	await conn.execute("INSERT INTO w VALUES (1, 'a'), (2, 'b')");
	db = createDbService(() => conn);
	repo = new BaseRepository(W, conn);
});

afterEach(() => {
	setAtlasStrictMode(false);
});

afterAll(async () => {
	await conn.close();
});

describe("atlas > strict mode and db.raw() fragments", () => {
	it("refuses a raw fragment in select, orderBy, update and a where value", () => {
		setAtlasStrictMode(true);
		expect(() => repo.query().select(db.raw(`(SELECT ${INPUT}) AS x`))).toThrow(
			/strict mode/,
		);
		expect(() =>
			repo.query().orderBy(db.raw(`(CASE WHEN ${INPUT} END)`)),
		).toThrow(/strict mode/);
		expect(() =>
			repo
				.query()
				.where("id", 1)
				.update({ name: db.raw(`(SELECT ${INPUT})`) }),
		).toThrow(/strict mode/);
		expect(() => repo.query().where("name", db.raw(INPUT))).toThrow(
			/strict mode/,
		);
		expect(() => repo.query().whereNot("name", db.raw(INPUT))).toThrow(
			/strict mode/,
		);
	});

	it("leaves the rows alone when the update is refused", async () => {
		setAtlasStrictMode(true);
		await expect(async () =>
			repo
				.query()
				.where("id", 1)
				.update({ name: db.raw("'x'") }),
		).rejects.toThrow(/strict mode/);
		setAtlasStrictMode(false);
		const rows = await db.from("w").orderBy("id");
		expect(rows.map((r) => r.name)).toEqual(["a", "b"]);
	});

	it("still accepts db.ref() and plain values", async () => {
		setAtlasStrictMode(true);
		const rows = await repo.query().orderBy(db.ref("w.name"), "desc");
		expect(rows.map((r) => r.name)).toEqual(["b", "a"]);
		const byRef = await repo.query().where("name", db.ref("name"));
		expect(byRef).toHaveLength(2);
	});

	it("accepts the fragments when strict mode is off", async () => {
		const rows = await repo
			.query()
			.select("id", db.raw("id + 1 AS next"))
			.orderBy(db.raw("id"), "desc");
		expect(rows.map((r) => r.id)).toEqual([2, 1]);
	});
});
