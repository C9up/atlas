/**
 * An insert that sets no column, and an insert with no row at all.
 *
 * `insert({})` compiled `INSERT INTO "t" () VALUES ()`, which Postgres and
 * SQLite reject, and the builder hid it by resolving `[]` without running
 * anything. The answer now follows the query builder Lucid sits on: one empty
 * row is a row of defaults in each dialect's own syntax (`DEFAULT VALUES`,
 * `() VALUES ()` on MySQL); an empty list, or several rows with no column, has
 * no statement and is refused with `E_ATLAS_EMPTY_INSERT`. A model saved with
 * nothing set used to be marked persisted without any INSERT; it now writes its
 * row of defaults, as a Lucid model does. An upsert with no conflict target
 * (`onConflict()`) never compiles the invalid `ON CONFLICT ()`: it takes the
 * dialect's bare form, and Postgres, which has none for DO UPDATE, refuses.
 *
 * Runs on SQLite always, and on Postgres/MySQL when ATLAS_TEST_PG_URL /
 * ATLAS_TEST_MYSQL_URL are set.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, errors, PrimaryKey } from "../../src/index.js";
import { setAtlasDialect } from "../../src/query/native.js";
import {
	clearDb,
	createDbService,
	type DbService,
	setDb,
} from "../../src/services/db.js";

interface Target {
	dialect: "sqlite" | "postgres" | "mysql";
	url: string;
	ddl: string;
	/** What `insert({}).toSQL().sql` reads in this dialect. */
	defaultsSql: string;
}

const TARGETS: Target[] = [
	{
		dialect: "sqlite",
		url: "sqlite::memory:",
		ddl: "CREATE TABLE ei_items (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL DEFAULT 'none', n INTEGER DEFAULT 7)",
		defaultsSql: 'INSERT INTO "ei_items" DEFAULT VALUES',
	},
];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push({
		dialect: "postgres",
		url: process.env.ATLAS_TEST_PG_URL,
		ddl: "CREATE TABLE ei_items (id SERIAL PRIMARY KEY, label TEXT NOT NULL DEFAULT 'none', n INT DEFAULT 7)",
		defaultsSql: 'INSERT INTO "ei_items" DEFAULT VALUES',
	});
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push({
		dialect: "mysql",
		url: process.env.ATLAS_TEST_MYSQL_URL,
		ddl: "CREATE TABLE ei_items (id INT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(20) NOT NULL DEFAULT 'none', n INT DEFAULT 7)",
		defaultsSql: "INSERT INTO `ei_items` () VALUES ()",
	});
}

class EiItem extends BaseModel {
	static override table = "ei_items";
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
	@Column() declare n: number | null;
}

describe.each(TARGETS)("atlas > empty inserts on $dialect", (target) => {
	let conn: AsyncDatabaseConnection;
	let db: DbService;

	const rows = async (): Promise<Array<{ label: unknown; n: unknown }>> =>
		conn.query<{ label: unknown; n: unknown }>(
			"SELECT label, n FROM ei_items ORDER BY id",
		);

	beforeAll(async () => {
		setAtlasDialect(target.dialect);
		conn = await createNapiConnection(target.url, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS ei_items");
		await conn.execute(target.ddl);
		db = createDbService(() => conn);
		setDb(conn);
	});

	afterAll(async () => {
		clearDb(conn);
		await conn.execute("DROP TABLE IF EXISTS ei_items");
		await conn.close();
	});

	beforeEach(async () => {
		await conn.execute("DELETE FROM ei_items");
	});

	it("insert({}) compiles a row of defaults in the dialect's syntax", () => {
		const compiled = db.table("ei_items").insert({}).toSQL();
		expect(compiled.sql).toBe(target.defaultsSql);
		expect(compiled.bindings).toEqual([]);
	});

	it("insert({}) writes one row of defaults", async () => {
		const result = await db.table("ei_items").insert({});
		expect(await rows()).toEqual([{ label: "none", n: 7 }]);
		// The id the database generated, where the driver reports one.
		if (target.dialect === "postgres") expect(result).toEqual([]);
		else expect(result).toEqual([expect.any(Number)]);
	});

	if (target.dialect !== "mysql") {
		it("insert({}).returning() hands back the row the defaults made", async () => {
			const result = await db
				.table("ei_items")
				.insert({})
				.returning("label", "n");
			expect(result).toEqual([{ label: "none", n: 7 }]);
		});
	}

	it("insert([]) and multiInsert([]) have no row and are refused", async () => {
		await expect(db.table("ei_items").insert([])).rejects.toBeInstanceOf(
			errors.E_ATLAS_EMPTY_INSERT,
		);
		await expect(db.table("ei_items").multiInsert([])).rejects.toBeInstanceOf(
			errors.E_ATLAS_EMPTY_INSERT,
		);
		expect(() => db.table("ei_items").multiInsert([]).toSQL()).toThrow(
			errors.E_ATLAS_EMPTY_INSERT,
		);
		expect(await rows()).toEqual([]);
	});

	it("several rows with no column are refused, as no statement exists for them", async () => {
		await expect(
			db.table("ei_items").multiInsert([{}, {}]),
		).rejects.toBeInstanceOf(errors.E_ATLAS_EMPTY_INSERT);
		expect(await rows()).toEqual([]);
	});

	it("one empty row in a list is a row of defaults", async () => {
		await db.table("ei_items").multiInsert([{}]);
		await db.table("ei_items").insert([{}]);
		expect(await rows()).toEqual([
			{ label: "none", n: 7 },
			{ label: "none", n: 7 },
		]);
	});

	it("insert(array) inserts every row, as multiInsert does", async () => {
		await db.table("ei_items").insert([{ label: "a" }, { label: "b", n: 2 }]);
		expect(await rows()).toEqual([
			{ label: "a", n: null },
			{ label: "b", n: 2 },
		]);
	});

	if (target.dialect === "sqlite") {
		it("an upsert of a row of defaults is refused: SQLite has no ON CONFLICT after DEFAULT VALUES", async () => {
			await expect(
				db.table("ei_items").insert({}).onConflict("id").ignore(),
			).rejects.toBeInstanceOf(errors.E_UNSUPPORTED);
			expect(await rows()).toEqual([]);
		});
	} else {
		it("an upsert of a row of defaults writes that row", async () => {
			await db.table("ei_items").insert({}).onConflict("id").ignore();
			expect(await rows()).toEqual([{ label: "none", n: 7 }]);
		});
	}

	it("onConflict() with no column ignores a conflict on any unique constraint", async () => {
		const builder = db
			.table("ei_items")
			.insert({ id: 1, label: "first" })
			.onConflict()
			.ignore();
		expect(builder.toSQL().sql).not.toContain("()");
		await builder;
		await db
			.table("ei_items")
			.insert({ id: 1, label: "second" })
			.onConflict()
			.ignore();
		expect(await rows()).toEqual([{ label: "first", n: 7 }]);
	});

	if (target.dialect === "postgres") {
		it("onConflict() with no column refuses merge(): Postgres needs the target to update", async () => {
			await expect(
				db.table("ei_items").insert({ id: 1, label: "a" }).onConflict().merge(),
			).rejects.toBeInstanceOf(errors.E_UNSUPPORTED);
			expect(() =>
				db
					.table("ei_items")
					.insert({ id: 1, label: "a" })
					.onConflict()
					.merge({ label: "b" })
					.toSQL(),
			).toThrow(errors.E_UNSUPPORTED);
			expect(await rows()).toEqual([]);
		});
	} else {
		it("onConflict() with no column merges on any unique constraint", async () => {
			await db.table("ei_items").insert({ id: 1, label: "first" });
			await db
				.table("ei_items")
				.insert({ id: 1, label: "second" })
				.onConflict()
				.merge(["label"]);
			expect(await rows()).toEqual([{ label: "second", n: 7 }]);
		});
	}

	it("a model saved with nothing set writes its row of defaults", async () => {
		const item = new EiItem();
		await item.save();
		expect(item.$isPersisted).toBe(true);
		expect(typeof item.id).toBe("number");
		const created = await EiItem.create({});
		expect(created.id).not.toBe(item.id);
		expect(await rows()).toEqual([
			{ label: "none", n: 7 },
			{ label: "none", n: 7 },
		]);
		const found = await EiItem.find(created.id);
		expect(found?.label).toBe("none");
	});

	it("createMany() of models with nothing set writes one row of defaults each", async () => {
		const created = await EiItem.createMany([{}, {}]);
		expect(created).toHaveLength(2);
		expect(created.every((m) => m.$isPersisted)).toBe(true);
		expect(new Set(created.map((m) => m.id)).size).toBe(2);
		expect(await rows()).toEqual([
			{ label: "none", n: 7 },
			{ label: "none", n: 7 },
		]);
	});

	it("createMany() of models that set different columns writes each row as given", async () => {
		const created = await EiItem.createMany([{ label: "a" }, { n: 3 }]);
		expect(created.map((m) => m.$isPersisted)).toEqual([true, true]);
		expect(await rows()).toEqual([
			{ label: "a", n: 7 },
			{ label: "none", n: 3 },
		]);
	});
});
