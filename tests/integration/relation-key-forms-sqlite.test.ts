/**
 * A relation key may be written as the property (`folderId`) or as the column
 * (`folder_id`), and both must load the same rows.
 *
 * `@HasMany(() => Note, { foreignKey: 'folderId' })` used to compile
 * `WHERE folder_id IN (…)` — the query resolves columns — and then group the
 * raw rows by `row.folderId`, which no row has. Every folder got `[]`, with no
 * error. `whereHas` wrote the key verbatim into its correlated subquery, and
 * the through relations read raw rows the same way.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import {
	BelongsTo,
	Column,
	Entity,
	HasMany,
	HasManyThrough,
	HasOne,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("rk_notes")
class Note extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare folderId: number;
	@Column() declare body: string;
}

@Entity("rk_covers")
class Cover extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare folderId: number;
}

@Entity("rk_folders")
class Folder extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare shelfId: number;
	@HasMany(() => Note, { foreignKey: "folderId" })
	declare notesByProperty: Note[];
	@HasMany(() => Note, { foreignKey: "folder_id" })
	declare notesByColumn: Note[];
	@HasOne(() => Cover, { foreignKey: "folderId" })
	declare cover: Cover | null;
}

@Entity("rk_shelves")
class Shelf extends BaseEntity {
	@PrimaryKey() declare id: number;
	@HasManyThrough(
		() => Note,
		() => Folder,
		{
			firstKey: "shelfId",
			secondKey: "folderId",
		},
	)
	declare notes: Note[];
}

@Entity("rk_stickers")
class Sticker extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column({ columnName: "holder_folder" }) declare folderRef: number;
	// Neither `holderId` (relation + `Id`) nor the default `folder_id` exists:
	// only the declared key, in its column form, names the right field.
	@BelongsTo(() => Folder, { foreignKey: "holder_folder" })
	declare holder: Folder | null;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE rk_shelves (id INTEGER PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE rk_folders (id INTEGER PRIMARY KEY, shelf_id INTEGER)",
	);
	await conn.execute(
		"CREATE TABLE rk_notes (id INTEGER PRIMARY KEY, folder_id INTEGER, body TEXT)",
	);
	await conn.execute(
		"CREATE TABLE rk_covers (id INTEGER PRIMARY KEY, folder_id INTEGER)",
	);
	await conn.execute(
		"CREATE TABLE rk_stickers (id INTEGER PRIMARY KEY, holder_folder INTEGER)",
	);
	await conn.execute("INSERT INTO rk_shelves (id) VALUES (1)");
	await conn.execute(
		"INSERT INTO rk_folders (id, shelf_id) VALUES (10, 1), (20, 1)",
	);
	await conn.execute(
		"INSERT INTO rk_notes (id, folder_id, body) VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c')",
	);
	await conn.execute("INSERT INTO rk_covers (id, folder_id) VALUES (5, 20)");
	await conn.execute(
		"INSERT INTO rk_stickers (id, holder_folder) VALUES (7, 20)",
	);
});

afterAll(async () => {
	await conn.close();
});

function bodies(notes: readonly Note[]): string[] {
	return notes.map((n) => n.body).sort();
}

describe("relation keys accept the property and the column form", () => {
	it("@HasMany preloads the same rows with foreignKey as property or column", async () => {
		const folders = await new BaseRepository(Folder, conn)
			.query()
			.preload("notesByProperty")
			.preload("notesByColumn")
			.orderBy("id")
			.exec();

		expect(folders.map((f) => bodies(f.notesByProperty))).toEqual([
			["a", "b"],
			["c"],
		]);
		expect(folders.map((f) => bodies(f.notesByColumn))).toEqual([
			["a", "b"],
			["c"],
		]);
	});

	it("@HasOne preloads with foreignKey as property", async () => {
		const folders = await new BaseRepository(Folder, conn)
			.query()
			.preload("cover")
			.orderBy("id")
			.exec();

		expect(folders.map((f) => f.cover?.id ?? null)).toEqual([null, 5]);
	});

	it("@BelongsTo preloads with foreignKey as a mapped column", async () => {
		const [sticker] = await new BaseRepository(Sticker, conn)
			.query()
			.preload("holder")
			.exec();

		expect(sticker?.holder?.id).toBe(20);
	});

	it("@HasManyThrough preloads with firstKey and secondKey as properties", async () => {
		const [shelf] = await new BaseRepository(Shelf, conn)
			.query()
			.preload("notes")
			.exec();

		expect(bodies(shelf?.notes ?? [])).toEqual(["a", "b", "c"]);
	});

	it("whereHas and withCount resolve a property-form key", async () => {
		const repo = new BaseRepository(Folder, conn);
		const withNoteA = await repo
			.query()
			.whereHas("notesByProperty", (q) => q.where("body", "a"))
			.exec();
		expect(withNoteA.map((f) => f.id)).toEqual([10]);

		const withCover = await repo.query().has("cover").exec();
		expect(withCover.map((f) => f.id)).toEqual([20]);

		const shelves = await new BaseRepository(Shelf, conn)
			.query()
			.withCount("notes")
			.exec();
		expect(shelves[0]?.$extras.notes_count).toBe(3);
	});

	it("load() and related() resolve a property-form key lazily", async () => {
		const repo = new BaseRepository(Folder, conn);
		const folder = await repo.findOrFail(10);
		await folder.load("notesByProperty");
		expect(bodies(folder.notesByProperty)).toEqual(["a", "b"]);

		const created = await folder.related("notesByProperty").create({
			body: "d",
		});
		expect(created.folderId).toBe(10);
		const rows = await conn.query<{ folder_id: number }>(
			"SELECT folder_id FROM rk_notes WHERE body = 'd'",
		);
		expect(rows).toEqual([{ folder_id: 10 }]);
		await conn.execute("DELETE FROM rk_notes WHERE body = 'd'");
	});
});
