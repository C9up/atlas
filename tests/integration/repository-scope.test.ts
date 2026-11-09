/**
 * A repository scope reaches every query the repository's relations run.
 *
 * A tenant filter written as an override of `query()` filtered the root rows
 * only: a preload ran on a fresh repository and a bare query, so a row of
 * another tenant that referenced a row of this one came back with it. The
 * `scope` option is carried to the related, pivot and intermediate tables, to
 * nested preloads, to `whereHas` / `withCount`, and to the `load()` /
 * `related()` of the entities the repository hydrated.
 *
 * SQLite always runs; Postgres runs when ATLAS_TEST_PG_URL is set, which is
 * where the placeholders of the scoped subqueries are renumbered.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
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
	ManyToMany,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { ModelQuery, type QueryScope } from "../../src/ModelQuery.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";

@Entity("sc_comments")
class Comment extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare noteId: number;
	@Column() declare companyId: number;
	@Column() declare body: string;
}

@Entity("sc_notes")
class Note extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare folderId: number;
	@Column() declare companyId: number;
	@Column() declare body: string;
	@HasMany(() => Comment) declare comments: Comment[];
	@BelongsTo(() => Folder) declare folder: Folder | null;
}

/** Shared by every tenant: no `company_id`, the scope lets it through. */
@Entity("sc_tags")
class Tag extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
}

@Entity("sc_folders")
class Folder extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare shelfId: number;
	@Column() declare companyId: number;
	@HasMany(() => Note) declare notes: Note[];
	@ManyToMany(() => Tag, {
		pivotTable: "sc_folder_tags",
		foreignKey: "folder_id",
		otherKey: "tag_id",
	})
	declare tags: Tag[];
}

@Entity("sc_shelves")
class Shelf extends BaseEntity {
	@PrimaryKey() declare id: number;
	@HasMany(() => Folder) declare folders: Folder[];
	@HasManyThrough(
		() => Note,
		() => Folder,
	)
	declare notes: Note[];
}

const TENANT_TABLES = new Set([
	"sc_comments",
	"sc_notes",
	"sc_folders",
	"sc_folder_tags",
]);

let current = 1;
const scope: QueryScope = (q, table) => {
	if (TENANT_TABLES.has(table)) q.where("company_id", current);
};

/** `related(name).query()` is typed for every relation kind; narrow it. */
async function relatedRows(
	entity: BaseEntity,
	relation: string,
): Promise<BaseEntity[]> {
	const q = entity.related(relation).query();
	if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
	return q.exec();
}

function bodies(rows: readonly { body: string }[] | undefined): string[] {
	return (rows ?? []).map((r) => r.body).sort();
}

function suite(label: string, url: string, dialect: AtlasDialect): void {
	(url ? describe : describe.skip)(`repository scope ${label}`, () => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			setAtlasDialect(dialect);
			conn = await createNapiConnection(url, 1, 2);
			for (const t of [
				"sc_comments",
				"sc_notes",
				"sc_folder_tags",
				"sc_tags",
				"sc_folders",
				"sc_shelves",
			])
				await conn.execute(`DROP TABLE IF EXISTS ${t}`);
			await conn.execute("CREATE TABLE sc_shelves (id INTEGER PRIMARY KEY)");
			await conn.execute(
				"CREATE TABLE sc_folders (id INTEGER PRIMARY KEY, shelf_id INTEGER, company_id INTEGER)",
			);
			await conn.execute(
				"CREATE TABLE sc_notes (id INTEGER PRIMARY KEY, folder_id INTEGER, company_id INTEGER, body VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE sc_comments (id INTEGER PRIMARY KEY, note_id INTEGER, company_id INTEGER, body VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE sc_tags (id INTEGER PRIMARY KEY, label VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE sc_folder_tags (folder_id INTEGER, tag_id INTEGER, company_id INTEGER)",
			);
			await conn.execute("INSERT INTO sc_shelves (id) VALUES (1)");
			// Folder 10 is company 1's, folder 20 company 2's, both on shelf 1.
			await conn.execute(
				"INSERT INTO sc_folders (id, shelf_id, company_id) VALUES (10, 1, 1), (20, 1, 2)",
			);
			// Note 2 belongs to company 2 but points at company 1's folder: the
			// row a preload used to hand back.
			await conn.execute(
				"INSERT INTO sc_notes (id, folder_id, company_id, body) VALUES " +
					"(1, 10, 1, 'mine'), (2, 10, 2, 'theirs'), (3, 20, 2, 'their folder')",
			);
			await conn.execute(
				"INSERT INTO sc_comments (id, note_id, company_id, body) VALUES " +
					"(1, 1, 1, 'ok'), (2, 1, 2, 'leak')",
			);
			await conn.execute(
				"INSERT INTO sc_tags (id, label) VALUES (1, 'red'), (2, 'blue')",
			);
			// The second link is company 2's, on company 1's folder.
			await conn.execute(
				"INSERT INTO sc_folder_tags (folder_id, tag_id, company_id) VALUES (10, 1, 1), (10, 2, 2)",
			);
		});

		beforeEach(() => {
			setAtlasDialect(dialect);
			current = 1;
		});

		afterAll(async () => {
			await conn?.close();
		});

		const folders = () => new BaseRepository(Folder, conn, { scope });

		it("filters the root query, and an orWhere cannot reach past it", async () => {
			const rows = await folders()
				.query()
				.where("id", 10)
				.orWhere("id", 20)
				.exec();
			expect(rows.map((f) => f.id)).toEqual([10]);
		});

		it("filters a preload and a nested preload", async () => {
			const [folder] = await folders()
				.query()
				.where("id", 10)
				.preload("notes", (q) => q.preload("comments"))
				.exec();
			expect(bodies(folder?.notes)).toEqual(["mine"]);
			expect(bodies(folder?.notes[0]?.comments)).toEqual(["ok"]);
		});

		it("filters the pivot of a many-to-many preload", async () => {
			const [folder] = await folders()
				.query()
				.where("id", 10)
				.preload("tags")
				.exec();
			expect(folder?.tags.map((t) => t.label)).toEqual(["red"]);
		});

		it("filters the intermediate rows of a through preload", async () => {
			const [shelf] = await new BaseRepository(Shelf, conn, { scope })
				.query()
				.preload("notes")
				.preload("folders")
				.exec();
			// Folder 20 is company 2's: neither it nor the notes reached through it.
			expect(shelf?.folders.map((f) => f.id)).toEqual([10]);
			expect(bodies(shelf?.notes)).toEqual(["mine"]);
		});

		it("filters load() and related() on an entity the repository hydrated", async () => {
			const folder = await folders().findOrFail(10);
			await folder.load("notes");
			expect(bodies(folder.notes)).toEqual(["mine"]);

			const notes = await relatedRows(folder, "notes");
			expect(notes.map((n) => n.body)).toEqual(["mine"]);
			const tags = await relatedRows(folder, "tags");
			expect(tags.map((t) => t.label)).toEqual(["red"]);

			// One level down: the preloaded note's own load() keeps the scope.
			const note = folder.notes[0];
			if (!note) throw new Error("expected a note");
			await note.load("comments");
			expect(bodies(note.comments)).toEqual(["ok"]);
		});

		it("filters whereHas and withCount, pivot and intermediate included", async () => {
			const [counted] = await folders()
				.query()
				.where("id", 10)
				.withCount("notes")
				.withCount("tags")
				.exec();
			expect(Number(counted?.$extras.notes_count)).toBe(1);
			expect(Number(counted?.$extras.tags_count)).toBe(1);

			const withBlue = await folders()
				.query()
				.whereHas("tags", (q) => q.where("label", "blue"))
				.exec();
			expect(withBlue).toEqual([]);

			const [shelf] = await new BaseRepository(Shelf, conn, { scope })
				.query()
				.withCount("notes")
				.exec();
			expect(Number(shelf?.$extras.notes_count)).toBe(1);
		});

		it("restricts a write by id to the scope", async () => {
			const notes = new BaseRepository(Note, conn, { scope });
			await notes.updateById(2, { body: "hijacked" });
			await notes.increment(2, "folderId", 1);
			const [row] = await conn.query<{ body: string; folder_id: number }>(
				"SELECT body, folder_id FROM sc_notes WHERE id = 2",
			);
			expect(row?.body).toBe("theirs");
			expect(Number(row?.folder_id)).toBe(10);
		});

		it("keeps the scope on a transaction copy", async () => {
			await conn.transaction?.(async (trx) => {
				const [folder] = await folders()
					.useTransaction(trx)
					.query()
					.where("id", 10)
					.preload("notes")
					.exec();
				expect(bodies(folder?.notes)).toEqual(["mine"]);
			});
		});

		it("reads the scope's state when the query compiles", async () => {
			const q = new BaseRepository(Note, conn, { scope }).query();
			current = 2;
			expect(bodies(await q.exec())).toEqual(["their folder", "theirs"]);
		});

		it("leaves a repository without a scope as it was", async () => {
			const [folder] = await new BaseRepository(Folder, conn)
				.query()
				.where("id", 10)
				.preload("notes")
				.preload("tags")
				.exec();
			expect(bodies(folder?.notes)).toEqual(["mine", "theirs"]);
			expect(folder?.tags.map((t) => t.label).sort()).toEqual(["blue", "red"]);
		});

		it("a scope that throws refuses the preload", async () => {
			const refuse: QueryScope = (_q, table) => {
				if (table === "sc_notes") throw new Error("notes are off limits");
			};
			await expect(
				new BaseRepository(Folder, conn, { scope: refuse })
					.query()
					.preload("notes")
					.exec(),
			).rejects.toThrow("notes are off limits");
		});
	});
}

suite("SQLite", "sqlite::memory:", "sqlite");
suite("PG", process.env.ATLAS_TEST_PG_URL ?? "", "postgres");
