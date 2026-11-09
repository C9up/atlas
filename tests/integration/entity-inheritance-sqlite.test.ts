/**
 * An entity subclass keeps its parent's columns and relations when it
 * declares its own.
 *
 * The decorators keep one metadata array per class and the readers took the
 * first array found up the prototype chain, so a subclass with a single
 * `@Column()` of its own lost every inherited column and relation: they were
 * neither hydrated nor written, and inherited relations could not preload.
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
	getColumnMetadata,
	getRelationMetadata,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("teams")
class Team extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

@Entity("members")
class Member extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@Column({ columnName: "team_id" }) declare teamId: number;
	@Column({ type: "text" }) declare note: string;
	@BelongsTo(() => Team, { foreignKey: "teamId" }) declare team: Team;
}

@Entity("members")
class Captain extends Member {
	@Column() declare rank: number;
	@Column({ type: "text", serializeAs: null }) declare note: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE members (id INTEGER PRIMARY KEY, name TEXT, team_id INTEGER, note TEXT, rank INTEGER)",
	);
	await conn.execute("INSERT INTO teams VALUES (1, 'red')");
});

afterAll(async () => {
	await conn.close();
});

describe("atlas > entity inheritance keeps the parent's metadata", () => {
	it("lists inherited columns and relations beside the subclass's own", () => {
		expect(getColumnMetadata(Captain).map((c) => c.propertyKey)).toEqual([
			"id",
			"name",
			"teamId",
			"note",
			"rank",
		]);
		expect(getRelationMetadata(Captain).map((r) => r.propertyKey)).toEqual([
			"team",
		]);
		// The parent is untouched by its subclass.
		expect(getColumnMetadata(Member).map((c) => c.propertyKey)).toEqual([
			"id",
			"name",
			"teamId",
			"note",
		]);
	});

	it("a redeclared column takes the subclass's options in place", () => {
		const note = getColumnMetadata(Captain).find(
			(c) => c.propertyKey === "note",
		);
		expect(note?.serializeAs).toBeNull();
		const parentNote = getColumnMetadata(Member).find(
			(c) => c.propertyKey === "note",
		);
		expect(parentNote?.serializeAs).toBeUndefined();
	});

	it("writes and hydrates the inherited columns", async () => {
		const repo = new BaseRepository(Captain, conn);
		const captain = new Captain();
		captain.id = 7;
		captain.name = "Ada";
		captain.teamId = 1;
		captain.note = "private";
		captain.rank = 3;
		await repo.save(captain);

		const rows = await conn.query("SELECT * FROM members WHERE id = 7");
		expect(rows).toEqual([
			{ id: 7, name: "Ada", team_id: 1, note: "private", rank: 3 },
		]);

		const found = await repo.find(7);
		expect(found?.name).toBe("Ada");
		expect(found?.teamId).toBe(1);
		expect(found?.rank).toBe(3);
	});

	it("preloads an inherited relation", async () => {
		const repo = new BaseRepository(Captain, conn);
		const [captain] = await repo.query().where("id", 7).preload("team");
		expect(captain?.team?.name).toBe("red");
	});
});
