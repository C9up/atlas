/**
 * Adonis Lucid parity: `related('skills').create(values, pivotAttributes)` and
 * `.save(related, pivotAttributes)` (and their bulk forms) write extra columns
 * onto the pivot row alongside the FK/otherKey, in one transaction.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";

@Entity("mm_skills")
class MSkill extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

@Entity("mm_users")
class MUser extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@ManyToMany(() => MSkill, {
		pivotTable: "mm_user_skill",
		foreignKey: "user_id",
		otherKey: "skill_id",
		pivotColumns: ["proficiency"],
	})
	declare skills: MSkill[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE mm_users (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute("CREATE TABLE mm_skills (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE mm_user_skill (user_id TEXT, skill_id TEXT, proficiency INTEGER)",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > m2m create/save with pivotAttributes (Lucid)", () => {
	it("create(values, pivotAttributes) writes the pivot extra", async () => {
		const repo = new BaseRepository(MUser, conn);
		const user = await repo.create({ id: "u1", name: "Ada" });
		await user
			.related("skills")
			.create({ id: "s1", name: "ts" }, { proficiency: 5 });

		const pivot = await conn.query<{ proficiency: number }>(
			"SELECT proficiency FROM mm_user_skill WHERE user_id = 'u1' AND skill_id = 's1'",
		);
		expect(pivot[0]?.proficiency).toBe(5);
	});

	it("save(related, pivotAttributes) writes the pivot extra", async () => {
		const repo = new BaseRepository(MUser, conn);
		const user = await repo.create({ id: "u2", name: "Linus" });
		const skill = new MSkill();
		skill.id = "s2";
		skill.name = "rust";
		await user.related("skills").save(skill, { proficiency: 9 });

		const pivot = await conn.query<{ proficiency: number }>(
			"SELECT proficiency FROM mm_user_skill WHERE user_id = 'u2' AND skill_id = 's2'",
		);
		expect(pivot[0]?.proficiency).toBe(9);
	});

	it("createMany(rows, pivotAttributes[]) aligns extras by index", async () => {
		const repo = new BaseRepository(MUser, conn);
		const user = await repo.create({ id: "u3", name: "Grace" });
		await user.related("skills").createMany(
			[
				{ id: "s3", name: "cobol" },
				{ id: "s4", name: "asm" },
			],
			[{ proficiency: 7 }, { proficiency: 4 }],
		);

		const rows = await conn.query<{ skill_id: string; proficiency: number }>(
			"SELECT skill_id, proficiency FROM mm_user_skill WHERE user_id = 'u3' ORDER BY skill_id",
		);
		expect(rows).toEqual([
			{ skill_id: "s3", proficiency: 7 },
			{ skill_id: "s4", proficiency: 4 },
		]);
	});
});
