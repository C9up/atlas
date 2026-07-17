/**
 * m2m `sync()` correctness against real SQLite — the four failure modes unit
 * mocks can't reach, all exercised with an INTEGER related PK (object-form keys
 * arrive as strings, so this is where the string/number bugs bite):
 *
 *   1. object-form integer keys + an idempotent `sync` must NOT churn the pivot
 *      (the diff compares "1" against the numeric id the DB returns);
 *   2. `sync` UPDATES a changed pivot attribute in place (Adonis Lucid parity)
 *      rather than leaving it stale;
 *   3. `sync` is atomic — a failing attach rolls back the detach it already did.
 *
 * A non-declared `marker` column, raw-set after the initial attach, is the
 * sentinel: it survives an in-place UPDATE but is lost by a detach+re-attach, so
 * asserting `marker === 'keep'` deterministically proves "no churn" without
 * relying on timestamp timing.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
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

@Entity("us_skills")
class SSkill extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
}

@Entity("us_users")
class SUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;

	@ManyToMany(() => SSkill, {
		pivotTable: "us_pivot",
		foreignKey: "user_id",
		otherKey: "skill_id",
		pivotColumns: ["level"],
		pivotTimestamps: true,
	})
	declare skills: SSkill[];
}

let conn: AsyncDatabaseConnection;

const pivotRows = (): Promise<Array<Record<string, unknown>>> =>
	conn.query<Record<string, unknown>>(
		"SELECT skill_id, level, marker FROM us_pivot WHERE user_id = 1 ORDER BY skill_id",
	);

async function skillsProxy() {
	const user = await new BaseRepository(SUser, conn).findOrFail(1);
	const proxy = user.related("skills");
	if (proxy.type !== "manyToMany")
		throw new Error("expected a manyToMany proxy");
	return proxy;
}

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE us_users (id INTEGER PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE us_skills (id INTEGER PRIMARY KEY, label TEXT)",
	);
	// `note` is NOT NULL: an attach that omits it (finding #3 atomicity test)
	// fails at the DB, letting us prove the detach rolls back. `marker` is a
	// non-declared sentinel column (see file header).
	await conn.execute(
		"CREATE TABLE us_pivot (user_id INTEGER, skill_id INTEGER, level INTEGER, note TEXT NOT NULL, marker TEXT, created_at TEXT, updated_at TEXT)",
	);
	await conn.execute("INSERT INTO us_users VALUES (1, 'Ada')");
	await conn.execute(
		"INSERT INTO us_skills VALUES (1, 'rust'), (2, 'sql'), (3, 'ts')",
	);
});

afterAll(async () => {
	await conn?.close();
});

beforeEach(async () => {
	// Reset to a known {1: level 1, 2: level 2} state with a live sentinel.
	await conn.execute("DELETE FROM us_pivot");
	const tags = await skillsProxy();
	await tags.attach({ 1: { level: 1, note: "a" }, 2: { level: 2, note: "b" } });
	await conn.execute("UPDATE us_pivot SET marker = 'keep' WHERE user_id = 1");
});

describe("atlas > m2m sync() (real SQLite, integer PK)", () => {
	it("object-form integer keys: an idempotent sync does NOT churn the pivot", async () => {
		const tags = await skillsProxy();
		// Same set, same attributes — the diff must resolve to a no-op. A broken
		// string-vs-number diff would detach both and re-insert both (marker lost,
		// possibly duplicated rows).
		await tags.sync({ 1: { level: 1, note: "a" }, 2: { level: 2, note: "b" } });

		const rows = await pivotRows();
		expect(rows.map((r) => r.skill_id)).toEqual([1, 2]); // no duplicates
		// Sentinel survived → the rows were never detached/re-inserted.
		expect(rows.every((r) => r.marker === "keep")).toBe(true);
	});

	it("updates a changed pivot attribute in place (Lucid parity), keeping the row", async () => {
		const tags = await skillsProxy();
		await tags.sync({ 1: { level: 9, note: "a" }, 2: { level: 2, note: "b" } });

		const rows = await pivotRows();
		expect(rows.map((r) => r.skill_id)).toEqual([1, 2]); // still exactly two
		const s1 = rows.find((r) => r.skill_id === 1);
		const s2 = rows.find((r) => r.skill_id === 2);
		expect(s1?.level).toBe(9); // attribute refreshed
		expect(s2?.level).toBe(2); // untouched row unchanged
		// UPDATE in place, not detach+re-insert → sentinel kept.
		expect(s1?.marker).toBe("keep");
	});

	it("is atomic — a failing attach rolls back the detach it already applied", async () => {
		const tags = await skillsProxy();
		// Target {2, 3}: skill 1 is detached, skill 3 is attached WITHOUT `note`,
		// which violates the NOT NULL pivot column. The whole sync must roll back.
		await expect(
			tags.sync({ 2: { level: 2, note: "b" }, 3: { level: 3 } }),
		).rejects.toThrow();

		const rows = await pivotRows();
		// Detach of skill 1 was rolled back; skill 3 never landed.
		expect(rows.map((r) => r.skill_id)).toEqual([1, 2]);
		expect(rows.every((r) => r.marker === "keep")).toBe(true);
	});
});
