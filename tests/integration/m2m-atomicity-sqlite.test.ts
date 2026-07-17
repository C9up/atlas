/**
 * manyToMany write paths (create/save) must be ATOMIC and persist an unsaved
 * parent first, exactly like hasOne/hasMany — the related row + pivot insert (and
 * an unsaved parent's save) run in ONE transaction, rolled back on any failure so
 * there's no orphan related row and no pivot to a missing parent, with domain
 * events flushed only after commit (AdonisJS/Lucid parity).
 *
 * Also pins sync()'s attribute diff treating `null` and `""` as DISTINCT.
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
	beforeCreate,
	Column,
	type DomainEvent,
	Entity,
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

const events: DomainEvent[] = [];

@Entity("mm_skills")
class MSkill extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare label: string;

	@beforeCreate() static stamp(s: MSkill): void {
		s.addDomainEvent("skill.created", { id: s.id });
	}
}

@Entity("mm_users")
class MUser extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	// Happy-path pivot (no constraints beyond the two FKs).
	@ManyToMany(() => MSkill, {
		pivotTable: "mm_ok_pivot",
		foreignKey: "user_id",
		otherKey: "skill_id",
	})
	declare skills: MSkill[];

	// Atomicity pivot — `slot` is NOT NULL with no default, so the pivot INSERT
	// (which never writes it) fails AFTER the related row was created.
	@ManyToMany(() => MSkill, {
		pivotTable: "mm_bad_pivot",
		foreignKey: "user_id",
		otherKey: "skill_id",
	})
	declare badSkills: MSkill[];
}

@Entity("nn_tags")
class NnTag extends BaseEntity {
	@PrimaryKey() declare id: string;
}

@Entity("nn_owners")
class NnOwner extends BaseEntity {
	@PrimaryKey() declare id: string;
	@ManyToMany(() => NnTag, {
		pivotTable: "nn_pivot",
		foreignKey: "owner_id",
		otherKey: "tag_id",
		pivotColumns: ["note"],
	})
	declare tags: NnTag[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE mm_users (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE mm_skills (id TEXT PRIMARY KEY, label TEXT)",
	);
	await conn.execute("CREATE TABLE mm_ok_pivot (user_id TEXT, skill_id TEXT)");
	await conn.execute(
		"CREATE TABLE mm_bad_pivot (user_id TEXT, skill_id TEXT, slot TEXT NOT NULL)",
	);
	await conn.execute("CREATE TABLE nn_owners (id TEXT PRIMARY KEY)");
	await conn.execute("CREATE TABLE nn_tags (id TEXT PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE nn_pivot (owner_id TEXT, tag_id TEXT, note TEXT)",
	);
	await conn.execute("INSERT INTO nn_owners VALUES ('o1')");
	await conn.execute("INSERT INTO nn_tags VALUES ('t1')");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > m2m create is atomic + persists an unsaved parent", () => {
	it("persists the UNSAVED parent, then the related row, then the pivot (P2)", async () => {
		events.length = 0;
		const userRepo = new BaseRepository(MUser, conn);
		userRepo.onDomainEvents = async (e) => {
			events.push(...e);
		};
		const user = new MUser();
		user.id = "u1";
		user.name = "Ada";
		expect(user.$isPersisted).toBe(false);

		await userRepo
			.relatedProxy(user, "skills")
			.create({ id: "s1", label: "rust" });

		// Parent saved, related saved, pivot linked — all three.
		expect(user.$isPersisted).toBe(true);
		expect(await new BaseRepository(MUser, conn).find("u1")).not.toBeNull();
		expect(await new BaseRepository(MSkill, conn).find("s1")).not.toBeNull();
		const [pivot] = await conn.query<Record<string, unknown>>(
			"SELECT * FROM mm_ok_pivot WHERE user_id = 'u1' AND skill_id = 's1'",
		);
		expect(pivot).toBeDefined();
		// The related row's event fired — once, after commit.
		expect(events.map((e) => e.data.id)).toEqual(["s1"]);
	});

	it("rolls back the related row AND emits no events when the pivot insert fails (P1)", async () => {
		events.length = 0;
		const userRepo = new BaseRepository(MUser, conn);
		userRepo.onDomainEvents = async (e) => {
			events.push(...e);
		};
		const user = new MUser();
		user.id = "u2";
		user.name = "Linus";

		// The bad pivot's NOT NULL `slot` makes the pivot INSERT fail after the
		// related MSkill row was created on the transaction.
		await expect(
			userRepo.relatedProxy(user, "badSkills").create({ id: "s2", label: "c" }),
		).rejects.toThrow();

		// No orphan related row, no parent row, and the deferred event never fired
		// (the transaction rolled back before commit).
		expect(await new BaseRepository(MSkill, conn).find("s2")).toBeNull();
		expect(await new BaseRepository(MUser, conn).find("u2")).toBeNull();
		expect(events).toEqual([]);

		// The parent was persisted on the rolled-back trx, so it MUST report $isNew
		// again — otherwise reusing it would skip the parent save and orphan the FK.
		expect(user.$isPersisted).toBe(false);

		// And it stays usable: REPO_REF was re-pointed at the durable repo, so a fresh
		// write re-saves the parent (reads $isNew) then the related row + pivot.
		await userRepo
			.relatedProxy(user, "skills")
			.create({ id: "s3", label: "go" });
		expect(await new BaseRepository(MUser, conn).find("u2")).not.toBeNull();
		const [pivot] = await conn.query<Record<string, unknown>>(
			"SELECT * FROM mm_ok_pivot WHERE user_id = 'u2' AND skill_id = 's3'",
		);
		expect(pivot).toBeDefined();
	});

	it("reverts a caller-passed related instance when the pivot insert fails (P2)", async () => {
		events.length = 0;
		const userRepo = new BaseRepository(MUser, conn);
		userRepo.onDomainEvents = async (e) => {
			events.push(...e);
		};
		// Persisted parent so we isolate the RELATED revert (the parent already exists).
		const user = await userRepo.create({ id: "u4", name: "Guido" });
		const skill = new MSkill();
		skill.id = "s4";
		skill.label = "py";
		expect(skill.$isPersisted).toBe(false);

		// m2m.save persists the related row, THEN the bad pivot's NOT NULL `slot` makes
		// the pivot INSERT fail → the whole transaction rolls back.
		await expect(
			userRepo.relatedProxy(user, "badSkills").save(skill),
		).rejects.toThrow();

		// Related row rolled back, no event published.
		expect(await new BaseRepository(MSkill, conn).find("s4")).toBeNull();
		expect(events).toEqual([]);

		// The caller's related instance was inserted on the rolled-back trx, so it MUST
		// report $isNew again with its "skill.created" event dropped — otherwise reusing
		// it would skip the re-insert and write a pivot to a phantom related row.
		expect(skill.$isPersisted).toBe(false);
		expect(skill.getDomainEvents()).toEqual([]);

		// Reusable: a clean save re-inserts the related (reads $isNew) + pivot, and
		// publishes the event EXACTLY once (the stale one did not linger).
		await userRepo.relatedProxy(user, "skills").save(skill);
		expect(await new BaseRepository(MSkill, conn).find("s4")).not.toBeNull();
		expect(events.map((e) => e.data.id)).toEqual(["s4"]);
	});
});

describe("atlas > m2m sync() diff treats null and empty string as distinct (P3)", () => {
	it("updates a pivot attribute from empty string to null (the collapse case)", async () => {
		const owner = await new BaseRepository(NnOwner, conn).findOrFail("o1");
		const tags = owner.related("tags");
		if (tags.type !== "manyToMany") throw new Error("expected m2m");

		// Store "" then sync to null. A `String(x ?? "")` comparison maps BOTH ""
		// and null to "" and would skip the update, leaving the stale "". The diff
		// must see them as distinct and rewrite the row.
		await tags.attach({ t1: { note: "" } });
		await tags.sync({ t1: { note: null } });

		const [row] = await conn.query<Record<string, unknown>>(
			"SELECT note FROM nn_pivot WHERE owner_id = 'o1' AND tag_id = 't1'",
		);
		expect(row.note).toBeNull();
	});
});

describe("atlas > m2m attach/detach/sync require a persisted parent (P1)", () => {
	it("rejects an UNSAVED parent (no pivot row with a null/absent FK)", async () => {
		const user = new MUser();
		user.id = "gx"; // manual PK, but never loaded — not a row
		const proxy = new BaseRepository(MUser, conn).relatedProxy(user, "skills");
		if (proxy.type !== "manyToMany") throw new Error("expected m2m");

		await expect(proxy.attach(["s1"])).rejects.toThrow(/not persisted/i);
		await expect(proxy.detach(["s1"])).rejects.toThrow(/not persisted/i);
		await expect(proxy.sync(["s1"])).rejects.toThrow(/not persisted/i);
	});

	it("rejects a keyless projection parent with a PK error", async () => {
		await new BaseRepository(MUser, conn).create({ id: "real", name: "R" });
		const [proj] = await new BaseRepository(MUser, conn)
			.query()
			.select("COUNT(*) as n")
			.exec();
		const proxy = new BaseRepository(MUser, conn).relatedProxy(proj, "skills");
		if (proxy.type !== "manyToMany") throw new Error("expected m2m");

		await expect(proxy.attach(["s1"])).rejects.toThrow(/primary key/i);
		await expect(proxy.sync(["s1"])).rejects.toThrow(/primary key/i);
	});
});

describe("atlas > a child write does NOT flush an already-persisted parent's events (P2)", () => {
	it("emits only the child's events, leaving the persisted parent's untouched", async () => {
		events.length = 0;
		const userRepo = new BaseRepository(MUser, conn);
		userRepo.onDomainEvents = async (e) => {
			events.push(...e);
		};
		// A persisted parent carrying an unrelated pending event (belongs to whoever
		// saves the parent, not to a child mutation).
		const user = await userRepo.create({ id: "pe1", name: "Ada" });
		user.addDomainEvent("user.touched", { id: "pe1" });
		events.length = 0;

		await userRepo
			.relatedProxy(user, "skills")
			.create({ id: "ce1", label: "rust" });

		// Only the child (MSkill) event fired — the parent was not saved, so its
		// pending event must NOT be emitted as a side effect…
		expect(events.map((e) => e.name)).toEqual(["skill.created"]);
		// …and it's still sitting on the parent, undispatched.
		expect(user.getDomainEvents().map((e) => e.name)).toEqual(["user.touched"]);
	});
});
