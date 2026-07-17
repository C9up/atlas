/**
 * Round-12 relation side-effect fixes against real SQLite:
 *
 *   P2 — belongsTo.associate() must NOT flush an ALREADY-persisted owner's
 *        pending domain events (it only saves an UNSAVED owner). The parent it
 *        always saves, so the parent's events do flush. Same seam as
 *        withParentSaved's `savedParentHere` snapshot.
 *   P2/P3 — eager preload must boot the related/through model on demand
 *        (ensureEntityMetadata), not silently no-op when the related class was
 *        never independently touched (Lucid lazy-boots models).
 *   P3 — the m2m direct proxy must read the parent's (custom local) key at
 *        OPERATION time, not capture it at `related()` time — a key mutated
 *        before attach() must be the one written.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	BelongsTo,
	beforeCreate,
	Column,
	type DomainEvent,
	HasMany,
	ManyToMany,
	ModelQuery,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// ─── P2: associate() domain-event side-effect ───────────────────
class EvAuthor extends BaseModel {
	static override table = "ev_authors";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}
class EvPost extends BaseModel {
	static override table = "ev_posts";
	@PrimaryKey() declare id: string;
	@Column() declare authorId: string | null;
	@BelongsTo(() => EvAuthor, { foreignKey: "author_id" })
	declare author: EvAuthor | null;
}

// ─── P2/P3: preload boots an un-touched related model ───────────
// `LbTag` is referenced ONLY through the relation and its rows are inserted via
// raw SQL — it is NEVER booted through its own repo, so `getEntityMetadata`
// would be empty and the OLD preload path dropped it silently.
class LbTag extends BaseModel {
	static override table = "lb_tags";
	@PrimaryKey() declare id: string;
	@Column() declare postId: string;
	@Column() declare label: string;
}
class LbPost extends BaseModel {
	static override table = "lb_posts";
	@PrimaryKey() declare id: string;
	@HasMany(() => LbTag, { foreignKey: "post_id" })
	declare tags: LbTag[];
}

// ─── P3: m2m localKey read lazily ───────────────────────────────
class LzRole extends BaseModel {
	static override table = "lz_roles";
	@PrimaryKey() declare id: string;
}
class LzUser extends BaseModel {
	static override table = "lz_users";
	@PrimaryKey() declare id: string;
	@Column() declare code: string;
	@ManyToMany(() => LzRole, {
		pivotTable: "lz_pivot",
		foreignKey: "user_code",
		otherKey: "role_id",
		localKey: "code",
	})
	declare roles: LzRole[];
}

// ─── P2: related().firstOrCreate() double-flush ─────────────────
class FcChild extends BaseModel {
	static override table = "fc_children";
	@PrimaryKey() declare id: string;
	@Column() declare parentId: string;
	@beforeCreate() static stamp(c: FcChild): void {
		c.addDomainEvent("child.created", { id: c.id });
	}
}
class FcParent extends BaseModel {
	static override table = "fc_parents";
	@PrimaryKey() declare id: string;
	@HasMany(() => FcChild, { foreignKey: "parent_id" })
	declare children: FcChild[];
}

// ─── P2: saveMany() runs hooks/events on the PASSED instances ────
class SmWidget extends BaseModel {
	static override table = "sm_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@Column() declare stamped: string | null;
	@beforeCreate() static stamp(w: SmWidget): void {
		w.stamped = "hooked"; // hook MUTATION — must land on the caller's instance
		w.addDomainEvent("widget.created", { id: w.id });
	}
}
class SmChild extends BaseModel {
	static override table = "sm_children";
	@PrimaryKey() declare id: string;
	@Column() declare parentId: string;
	@beforeCreate() static stamp(c: SmChild): void {
		c.addDomainEvent("child.created", { id: c.id });
	}
}
class SmParent extends BaseModel {
	static override table = "sm_parents";
	@PrimaryKey() declare id: string;
	@HasMany(() => SmChild, { foreignKey: "parent_id" })
	declare children: SmChild[];
}

// ─── P3: m2m query with a schema-qualified pivot table ──────────
class RqRole extends BaseModel {
	static override table = "rq_roles";
	@PrimaryKey() declare id: string;
}
class RqUser extends BaseModel {
	static override table = "rq_users";
	@PrimaryKey() declare id: string;
	@ManyToMany(() => RqRole, {
		pivotTable: "reporting.user_roles", // schema-qualified (Postgres)
		foreignKey: "user_id",
		otherKey: "role_id",
	})
	declare roles: RqRole[];
}
class EvilUser extends BaseModel {
	static override table = "evil_users";
	@PrimaryKey() declare id: string;
	// A pivot name carrying a quote — the raw whereHas/withCount fragment must
	// REJECT it (strict per-segment guard), never emit it into SQL.
	@ManyToMany(() => RqRole, {
		pivotTable: 'x"; DROP TABLE users; --',
		foreignKey: "user_id",
		otherKey: "role_id",
	})
	declare roles: RqRole[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE ev_authors (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE ev_posts (id TEXT PRIMARY KEY, author_id TEXT)",
	);
	await conn.execute(
		"CREATE TABLE lb_tags (id TEXT PRIMARY KEY, post_id TEXT, label TEXT)",
	);
	await conn.execute("CREATE TABLE lb_posts (id TEXT PRIMARY KEY)");
	await conn.execute("CREATE TABLE lz_users (id TEXT PRIMARY KEY, code TEXT)");
	await conn.execute("CREATE TABLE lz_roles (id TEXT PRIMARY KEY)");
	await conn.execute("CREATE TABLE lz_pivot (user_code TEXT, role_id TEXT)");
	await conn.execute("CREATE TABLE fc_parents (id TEXT PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE fc_children (id TEXT PRIMARY KEY, parent_id TEXT)",
	);
	await conn.execute(
		"CREATE TABLE sm_widgets (id TEXT PRIMARY KEY, name TEXT, stamped TEXT)",
	);
	await conn.execute("CREATE TABLE sm_parents (id TEXT PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE sm_children (id TEXT PRIMARY KEY, parent_id TEXT)",
	);
	await conn.execute("CREATE TABLE rq_users (id TEXT PRIMARY KEY)");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	for (const t of [
		"ev_posts",
		"ev_authors",
		"lb_tags",
		"lb_posts",
		"lz_pivot",
		"lz_roles",
		"lz_users",
		"fc_children",
		"fc_parents",
		"sm_widgets",
		"sm_children",
		"sm_parents",
		"rq_users",
	]) {
		await conn.execute(`DELETE FROM ${t}`);
	}
});

describe("atlas > associate() domain-event side-effect (P2)", () => {
	it("does NOT flush an already-persisted owner's pending events, but does flush the parent's", async () => {
		// The proxy dispatches through the repo stored in the parent's REPO_REF —
		// i.e. the repo that CREATED the parent — so the sink must live on THAT
		// instance ($repo() returns a fresh repo each call).
		const repo = EvPost.$repo();
		const seen: DomainEvent[] = [];
		repo.onDomainEvents = async (events) => {
			seen.push(...events);
		};

		const author = await EvAuthor.create({ id: "a1", name: "Ada" });
		// A pending event on an ALREADY-persisted owner — associate() didn't save
		// this owner, so it's not associate()'s job to publish its events.
		author.addDomainEvent("owner.stale", { id: "a1" });

		const post = await repo.create({ id: "p1", authorId: null });
		post.addDomainEvent("parent.linked", { id: "p1" });

		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");
		await rel.associate(author);

		const names = seen.map((e) => e.name);
		expect(names).not.toContain("owner.stale"); // owner untouched → not flushed
		expect(names).toContain("parent.linked"); // parent saved → flushed
		expect(post.authorId).toBe("a1"); // association still made
	});

	it("DOES flush an unsaved owner's events (associate saved it)", async () => {
		const repo = EvPost.$repo();
		const seen: DomainEvent[] = [];
		repo.onDomainEvents = async (events) => {
			seen.push(...events);
		};

		const author = new EvAuthor();
		author.id = "a2";
		author.name = "Grace";
		author.addDomainEvent("owner.born", { id: "a2" });
		expect(author.$isPersisted).toBe(false);

		const post = await repo.create({ id: "p2", authorId: null });
		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");
		await rel.associate(author);

		expect(seen.map((e) => e.name)).toContain("owner.born");
		expect(author.$isPersisted).toBe(true);
	});
});

describe("atlas > eager preload boots an un-touched related model (P2/P3)", () => {
	it("preloads a relation whose target was never booted via its own repo", async () => {
		await LbPost.create({ id: "post1" });
		// Insert the related rows WITHOUT ever touching LbTag through its repo, so
		// its @Entity metadata is unbooted. Before the fix, #buildPreloadContext
		// read getEntityMetadata(LbTag) → undefined → returned null → tags empty.
		await conn.execute(
			"INSERT INTO lb_tags (id, post_id, label) VALUES ('t1', 'post1', 'alpha')",
		);
		await conn.execute(
			"INSERT INTO lb_tags (id, post_id, label) VALUES ('t2', 'post1', 'beta')",
		);

		const [loaded] = await LbPost.query()
			.where("id", "post1")
			.preload("tags")
			.exec();
		expect(loaded.tags.map((t) => t.label).sort()).toEqual(["alpha", "beta"]);
	});
});

describe("atlas > related().firstOrCreate() dispatches via a SINGLE hook (P2)", () => {
	it("does not double-register the child's post-commit flush", async () => {
		const repo = FcParent.$repo();
		let calls = 0;
		const delivered: string[] = [];
		// Fail the FIRST dispatch attempt: #dispatchDomainEvents re-queues the events
		// and swallows the error (post-commit). A buggy DOUBLE registration would then
		// retry the re-queued events in the SAME commit → a second sink call that
		// re-delivers to the bus. A single registration attempts dispatch exactly once.
		repo.onDomainEvents = async (events) => {
			calls++;
			if (calls === 1) throw new Error("bus hiccup");
			delivered.push(...events.map((e) => e.name));
		};

		const parent = await repo.create({ id: "p1" });
		const rel = parent.related("children");
		if (rel.type !== "hasMany") throw new Error("expected hasMany");
		// firstOrCreate goes through #inManagedTx (its own post-commit dispatch); the
		// relation wrapper must NOT add a second one.
		await rel.firstOrCreate({ id: "c1" }, {});

		expect(calls).toBe(1); // one attempt, not two → no in-commit re-delivery
		expect(delivered).toEqual([]); // the duplicating retry never happened
	});
});

describe("atlas > saveMany() persists the CALLER's instances (P2)", () => {
	it("runs hooks (mutations + events) on the passed instances, not clones", async () => {
		const repo = SmWidget.$repo();
		const events: string[] = [];
		repo.onDomainEvents = async (ev) => {
			events.push(...ev.map((e) => e.name));
		};

		const w1 = new SmWidget();
		w1.id = "w1";
		w1.name = "A";
		const w2 = new SmWidget();
		w2.id = "w2";
		w2.name = "B";

		const result = await repo.saveMany([w1, w2]);

		// The beforeCreate mutation lands on the ORIGINAL instances (old path ran the
		// hook on discarded clones, so w1.stamped stayed null).
		expect(w1.stamped).toBe("hooked");
		expect(w2.stamped).toBe("hooked");
		expect(result[0]).toBe(w1); // returns the same objects
		expect(w1.$isPersisted).toBe(true);
		expect(events.sort()).toEqual(["widget.created", "widget.created"]);
	});

	it("does not LOSE hook events in a deferred relation saveMany (hasMany, fresh children)", async () => {
		const repo = SmParent.$repo();
		const events: string[] = [];
		repo.onDomainEvents = async (ev) => {
			events.push(...ev.map((e) => e.name));
		};

		const parent = await repo.create({ id: "p1" });
		const c1 = new SmChild();
		c1.id = "c1";
		const c2 = new SmChild();
		c2.id = "c2";

		const rel = parent.related("children");
		if (rel.type !== "hasMany") throw new Error("expected hasMany");
		// Deferred (withParentSaved) → hooks add events, flushed post-commit against
		// the passed instances. The old clone path left those instances event-less.
		await rel.saveMany([c1, c2]);

		expect(events.sort()).toEqual(["child.created", "child.created"]);
	});
});

describe("atlas > m2m related().query() supports a schema-qualified pivot (P3)", () => {
	it("quotes `schema.table` per-segment instead of rejecting it", async () => {
		const user = await RqUser.create({ id: "u1" });
		const rel = user.related("roles");
		if (rel.type !== "manyToMany") throw new Error("expected m2m");

		// Before the fix, the inline quote rejected the dot → "Unsafe identifier".
		const query = rel.query();
		if (!(query instanceof ModelQuery))
			throw new Error("expected a ModelQuery");
		const { sql } = query.toSQL();
		expect(sql).toContain('"reporting"."user_roles"');
	});

	it("withCount()/whereHas() also quote a schema-qualified pivot per-segment", async () => {
		// #makeRelationSub is a distinct raw-SQL path from related().query() — the
		// clone fallow surfaced. It must quote the schema too, not emit
		// "reporting.user_roles" as one dotted identifier.
		const { sql } = RqUser.query().withCount("roles").toSQL();
		expect(sql).toContain('"reporting"."user_roles"');
		expect(sql).not.toContain('"reporting.user_roles"');
	});

	it("rejects a relation whose pivot metadata carries a quote (P3 security)", () => {
		// The raw fragment binds no params for identifiers, so an unsafe pivot name
		// must throw, never reach the SQL string.
		expect(() => EvilUser.query().withCount("roles").toSQL()).toThrow(
			/Unsafe identifier/i,
		);
	});
});

describe("atlas > m2m proxy reads the parent's local key LAZILY (P3)", () => {
	it("attach() targets the CURRENT localKey, not the one captured at related()", async () => {
		const user = await LzUser.create({ id: "u1", code: "ABC" });
		await LzRole.create({ id: "r1" });

		const roles = user.related("roles");
		if (roles.type !== "manyToMany") throw new Error("expected m2m");
		// Mutate the local key AFTER obtaining the proxy but BEFORE attach().
		user.code = "ZZZ";
		await roles.attach(["r1"]);

		const [pivot] = await conn.query<Record<string, unknown>>(
			"SELECT user_code FROM lz_pivot",
		);
		// The pivot FK carries the current key ("ZZZ"), NOT the captured "ABC".
		expect(pivot.user_code).toBe("ZZZ");
	});
});
