import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import { MassAssignmentError } from "../../src/errors.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";

@Entity("users")
class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare name: string;
	@Column() declare role: string;
}

@Entity("posts")
class FillablePost extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare title: string;
	@Column() declare body: string;
	@Column() declare authorId: number;
	static fillable = ["title", "body"];
}

@Entity("posts")
class GuardedPost extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare title: string;
	@Column() declare authorId: number;
	static guarded = ["authorId"];
}

@Entity("posts")
class AmbiguousPost extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare title: string;
	static fillable = ["title"];
	static guarded = ["title"];
}

describe("atlas > BaseEntity > setProp / extras", () => {
	it("setProp assigns dynamically", () => {
		const u = new User();
		u.setProp("email", "x@y");
		expect(u.email).toBe("x@y");
	});

	it("setExtra/getExtra round-trips through $extras", () => {
		const u = new User();
		u.setExtra("posts_count", 7);
		expect(u.getExtra("posts_count")).toBe(7);
	});

	it("getExtra returns the default when the key is absent", () => {
		const u = new User();
		expect(u.getExtra("missing", "fallback")).toBe("fallback");
	});
});

describe("atlas > BaseEntity > markAsPersisted / dirty / rollback", () => {
	it("$dirty is empty immediately after markAsPersisted", () => {
		const u = new User();
		u.email = "a@a";
		u.name = "A";
		u.markAsPersisted();
		expect(u.$dirty).toEqual({});
		expect(u.isDirty()).toBe(false);
	});

	it("isDirty(field) reports per-column dirtiness", () => {
		const u = new User();
		u.email = "a@a";
		u.markAsPersisted();
		u.email = "b@b";
		expect(u.isDirty("email")).toBe(true);
		expect(u.isDirty("name")).toBe(false);
	});

	it("rollback restores the previously persisted reference for dirty columns", () => {
		const u = new User();
		u.email = "snapshot@x";
		u.markAsPersisted();
		u.email = "transient@x";
		u.rollback();
		expect(u.email).toBe("snapshot@x");
	});

	it("Date columns are compared by getTime() for dirty detection", () => {
		class TimedEntity extends BaseEntity {
			declare ts: Date;
		}
		const t = new TimedEntity();
		t.ts = new Date("2024-01-01T00:00:00Z");
		t.markAsPersisted();
		// Replace with a NEW Date object representing the same instant.
		t.ts = new Date("2024-01-01T00:00:00Z");
		expect(t.isDirty("ts")).toBe(false);
	});
});

describe("atlas > BaseEntity > fill / merge", () => {
	it("fill assigns whitelisted columns and rejects others", () => {
		const p = new FillablePost();
		p.fill({ title: "T", body: "B" });
		expect(p.title).toBe("T");
		expect(p.body).toBe("B");
		expect(() => p.fill({ authorId: 99 })).toThrow(MassAssignmentError);
	});

	it("fill with `guarded` rejects only the guarded keys", () => {
		const p = new GuardedPost();
		p.fill({ title: "T" });
		expect(p.title).toBe("T");
		expect(() => p.fill({ authorId: 99 })).toThrow(MassAssignmentError);
	});

	it("fill resets unspecified fillable fields that have no $original snapshot", () => {
		const p = new FillablePost();
		p.title = "first";
		p.body = "first body";
		p.fill({ title: "only-title" });
		// `body` not in payload, no $original (entity is fresh) → property removed.
		expect("body" in p).toBe(false);
	});

	it("fill restores unspecified fillable fields from $original when persisted", () => {
		const p = new FillablePost();
		p.title = "original-title";
		p.body = "original-body";
		p.markAsPersisted();
		p.fill({ title: "new-title" });
		// `body` absent from the payload, but the persisted snapshot held it.
		expect(p.body).toBe("original-body");
	});

	it("fill throws when both fillable AND guarded are declared", () => {
		const p = new AmbiguousPost();
		expect(() => p.fill({ title: "T" })).toThrow(
			/both 'fillable' and 'guarded'/,
		);
	});

	it("merge patches only the specified keys (no reset)", () => {
		const p = new FillablePost();
		p.title = "t";
		p.body = "b";
		p.merge({ title: "new" });
		expect(p.title).toBe("new");
		expect(p.body).toBe("b");
	});

	it("merge throws on a guarded field", () => {
		const p = new GuardedPost();
		expect(() => p.merge({ authorId: 99 })).toThrow(MassAssignmentError);
	});

	it("merge throws when both fillable AND guarded are declared", () => {
		const p = new AmbiguousPost();
		expect(() => p.merge({ title: "T" })).toThrow(
			/both 'fillable' and 'guarded'/,
		);
	});
});

describe("atlas > BaseEntity > domain events", () => {
	it("addDomainEvent + getDomainEvents non-destructive read", () => {
		const u = new User();
		u.addDomainEvent("UserCreated", { id: 1 });
		u.addDomainEvent("UserVerified", { id: 1 });
		expect(u.hasDomainEvents()).toBe(true);
		expect(u.getDomainEvents()).toEqual([
			{ name: "UserCreated", data: { id: 1 } },
			{ name: "UserVerified", data: { id: 1 } },
		]);
		// Read is non-destructive — events still present.
		expect(u.getDomainEvents()).toHaveLength(2);
	});

	it("clearDomainEvents removes all", () => {
		const u = new User();
		u.addDomainEvent("X", {});
		u.clearDomainEvents();
		expect(u.hasDomainEvents()).toBe(false);
		expect(u.getDomainEvents()).toEqual([]);
	});

	it("flushDomainEvents returns then clears atomically", () => {
		const u = new User();
		u.addDomainEvent("A", {});
		u.addDomainEvent("B", {});
		const out = u.flushDomainEvents();
		expect(out.map((e) => e.name)).toEqual(["A", "B"]);
		expect(u.hasDomainEvents()).toBe(false);
	});
});

describe("atlas > BaseEntity > unhydrated lazy-load methods throw", () => {
	const messages: Array<[string, () => Promise<unknown>]> = [
		["refresh", () => new User().refresh()],
		["fresh", () => new User().fresh()],
		["loadCount", () => new User().loadCount("posts")],
		[
			"loadAggregate",
			() =>
				new User().loadAggregate("posts", () => {
					return undefined;
				}),
		],
		["load", () => new User().load("posts")],
	];

	for (const [name, invoke] of messages) {
		it(`${name}() rejects when the entity has no repo back-reference`, async () => {
			await expect(invoke()).rejects.toThrow(
				new RegExp(`${name}\\(\\) requires the entity to be hydrated`),
			);
		});
	}

	it("related() throws synchronously when no repo back-reference is set", () => {
		expect(() => new User().related("posts")).toThrow(
			/related\(\) requires the entity to be hydrated/,
		);
	});
});

describe("atlas > BaseEntity > toJSON", () => {
	it("includes column values and excludes internals", () => {
		const u = new User();
		u.id = 1;
		u.email = "a@a";
		u.name = "A";
		u.role = "user";
		const json = u.toJSON();
		expect(json).toMatchObject({
			id: 1,
			email: "a@a",
			name: "A",
			role: "user",
		});
		// Internal $dirty/$extras/$original keys are NOT serialized.
		expect("$dirty" in json).toBe(false);
		expect("$extras" in json).toBe(false);
		expect("$original" in json).toBe(false);
	});

	it("merges $extras values on top of column values", () => {
		const u = new User();
		u.id = 1;
		u.setExtra("posts_count", 5);
		expect(u.toJSON()).toMatchObject({ id: 1, posts_count: 5 });
	});

	it("respects `static hidden`", () => {
		class Secretive extends BaseEntity {
			declare id: number;
			declare password: string;
			static hidden = ["password"];
		}
		const s = new Secretive();
		s.id = 1;
		s.password = "leaked";
		const json = s.toJSON();
		expect(json.id).toBe(1);
		expect(json.password).toBeUndefined();
	});

	it("respects `static visible` (allowlist mode)", () => {
		class Restricted extends BaseEntity {
			declare id: number;
			declare a: string;
			declare b: string;
			static visible = ["a"];
		}
		const r = new Restricted();
		r.id = 1;
		r.a = "ok";
		r.b = "drop";
		const json = r.toJSON();
		expect(json).toEqual({ a: "ok" });
	});
});
