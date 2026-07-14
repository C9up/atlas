import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import {
	Column,
	Entity,
	HasMany,
	HasOne,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { MassAssignmentError } from "../../src/errors.js";

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

describe("atlas > BaseEntity > Lucid state flags", () => {
	it("a fresh instance is new + local, not persisted/deleted", () => {
		const u = new User();
		expect(u.$isNew).toBe(true);
		expect(u.$isPersisted).toBe(false);
		expect(u.$isLocal).toBe(true);
		expect(u.$isDeleted).toBe(false);
	});

	it("markAsPersisted sets $isPersisted/$isNew but keeps $isLocal (created in memory)", () => {
		const u = new User();
		u.markAsPersisted();
		expect(u.$isPersisted).toBe(true);
		expect(u.$isNew).toBe(false);
		expect(u.$isLocal).toBe(true);
	});

	it("markAsFromDatabase clears $isLocal (fetched instance)", () => {
		const u = new User();
		u.markAsFromDatabase();
		expect(u.$isLocal).toBe(false);
	});

	it("markAsDeleted sets $isDeleted", () => {
		const u = new User();
		u.markAsDeleted();
		expect(u.$isDeleted).toBe(true);
	});

	it("$primaryKeyValue reads the primary-key column", () => {
		const u = new User();
		u.id = 42;
		expect(u.$primaryKeyValue).toBe(42);
	});

	it("state flags never leak into $dirty", () => {
		const u = new User();
		u.email = "a@b";
		u.markAsPersisted();
		expect(u.$dirty).toEqual({});
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

	it("assertMassAssignable enforces guarded/fillable (repo create/createMany/updateOrCreate call this so they can't bypass fill's protection)", () => {
		expect(() => new GuardedPost().assertMassAssignable("authorId")).toThrow(
			MassAssignmentError,
		);
		expect(() => new GuardedPost().assertMassAssignable("title")).not.toThrow();
		expect(() =>
			new FillablePost().assertMassAssignable("title"),
		).not.toThrow();
		expect(() => new FillablePost().assertMassAssignable("authorId")).toThrow(
			MassAssignmentError,
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

	it("hides $extras by default and exposes them only with static serializeExtras (Lucid parity)", () => {
		const u = new User();
		u.id = 1;
		u.setExtra("posts_count", 5);
		// Default OFF: internal aggregates/pivot extras never leak into JSON.
		expect("posts_count" in u.toJSON()).toBe(false);

		class UserWithExtras extends BaseEntity {
			declare id: number;
			static serializeExtras = true;
		}
		const e = new UserWithExtras();
		e.id = 1;
		e.setExtra("posts_count", 5);
		expect(e.toJSON()).toMatchObject({ id: 1, posts_count: 5 });
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

	it("makeHidden hides a field on THIS instance only (Lucid parity)", () => {
		class Row extends BaseEntity {
			declare id: number;
			declare secret: string;
		}
		const a = new Row();
		a.id = 1;
		a.secret = "x";
		expect(a.makeHidden("secret")).toBe(a); // chainable
		expect(a.toJSON()).toEqual({ id: 1 });
		// a sibling instance is unaffected
		const b = new Row();
		b.id = 2;
		b.secret = "y";
		expect(b.toJSON().secret).toBe("y");
	});

	it("makeVisible reveals a statically-hidden field on THIS instance (Lucid parity)", () => {
		class Row extends BaseEntity {
			declare id: number;
			declare token: string;
			static hidden = ["token"];
		}
		const r = new Row();
		r.id = 1;
		r.token = "abc";
		expect(r.toJSON().token).toBeUndefined();
		r.makeVisible("token");
		expect(r.toJSON().token).toBe("abc");
	});

	it("toObject returns raw columns + $extras, ignoring hidden/serializeAs (Lucid parity)", () => {
		class Row extends BaseEntity {
			declare id: number;
			declare secret: string;
			static hidden = ["secret"];
		}
		const r = new Row();
		r.id = 1;
		r.secret = "x";
		r.setExtra("count", 5);
		const obj = r.toObject();
		expect(obj.id).toBe(1);
		expect(obj.secret).toBe("x"); // NOT hidden in toObject
		expect(obj.count).toBe(5); // $extras always included
		// toJSON still applies the hidden allowlist
		expect(r.toJSON().secret).toBeUndefined();
	});
});

@Entity("ser_authors")
class SerAuthor extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

@Entity("ser_books")
class SerBook extends BaseEntity {
	@PrimaryKey() declare id: number;
	// Relation renamed in JSON output.
	@HasOne(() => SerAuthor, { serializeAs: "writer" })
	declare author: SerAuthor;
	// Relation hidden from JSON output.
	@HasMany(() => SerAuthor, { serializeAs: null })
	declare contributors: SerAuthor[];
	// Relation with no serializeAs → keeps its property name.
	@HasOne(() => SerAuthor)
	declare editor: SerAuthor;
}

describe("atlas > BaseEntity > toJSON relation serializeAs", () => {
	it("renames a relation via serializeAs and hides one set to null", () => {
		const a = new SerAuthor();
		a.id = 1;
		a.name = "Ann";

		const book = new SerBook();
		book.id = 10;
		book.author = a;
		book.contributors = [a];
		book.editor = a;

		const json = book.toJSON();
		// Renamed: `author` → `writer`.
		expect("author" in json).toBe(false);
		expect(json.writer).toBeDefined();
		// Hidden: `contributors` (serializeAs: null) is dropped.
		expect("contributors" in json).toBe(false);
		// Untouched: `editor` keeps its property name.
		expect(json.editor).toBeDefined();
	});
});

describe("atlas > BaseEntity > fill/merge allowExtraProperties (Lucid parity)", () => {
	@Entity("accounts_x")
	class Account extends BaseEntity {
		@PrimaryKey() declare id: number;
		@Column() declare balance: number;
	}

	it("fill throws on an undeclared key by default (Lucid strict)", () => {
		const a = new Account();
		expect(() => a.fill({ balance: 10, bogus: 1 })).toThrow(
			/not a declared column/,
		);
	});

	it("fill(payload, true) silently drops undeclared keys", () => {
		const a = new Account();
		a.fill({ balance: 10, bogus: 1 }, true);
		expect(a.balance).toBe(10);
		expect("bogus" in a).toBe(false);
	});

	it("merge throws on an undeclared key by default; merge(_, true) drops it", () => {
		const a = new Account();
		expect(() => a.merge({ bogus: 1 })).toThrow(/not a declared column/);
		a.merge({ balance: 5, bogus: 1 }, true);
		expect(a.balance).toBe(5);
		expect("bogus" in a).toBe(false);
	});
});

describe("atlas > BaseEntity > serialize override hooks (Lucid parity)", () => {
	it("toJSON delegates to serializeAttributes/Relations/Computed, each overridable", () => {
		@Entity("accounts")
		class Account extends BaseEntity {
			@PrimaryKey() declare id: number;
			@Column() declare balance: number;
			// Override just the attributes hook: round the balance in JSON output.
			protected override serializeAttributes(): Record<string, unknown> {
				const base = super.serializeAttributes();
				return { ...base, balance: Math.round(this.balance) };
			}
		}
		const a = new Account();
		a.id = 1;
		a.balance = 9.87;
		expect(a.toJSON()).toEqual({ id: 1, balance: 10 });
	});
});
