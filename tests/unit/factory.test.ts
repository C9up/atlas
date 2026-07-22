import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { Factory, factory } from "../../src/testing/Factory.js";

@Entity("users")
class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare name: string;
	@Column() declare role: string;
}

const baseDefaults = () => ({
	email: "default@example.com",
	name: "Default",
	role: "user",
});

describe("atlas > factory > faker context", () => {
	it("passes a faker instance to the define callback (Lucid parity)", () => {
		const f = factory(User, ({ faker }) => ({
			email: faker.internet.email(),
			name: faker.person.fullName(),
			role: "user",
		}));
		const a = f.make();
		const b = f.make();
		// Faker produced real, distinct values.
		expect(typeof a.email).toBe("string");
		expect(a.email).toContain("@");
		expect(a.email).not.toBe(b.email);
	});

	it("still accepts a zero-argument callback (backward compatible)", () => {
		const f = factory(User, () => ({
			email: "static@example.com",
			name: "Static",
			role: "user",
		}));
		expect(f.make().email).toBe("static@example.com");
	});
});

describe("atlas > factory > Factory.define().build()", () => {
	it("builds the same factory as the factory() shorthand (Lucid define/build)", () => {
		const UserFactory = Factory.define(User, baseDefaults).build();
		const user = UserFactory.make();
		expect(user).toBeInstanceOf(User);
		expect(user.email).toBe("default@example.com");
	});
});

describe("atlas > factory > runtime context (ctx)", () => {
	it("passes ctx.isStubbed to the defaults callback and state callbacks (Lucid)", () => {
		const seen: boolean[] = [];
		const f = factory(User, (ctx) => {
			seen.push(ctx.isStubbed);
			return baseDefaults();
		}).state("s", (_, ctx) => {
			seen.push(ctx.isStubbed);
		});

		f.apply("s").makeStubbed();
		expect(seen).toEqual([true, true]); // stubbed build

		seen.length = 0;
		f.apply("s").make();
		expect(seen).toEqual([false, false]); // plain make
	});
});

describe("atlas > factory > before/after hooks", () => {
	it("before('makeStubbed') sets the PK; after('makeStubbed') runs (Lucid)", () => {
		const log: string[] = [];
		const f = factory(User, baseDefaults)
			.before("makeStubbed", (_, m) => {
				m.id = 999;
			})
			.after("makeStubbed", (_, m) => {
				log.push(`after:${m.id}`);
			});
		const u = f.makeStubbed();
		// The before hook set the PK, so the auto stub-id did NOT override it.
		expect(u.id).toBe(999);
		expect(log).toEqual(["after:999"]);
	});
});

describe("atlas > factory > make", () => {
	it("returns an un-persisted model instance carrying the defaults (Lucid make)", () => {
		const f = factory(User, baseDefaults);
		const user = f.make();
		// Lucid: an instance, not a plain object — no PK, not persisted.
		expect(user).toBeInstanceOf(User);
		expect(user.$isPersisted).toBe(false);
		expect(user.id).toBeUndefined();
		expect(user.email).toBe("default@example.com");
		expect(user.name).toBe("Default");
		expect(user.role).toBe("user");
	});

	it("merges pending overrides into the next make() call", () => {
		const f = factory(User, baseDefaults);
		const result = f.merge({ name: "Alice" }).make();
		expect(result.name).toBe("Alice");
		expect(result.email).toBe("default@example.com");
	});

	it("resets pending overrides after consumption", () => {
		const f = factory(User, baseDefaults);
		f.merge({ name: "Once" }).make();
		const next = f.make();
		expect(next.name).toBe("Default");
	});

	it("compounds successive merge() calls before consumption", () => {
		const f = factory(User, baseDefaults);
		const result = f.merge({ name: "A" }).merge({ role: "admin" }).make();
		expect(result.name).toBe("A");
		expect(result.role).toBe("admin");
	});
});

describe("atlas > factory > state / apply", () => {
	it("applies a declared state mutator", () => {
		const f = factory(User, baseDefaults).state("admin", (u) => {
			u.role = "admin";
		});
		expect(f.apply("admin").make().role).toBe("admin");
	});

	it("composes multiple states in declaration order", () => {
		const f = factory(User, baseDefaults)
			.state("admin", (u) => {
				u.role = "admin";
			})
			.state("verified", (u) => {
				u.email = `verified-${u.email}`;
			});

		const result = f.apply("admin", "verified").make();
		expect(result.role).toBe("admin");
		expect(result.email).toBe("verified-default@example.com");
	});

	it("throws a descriptive error when applying an undeclared state", () => {
		const f = factory(User, baseDefaults);
		expect(() => f.apply("ghost").make()).toThrow(
			/Factory state 'ghost' is not defined on UserFactory/,
		);
	});

	it("resets the active states after consumption", () => {
		const f = factory(User, baseDefaults).state("admin", (u) => {
			u.role = "admin";
		});
		f.apply("admin").make();
		expect(f.make().role).toBe("user");
	});
});

describe("atlas > factory > makeMany", () => {
	it("re-evaluates defaults for each row", () => {
		let i = 0;
		const f = factory(User, () => ({
			email: `u${i++}@x`,
			name: "n",
			role: "user",
		}));
		const rows = f.makeMany(3);
		expect(rows.map((r) => r.email)).toEqual(["u0@x", "u1@x", "u2@x"]);
	});

	it("keeps overrides + states active for every row in the batch", () => {
		const f = factory(User, baseDefaults).state("admin", (u) => {
			u.role = "admin";
		});
		const rows = f.merge({ name: "Alice" }).apply("admin").makeMany(2);
		for (const row of rows) {
			expect(row.name).toBe("Alice");
			expect(row.role).toBe("admin");
		}
	});

	it("returns an empty array for count=0 and resets pending state", () => {
		const f = factory(User, baseDefaults);
		expect(f.merge({ name: "X" }).makeMany(0)).toEqual([]);
		// pending overrides were captured in makeMany; subsequent make()
		// should see fresh defaults.
		expect(f.make().name).toBe("Default");
	});
});

describe("atlas > factory > mergeRecursive", () => {
	const nestedDefaults = () => ({
		name: "Default",
		settings: { theme: "light", notifications: { email: true, sms: false } },
	});

	it("deep-merges nested plain objects instead of replacing them", () => {
		const f = factory(User, nestedDefaults);
		const result = f
			.mergeRecursive({ settings: { notifications: { sms: true } } })
			.make();
		// Sibling keys survive at every level; only the deep leaf changed.
		expect(result.settings).toEqual({
			theme: "light",
			notifications: { email: true, sms: true },
		});
	});

	it("replaces arrays and primitives wholesale (only plain objects recurse)", () => {
		const f = factory(User, () => ({ tags: ["a", "b"], settings: { x: 1 } }));
		const result = f.mergeRecursive({ tags: ["c"], settings: { x: 2 } }).make();
		expect(result.tags).toEqual(["c"]);
		expect(result.settings).toEqual({ x: 2 });
	});

	it("refuses to write prototype-pollution keys", () => {
		const f = factory(User, baseDefaults);
		// JSON.parse produces an own "__proto__" key (not the real prototype),
		// which a naive recursive merge would walk into and pollute Object.prototype.
		const payload = JSON.parse('{"__proto__": {"polluted": true}}');
		f.mergeRecursive(payload).make();
		const probe: Record<string, unknown> = {};
		expect(probe.polluted).toBeUndefined();
		expect(Object.prototype).not.toHaveProperty("polluted");
	});
});

describe("atlas > factory > makeStubbed", () => {
	it("returns an entity instance carrying the data", () => {
		const f = factory(User, baseDefaults);
		const stub = f.makeStubbed();
		expect(stub).toBeInstanceOf(User);
		expect(stub.email).toBe("default@example.com");
	});

	it("marks the instance persisted and gives it a stub id", () => {
		const f = factory(User, baseDefaults);
		const stub = f.makeStubbed();
		expect(stub.$isPersisted).toBe(true);
		expect(typeof stub.id).toBe("number");
	});

	it("assigns a distinct stub id to each build", () => {
		const f = factory(User, baseDefaults);
		expect(f.makeStubbed().id).not.toBe(f.makeStubbed().id);
	});

	it("respects an explicitly provided primary key", () => {
		const f = factory(User, baseDefaults);
		expect(f.merge({ id: 99 }).makeStubbed().id).toBe(99);
	});

	it("applies merge + state to the stubbed entity", () => {
		const f = factory(User, baseDefaults).state("admin", (u) => {
			u.role = "admin";
		});
		const stub = f.merge({ name: "Bob" }).apply("admin").makeStubbed();
		expect(stub.name).toBe("Bob");
		expect(stub.role).toBe("admin");
	});

	it("resets pending state after stubbing", () => {
		const f = factory(User, baseDefaults);
		f.merge({ name: "Once" }).makeStubbed();
		expect(f.make().name).toBe("Default");
	});
});

describe("atlas > factory > pending state isolation on error", () => {
	it("a create() that throws for a missing connection does not leak pending state", async () => {
		const f = factory(User, baseDefaults);
		// No db passed and no bound client → resolveConnection throws. The pending
		// merge must NOT survive into the next call.
		await expect(f.merge({ name: "Leaked" }).create()).rejects.toThrow(
			/no connection/i,
		);
		const next = f.make();
		expect(next.name).toBe("Default");
	});
});

describe("atlas > factory > tap", () => {
	it("runs the tap callback on the stubbed instance", () => {
		const f = factory(User, baseDefaults);
		const stub = f
			.tap((u) => {
				u.name = "Tapped";
			})
			.makeStubbed();
		expect(stub.name).toBe("Tapped");
	});

	it("runs multiple taps in order", () => {
		const f = factory(User, baseDefaults);
		const stub = f
			.tap((u) => {
				u.name = "one";
			})
			.tap((u) => {
				u.name = `${String(u.name)}-two`;
			})
			.makeStubbed();
		expect(stub.name).toBe("one-two");
	});

	it("resets taps after consumption", () => {
		const f = factory(User, baseDefaults);
		f.tap((u) => {
			u.name = "Once";
		}).makeStubbed();
		expect(f.makeStubbed().name).toBe("Default");
	});

	it("applies the tap to every row of makeStubbedMany", () => {
		const f = factory(User, baseDefaults);
		const stubs = f
			.tap((u) => {
				u.role = "tapped";
			})
			.makeStubbedMany(3);
		expect(stubs.every((s) => s.role === "tapped")).toBe(true);
	});
});

describe("atlas > factory > makeStubbedMany", () => {
	it("returns N persisted instances, each with its own stub id", () => {
		let i = 0;
		const f = factory(User, () => ({
			email: `u${i++}@x`,
			name: "n",
			role: "user",
		}));
		const stubs = f.makeStubbedMany(3);
		expect(stubs).toHaveLength(3);
		for (const s of stubs) {
			expect(s).toBeInstanceOf(User);
			expect(s.$isPersisted).toBe(true);
		}
		// Defaults re-evaluated per row, and every stub id is distinct.
		expect(stubs.map((s) => s.email)).toEqual(["u0@x", "u1@x", "u2@x"]);
		expect(new Set(stubs.map((s) => s.id)).size).toBe(3);
	});

	it("keeps overrides + state active for the whole batch, then resets", () => {
		const f = factory(User, baseDefaults).state("admin", (u) => {
			u.role = "admin";
		});
		const stubs = f.merge({ name: "Alice" }).apply("admin").makeStubbedMany(2);
		for (const s of stubs) {
			expect(s.name).toBe("Alice");
			expect(s.role).toBe("admin");
		}
		expect(f.make().name).toBe("Default");
	});
});
