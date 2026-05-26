import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { factory } from "../../src/testing/Factory.js";

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

describe("atlas > factory > make", () => {
	it("returns the defaults shape verbatim", () => {
		const f = factory(User, baseDefaults);
		expect(f.make()).toEqual({
			email: "default@example.com",
			name: "Default",
			role: "user",
		});
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

describe("atlas > factory > makeStubbed", () => {
	it("returns an entity instance carrying the data", () => {
		const f = factory(User, baseDefaults);
		const stub = f.makeStubbed();
		expect(stub).toBeInstanceOf(User);
		expect(stub.email).toBe("default@example.com");
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
