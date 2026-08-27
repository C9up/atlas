import { describe, expect, it } from "vitest";
import * as atlas from "../src/index.js";

/**
 * A type declared and exported by its module, but named by no export path, is
 * invisible: `import type { CreateOptions } from '@c9up/atlas'` fails with
 * TS2305, and the consumer recopies the shape by hand — which then drifts the
 * day the real one changes.
 *
 * The rule this guards: if a public method takes it or hands it back, it has
 * to be nameable from the package root. Each case below both names the type
 * and pins the shape, so a rename is caught here rather than downstream.
 */
describe("public surface", () => {
	it("exports the classes a public method hands back", () => {
		// `paginate()` returns a Paginator; unexported, an app could not name
		// the value it had just been given.
		expect(atlas.Paginator).toBeDefined();
	});

	it("exports the option type `create` accepts", () => {
		const options: atlas.CreateOptions = {
			quiet: true,
			allowExtraProperties: false,
		};
		expect(options).toEqual({ quiet: true, allowExtraProperties: false });
	});

	it("exports the transaction option types", () => {
		const level: atlas.IsolationLevel = "serializable";
		const options: atlas.TransactionOptions = { isolationLevel: level };
		expect(options.isolationLevel).toBe("serializable");
	});

	it("exports the vocabulary a migration callback is written in", () => {
		// onDelete(action), foreign(...) and the alter operations all take
		// these, so a migration pulling one into a named constant needs to be
		// able to spell the type.
		const onDelete: atlas.ReferentialAction = "cascade";
		const reference: atlas.ForeignKeyReference = {
			table: "users",
			columns: ["id"],
			onDelete,
		};
		expect(reference.onDelete).toBe("cascade");

		const index: atlas.IndexDefinition = {
			name: "users_email_index",
			columns: ["email"],
			unique: true,
		};
		expect(index.columns).toEqual(["email"]);
	});

	it("exports the testing types a helper signature needs", () => {
		// Named, not built: FactoryContext carries a live Faker, and the point
		// here is that a helper can spell the parameter type at all.
		const reader = (context: atlas.FactoryContext): boolean =>
			context.isStubbed;
		expect(typeof reader).toBe("function");
	});
});
