/**
 * The Adonis Lucid–style subpaths (`@c9up/atlas/orm` | `/schema` | `/factories`)
 * resolve and re-export their expected surface.
 */
import { describe, expect, it } from "vitest";
import { Factory, factory } from "../../src/factories.js";
import { Migration, Schema, TableBuilder } from "../../src/lucid-schema.js";
import {
	BaseModel,
	belongsTo,
	Column,
	column,
	hasMany,
	manyToMany,
} from "../../src/orm.js";

describe("atlas > Lucid-style subpath exports", () => {
	it("@c9up/atlas/orm exposes the model + decorators", () => {
		expect(typeof BaseModel).toBe("function");
		expect(typeof Column).toBe("function");
		expect(typeof column).toBe("function");
		expect(typeof belongsTo).toBe("function");
		expect(typeof hasMany).toBe("function");
		expect(typeof manyToMany).toBe("function");
	});

	it("@c9up/atlas/schema exposes the migration + schema builders", () => {
		expect(typeof Migration).toBe("function");
		expect(typeof Schema).toBe("function");
		expect(typeof TableBuilder).toBe("function");
	});

	it("@c9up/atlas/factories exposes the factory", () => {
		expect(typeof Factory.define).toBe("function");
		expect(typeof factory).toBe("function");
	});
});
