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
import {
	BaseSeeder,
	dbSeedCommand,
	makeSeederCommand,
} from "../../src/seeders.js";
import type { BelongsTo, HasMany } from "../../src/types/relations.js";

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

	it("@c9up/atlas/seeders exposes the base seeder + CLI commands", () => {
		expect(typeof BaseSeeder).toBe("function");
		expect(typeof makeSeederCommand).toBe("function");
		expect(typeof dbSeedCommand).toBe("function");
	});

	it("@c9up/atlas/types/relations helpers resolve to atlas shapes", () => {
		class Post {}
		// Compile-time: HasMany<typeof Post> === Post[], BelongsTo<typeof Post> === Post | null.
		const many: HasMany<typeof Post> = [new Post()];
		const one: BelongsTo<typeof Post> = null;
		expect(Array.isArray(many)).toBe(true);
		expect(one).toBeNull();
	});
});
