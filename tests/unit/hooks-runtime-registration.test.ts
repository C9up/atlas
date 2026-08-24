/**
 * `Model.before()` / `Model.after()` — Lucid's runtime hook registration.
 *
 * Read off @adonisjs/lucid's published base_model/index.js:
 *
 *   static before(event, handler) { this.$hooks.add(`before:${event}`, handler) }
 *   static after(event, handler)  { this.$hooks.add(`after:${event}`, handler) }
 *
 * The decorators cover a hook declared in the entity's own class body. This is
 * for one that is not: a plugin, a test, or a package wiring itself into an
 * app's models has no class body to decorate.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { fireHooks, removeHook } from "../../src/decorators/hooks.js";

@Entity("widgets")
class Widget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

describe("atlas > runtime hook registration (Lucid parity)", () => {
	beforeEach(() => {
		// Each test registers its own; drop anything a previous one left.
		for (const kind of ["beforeSave", "afterCreate"] as const) {
			// biome-ignore lint/suspicious/noExplicitAny: test cleanup over a union
			const registry = Reflect.getOwnMetadata(
				Symbol.for("atlas:hooks"),
				Widget,
			) as any;
			if (registry?.[kind]) registry[kind] = [];
		}
	});

	it("runs a hook added with Model.before()", async () => {
		const seen: string[] = [];
		Widget.before("save", (entity) => {
			seen.push(String((entity as Widget).name));
		});

		const widget = new Widget();
		widget.name = "cog";
		await fireHooks(Widget, "beforeSave", widget);

		expect(seen).toEqual(["cog"]);
	});

	it("runs a hook added with Model.after()", async () => {
		const seen: string[] = [];
		Widget.after("create", () => {
			seen.push("created");
		});

		await fireHooks(Widget, "afterCreate", new Widget());

		expect(seen).toEqual(["created"]);
	});

	it("keeps registration order within a class", async () => {
		const order: number[] = [];
		Widget.before("save", () => {
			order.push(1);
		});
		Widget.before("save", () => {
			order.push(2);
		});

		await fireHooks(Widget, "beforeSave", new Widget());

		expect(order).toEqual([1, 2]);
	});

	it("awaits an async hook before moving on", async () => {
		const order: string[] = [];
		Widget.before("save", async () => {
			await new Promise((resolve) => setTimeout(resolve, 5));
			order.push("slow");
		});
		Widget.before("save", () => {
			order.push("fast");
		});

		await fireHooks(Widget, "beforeSave", new Widget());

		// Sequential, so a hook can mutate the entity before the next one reads it.
		expect(order).toEqual(["slow", "fast"]);
	});

	it("removeHook takes one back out", async () => {
		const seen: string[] = [];
		const handler = () => {
			seen.push("ran");
		};
		Widget.before("save", handler);

		expect(removeHook(Widget, "beforeSave", handler)).toBe(true);
		await fireHooks(Widget, "beforeSave", new Widget());

		expect(seen).toEqual([]);
	});

	it("attaches the hook to the SUBCLASS, not to BaseEntity", async () => {
		// `static before()` registers on `this`. Written as `BaseEntity` instead
		// — which is what biome's noThisInStatic rewrites it to — every entity in
		// the app would share one registry and a Widget hook would fire on an
		// unrelated model.
		@Entity("gadgets")
		class Gadget extends BaseEntity {
			@PrimaryKey() declare id: string;
		}

		const seen: string[] = [];
		Widget.before("save", () => {
			seen.push("widget");
		});

		await fireHooks(Gadget, "beforeSave", new Gadget());

		expect(seen).toEqual([]);
	});

	it("aborts the operation when a hook throws", async () => {
		Widget.before("save", () => {
			throw new Error("no");
		});

		await expect(fireHooks(Widget, "beforeSave", new Widget())).rejects.toThrow(
			"no",
		);
	});
});
