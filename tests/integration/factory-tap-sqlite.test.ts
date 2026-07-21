/**
 * Factory `.tap()` on the persistence path against real SQLite: a tap mutates
 * the model instance BEFORE the INSERT, so the mutated value must land in the
 * database (Adonis Lucid parity).
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity, Column, Entity, PrimaryKey } from "../../src/index.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { factory } from "../../src/testing/Factory.js";

@Entity("tap_widgets")
class TapWidget extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@Column() declare slug: string;
}

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE tap_widgets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, slug TEXT)",
	);
});

afterEach(async () => {
	await conn?.close();
});

describe("factory .tap() on create", () => {
	it("persists a value the tap derived from the instance", async () => {
		const WidgetFactory = factory(TapWidget, () => ({
			name: "Hello World",
			slug: "",
		})).tap((w) => {
			w.slug = String(w.name).toLowerCase().replace(/\s+/g, "-");
		});

		const widget = await WidgetFactory.create(conn);
		expect(widget.slug).toBe("hello-world");

		// The mutated value is on disk, not just in memory.
		const rows = await conn.query<{ slug: string }>(
			`SELECT slug FROM tap_widgets WHERE id = ${widget.id}`,
		);
		expect(rows[0]?.slug).toBe("hello-world");
	});

	it("taps every row of createMany", async () => {
		let i = 0;
		const WidgetFactory = factory(TapWidget, () => ({
			name: `w${i++}`,
			slug: "",
		})).tap((w) => {
			w.slug = `tagged-${String(w.name)}`;
		});

		const widgets = await WidgetFactory.createMany(3, conn);
		expect(widgets.map((w) => w.slug)).toEqual([
			"tagged-w0",
			"tagged-w1",
			"tagged-w2",
		]);
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM tap_widgets WHERE slug LIKE 'tagged-%'",
		);
		expect(rows[0]?.n).toBe(3);
	});
});
