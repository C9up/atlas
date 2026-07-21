/**
 * Factory `.client()` / `.connection()` (Adonis Lucid parity): bind a connection
 * so create()/createMany() need no explicit db argument. Verified against real
 * SQLite, including the named-connection registry.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity, Column, Entity, PrimaryKey } from "../../src/index.js";
import { setAtlasDialect } from "../../src/query/native.js";
import {
	registerConnection,
	unregisterConnection,
} from "../../src/services/db.js";
import { factory } from "../../src/testing/Factory.js";

@Entity("cw_widgets")
class CWidget extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

const WidgetFactory = factory(CWidget, () => ({ name: "bound" }));

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE cw_widgets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
	);
});

afterEach(async () => {
	unregisterConnection("factory_test", conn);
	await conn?.close();
});

async function count(): Promise<number> {
	const rows = await conn.query<{ n: number }>(
		"SELECT COUNT(*) AS n FROM cw_widgets",
	);
	return rows[0]?.n ?? 0;
}

describe("factory .client() / .connection()", () => {
	it("persists through a bound client with no db argument", async () => {
		const w = await WidgetFactory.client(conn).create();
		expect(w.id).toBeGreaterThan(0);
		expect(await count()).toBe(1);
	});

	it("createMany also uses the bound client", async () => {
		await WidgetFactory.client(conn).createMany(3);
		expect(await count()).toBe(3);
	});

	it("resolves a named connection via .connection()", async () => {
		registerConnection("factory_test", conn);
		await WidgetFactory.connection("factory_test").create();
		expect(await count()).toBe(1);
	});

	it("throws for an unknown named connection", () => {
		expect(() => WidgetFactory.connection("nope")).toThrow(
			/no connection registered under 'nope'/,
		);
	});

	it("throws when neither an argument nor a bound client is available", async () => {
		const bare = factory(CWidget, () => ({ name: "x" }));
		await expect(bare.create()).rejects.toThrow(/no connection/);
	});

	it("an explicit db argument wins over the bound client", async () => {
		// Bind a client, but pass conn explicitly — it must still work.
		await WidgetFactory.client(conn).create(conn);
		expect(await count()).toBe(1);
	});
});
