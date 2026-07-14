import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// Property `label` maps to the non-conventional DB column `full_label`, and
// `ownerId` → `owner_ref`. The default convention would give `label`/`owner_id`.
class Gizmo extends BaseModel {
	static override table = "gizmos";
	@PrimaryKey() declare id: string;
	@Column({ columnName: "full_label" }) declare label: string;
	@Column({ columnName: "owner_ref" }) declare ownerId: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE gizmos (id TEXT PRIMARY KEY, full_label TEXT, owner_ref TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > @Column({ columnName }) override (sqlite, e2e)", () => {
	it("create() writes to the overridden column and find() hydrates it back", async () => {
		const g = await Gizmo.create({ id: "1", label: "hammer", ownerId: "u1" });
		expect(g.label).toBe("hammer");

		// Raw row uses the real DB column names, not the property names.
		const [raw] = await conn.query<Record<string, unknown>>(
			"SELECT * FROM gizmos WHERE id = '1'",
		);
		expect(raw.full_label).toBe("hammer");
		expect(raw.owner_ref).toBe("u1");
		expect("label" in raw).toBe(false);

		// Hydration maps the DB column back to the property.
		const found = await Gizmo.find("1");
		expect(found?.label).toBe("hammer");
		expect(found?.ownerId).toBe("u1");
	});

	it("save() updates the overridden column by dirty property", async () => {
		const g = await Gizmo.find("1");
		if (!g) throw new Error("expected row");
		g.label = "mallet";
		await g.save();
		const [raw] = await conn.query<Record<string, unknown>>(
			"SELECT full_label FROM gizmos WHERE id = '1'",
		);
		expect(raw.full_label).toBe("mallet");
		expect((await Gizmo.find("1"))?.label).toBe("mallet");
	});

	it("where()/orderBy() on the property resolve to the overridden column in SQL", async () => {
		const q = Gizmo.query().where("label", "mallet").orderBy("ownerId", "desc");
		const { sql } = q.toSQL();
		expect(sql).toMatch(/full_label/);
		expect(sql).toMatch(/owner_ref/);
		// The property names must NOT leak into the SQL as bare identifiers.
		expect(sql).not.toMatch(/"label"/);
		expect(sql).not.toMatch(/"ownerId"/);

		const rows = await q.exec();
		expect(rows.map((r) => r.label)).toEqual(["mallet"]);
	});
});
