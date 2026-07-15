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
	// DB-generated default on an overridden column — exercises the INSERT ...
	// RETURNING reinjection path (must land on `note`, not `dbNote`).
	@Column({ columnName: "db_note" }) declare note: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE gizmos (id TEXT PRIMARY KEY, full_label TEXT, owner_ref TEXT, db_note TEXT DEFAULT 'auto')",
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

	it("INSERT ... RETURNING reinjects a DB default onto the right property (not dbNote)", async () => {
		// `note` is omitted → the DB DEFAULT 'auto' fills db_note → RETURNING must
		// land it on `note`, NOT a spurious `dbNote` (the reverse-map bug).
		const g = await Gizmo.create({ id: "2", label: "x", ownerId: "u2" });
		expect(g.note).toBe("auto");
		expect(g.dbNote).toBeUndefined();
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

	it("aggregates resolve the overridden column (would be 'no such column' if not)", async () => {
		// count('label') must compile to COUNT("full_label"); an unresolved
		// COUNT("label") throws "no such column: label" on sqlite.
		const n = await Gizmo.query().count("label");
		expect(n).toBeGreaterThanOrEqual(1);
	});

	it("select('label') resolves to the overridden column", async () => {
		const { sql } = Gizmo.query().select("label").toSQL();
		expect(sql).toMatch(/full_label/);
		expect(sql).not.toMatch(/"label"/);
		// `*` and expressions are left untouched.
		expect(Gizmo.query().select("*").toSQL().sql).toMatch(/\*/);
	});

	it("whereExpr('label', ...) resolves the overridden column", async () => {
		const { sql } = Gizmo.query().whereExpr("label", "=", "x").toSQL();
		expect(sql).toMatch(/full_label/);
		expect(sql).not.toMatch(/"label"/);
	});

	it("having('label', ...) resolves a model column but leaves aggregates/aliases raw", async () => {
		const bare = Gizmo.query()
			.groupBy("label")
			.having("label", "=", "x")
			.toSQL();
		expect(bare.sql).toMatch(/HAVING.*full_label/i);
		// An aggregate/alias is left verbatim (not resolved).
		const agg = Gizmo.query().having("COUNT(*)", ">", 1).toSQL();
		expect(agg.sql).toMatch(/COUNT\(\*\)/i);
	});

	it("cursorPaginate orders by an overridden column and the cursor round-trips", async () => {
		await Gizmo.truncate();
		for (const id of ["1", "2", "3"])
			await Gizmo.create({ id, label: `L${id}`, ownerId: "u", note: "n" });

		const page1 = await Gizmo.query().cursorPaginate({
			orderBy: "label",
			limit: 2,
		});
		expect(page1.items.map((g) => g.label)).toEqual(["L1", "L2"]);
		expect(page1.nextCursor).not.toBeNull();

		// The cursor must carry the label value (read from the property, not the
		// undefined `full_label` key on the entity) so page 2 continues correctly.
		const page2 = await Gizmo.query().cursorPaginate({
			orderBy: "label",
			limit: 2,
			cursor: page1.nextCursor ?? undefined,
		});
		expect(page2.items.map((g) => g.label)).toEqual(["L3"]);
	});

	it("select('label as name') resolves the column part but keeps the alias", async () => {
		const { sql } = Gizmo.query().select("label as name").toSQL();
		expect(sql).toMatch(/full_label/);
		expect(sql).toMatch(/name/i);
		expect(sql).not.toMatch(/"label"/);
	});

	it("create() accepts a DB column-name key and actually persists it", async () => {
		await Gizmo.truncate();
		// Payload keyed by the DB column names, not the TS properties.
		await Gizmo.create({ id: "dbk", full_label: "raw", owner_ref: "o9" });
		const found = await Gizmo.find("dbk");
		// Before the fix this set a phantom `full_label` prop and dropped it on insert.
		expect(found?.label).toBe("raw");
		expect(found?.ownerId).toBe("o9");
	});

	it("updateOrCreateMany matches a DB column-name predicate + payload key", async () => {
		await Gizmo.truncate();
		await Gizmo.create({ id: "k1", label: "orig", ownerId: "o", note: "n" });
		// Predicate key AND row keyed by the DB column name `full_label`.
		await Gizmo.updateOrCreateMany("full_label", [
			{ id: "k1", full_label: "orig", note: "updated" },
			{ id: "k2", full_label: "brand-new", note: "n2" },
		]);
		// k1 matched on full_label='orig' → updated; k2 inserted.
		expect((await Gizmo.find("k1"))?.note).toBe("updated");
		expect((await Gizmo.find("k2"))?.label).toBe("brand-new");
	});
});
