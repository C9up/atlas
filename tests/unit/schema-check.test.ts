/**
 * Schema verification — reconciles model `@Column` metadata against the LIVE
 * (in-memory SQLite) database. Covers the four drift categories, the clean
 * case, and the boot-guard `throw`/`warn` modes.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
	BaseEntity,
	checkSchema,
	Column,
	Entity,
	formatSchemaFindings,
	introspectTable,
	PrimaryKey,
	suggestColumn,
	typesCompatible,
	verifySchema,
} from "../../src/index.js";
import { Database } from "../../src/testing/TestDatabase.js";

@Entity("users")
class GoodUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare age: number;
	@Column() declare createdAt: Date; // → created_at, present + NOT NULL
}

@Entity("users")
class DriftUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare email: string; // ✓
	@Column({ type: "string" }) declare age: number; // ✗ DB age is INTEGER
	@Column() declare emial: string; // ✗ typo → did you mean `email`
	// no `createdAt` → users.created_at NOT NULL is unmapped (reverse drift)
}

@Entity("ghosts")
class GhostModel extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

describe("atlas > schema verification", () => {
	let db: Database;

	beforeAll(async () => {
		db = await Database.memory();
		await db.execute(
			`CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT NOT NULL,
				age INTEGER,
				created_at TEXT NOT NULL
			)`,
		);
	});

	afterAll(async () => {
		await db.close();
	});

	it("reports no findings when the model matches the database", async () => {
		const findings = await checkSchema([GoodUser], db, "sqlite");
		expect(findings).toEqual([]);
	});

	it("detects the four drift categories", async () => {
		const findings = await checkSchema([DriftUser], db, "sqlite");
		const byCol = new Map(findings.map((f) => [f.column, f]));

		expect(byCol.get("emial")?.kind).toBe("missing-in-db");
		expect(byCol.get("emial")?.suggestion).toBe("email");
		expect(byCol.get("age")?.kind).toBe("type-mismatch");
		expect(byCol.get("created_at")?.kind).toBe("missing-in-model");
		expect(byCol.has("email")).toBe(false);
		expect(findings).toHaveLength(3);
	});

	it("flags a model whose table does not exist", async () => {
		const findings = await checkSchema([GhostModel], db, "sqlite");
		expect(findings).toHaveLength(1);
		expect(findings[0]?.kind).toBe("missing-table");
		expect(findings[0]?.table).toBe("ghosts");
	});

	it("introspectTable returns null for an unknown table", async () => {
		expect(await introspectTable(db, "sqlite", "nope")).toBeNull();
	});

	it("introspectTable returns the real column shape", async () => {
		const cols = await introspectTable(db, "sqlite", "users");
		expect(cols?.map((c) => c.name).sort()).toEqual([
			"age",
			"created_at",
			"email",
			"id",
		]);
		const email = cols?.find((c) => c.name === "email");
		expect(email?.nullable).toBe(false);
		const age = cols?.find((c) => c.name === "age");
		expect(age?.nullable).toBe(true);
	});

	it("verifySchema throws (fail-fast) on drift by default", async () => {
		await expect(verifySchema([DriftUser], db, "sqlite")).rejects.toThrow(
			/schema issue/,
		);
	});

	it("verifySchema warns (non-blocking) in warn mode", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		try {
			const findings = await verifySchema([DriftUser], db, "sqlite", {
				mode: "warn",
			});
			expect(findings.length).toBeGreaterThan(0);
			expect(warn).toHaveBeenCalledOnce();
			expect(String(warn.mock.calls[0]?.[0])).toContain("did you mean");
		} finally {
			warn.mockRestore();
		}
	});

	it("verifySchema is silent + returns [] when models match", async () => {
		const findings = await verifySchema([GoodUser], db, "sqlite");
		expect(findings).toEqual([]);
	});

	it("formatSchemaFindings renders a didactic diff", () => {
		const out = formatSchemaFindings([
			{
				entity: "DriftUser",
				table: "users",
				kind: "missing-in-db",
				column: "emial",
				detail: "maps to column `emial`, which does not exist",
				suggestion: "email",
			},
		]);
		expect(out).toContain("users (DriftUser)");
		expect(out).toContain("✗ emial");
		expect(out).toContain("did you mean `email`");
	});
});

describe("atlas > schema verification > units", () => {
	it("typesCompatible: clear num↔text clashes are flagged, ambiguous types pass", () => {
		expect(typesCompatible("string", "INTEGER")).toBe(false);
		expect(typesCompatible("integer", "TEXT")).toBe(false);
		expect(typesCompatible("integer", "INTEGER")).toBe(true);
		expect(typesCompatible("string", "TEXT")).toBe(true);
		expect(typesCompatible("uuid", "TEXT")).toBe(true); // text group
		expect(typesCompatible("boolean", "INTEGER")).toBe(true); // bool stored as int
		// storage-dependent types are never falsely flagged
		expect(typesCompatible("timestamp", "TEXT")).toBe(true);
		expect(typesCompatible("binary", "BLOB")).toBe(true);
	});

	it("suggestColumn finds a near match within edit distance 2", () => {
		expect(suggestColumn("emial", ["email", "age", "id"])).toBe("email");
		expect(suggestColumn("zzzzzz", ["email", "age"])).toBeUndefined();
	});
});
