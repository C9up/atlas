/**
 * `createMany` fills in what `create` fills in.
 *
 * The batch path built its rows straight from the entities, so it skipped the
 * two things `#insert` does first: generating a `@PrimaryKey({ generated })`
 * and stamping an `autoCreate` timestamp. A generated key therefore reached the
 * database as null and the insert failed on the not-null constraint, while the
 * same entity created on its own went through fine — and applications worked
 * around it by generating the id themselves.
 *
 * The mysql branch was never affected: it inserts row by row through `#insert`.
 * Only the multi-row path bypassed it.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseRepository } from "../../src/BaseRepository.js";
import {
	BaseEntity,
	Column,
	column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("holdings")
class Holding extends BaseEntity {
	@PrimaryKey({ generated: "uuid" }) declare id: string;
	@Column() declare label: string;
	@column.dateTime({ autoCreate: true }) declare createdAt: unknown;
}

/** Records what was bound, and echoes the bound row back as RETURNING would. */
function recordingDb() {
	const inserts: unknown[][] = [];
	return {
		inserts,
		db: wrapPrepareMock({
			prepare: (sql: string) => ({
				run: () => ({ changes: 1 }),
				all: (...params: unknown[]) => {
					if (/insert/i.test(sql)) inserts.push(params);
					return [];
				},
			}),
		}),
	};
}

describe("atlas > createMany applies what create applies", () => {
	it("generates a uuid primary key for every row", async () => {
		const { inserts, db } = recordingDb();
		const repo = new BaseRepository(Holding, db, { dialect: "postgres" });

		await repo.createMany([{ label: "one" }, { label: "two" }]);

		const bound = inserts.flat().filter((v) => typeof v === "string");
		const uuids = bound.filter((v) =>
			/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
				String(v),
			),
		);
		// One per row, and two different ones — not one reused.
		expect(uuids).toHaveLength(2);
		expect(new Set(uuids).size).toBe(2);
	});

	it("binds a real value in the id position, not an empty one", async () => {
		const { inserts, db } = recordingDb();
		const repo = new BaseRepository(Holding, db, { dialect: "postgres" });

		await repo.createMany([{ label: "one" }]);

		// The id column IS in the statement — `INSERT INTO "holdings" ("id",
		// "label", "created_at")` — so an ungenerated key does not fall back to
		// a database default: it arrives empty and the not-null constraint
		// rejects the whole batch.
		const [id] = inserts[0] ?? [];
		expect(typeof id).toBe("string");
		expect(id).not.toBe("");
	});

	it("returns each column once", async () => {
		const seen: string[] = [];
		const db = wrapPrepareMock({
			prepare: (sql: string) => {
				seen.push(sql);
				return { run: () => ({ changes: 1 }), all: () => [] };
			},
		});

		await new BaseRepository(Holding, db, { dialect: "postgres" }).createMany([
			{ label: "one" },
		]);

		const insert = seen.find((s) => /^INSERT/i.test(s)) ?? "";
		const returning = insert.slice(insert.indexOf("RETURNING"));
		// The primary key is already among the columns; listing it first as well
		// returned it twice, which is ambiguous for anything mapping by name.
		expect(returning.match(/"id"/g)).toHaveLength(1);
	});

	it("stamps the autoCreate timestamp, as the single-row path does", async () => {
		const { inserts, db } = recordingDb();
		const repo = new BaseRepository(Holding, db, { dialect: "postgres" });

		await repo.createMany([{ label: "one" }]);

		const bound = inserts.flat().map(String);
		expect(bound.some((v) => /^\d{4}-\d{2}-\d{2}T/.test(v))).toBe(true);
	});

	it("leaves a caller-supplied primary key alone", async () => {
		const { inserts, db } = recordingDb();
		const repo = new BaseRepository(Holding, db, { dialect: "postgres" });

		await repo.createMany([{ id: "chosen-by-the-caller", label: "one" }]);

		expect(inserts.flat()).toContain("chosen-by-the-caller");
	});
});
