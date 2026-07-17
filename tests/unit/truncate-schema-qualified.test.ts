/**
 * Proactive fallow-surfaced fix (sibling of the whereHas schema-qualified bug):
 * BaseRepository.truncate() quoted the whole table name as ONE identifier, so a
 * schema-qualified model (`reporting.events`) produced `TRUNCATE "reporting.events"`
 * — a table literally named with a dot — instead of `"reporting"."events"`.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import type { DatabaseConnection } from "../../src/BaseRepository.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { BaseModel, PrimaryKey } from "../../src/index.js";

class SchemaEvent extends BaseModel {
	static override table = "reporting.events";
	@PrimaryKey() declare id: string;
}

function recordingPg(): { conn: DatabaseConnection; sql: string[] } {
	const sql: string[] = [];
	const conn: DatabaseConnection = {
		async execute(q: string) {
			sql.push(q);
			return { rowsAffected: 0 };
		},
		async query<T>(q: string): Promise<T[]> {
			sql.push(q);
			return [];
		},
	};
	return { conn, sql };
}

describe("atlas > truncate() quotes a schema-qualified table per-segment", () => {
	it('emits "reporting"."events", not "reporting.events"', async () => {
		const { conn, sql } = recordingPg();
		const repo = new BaseRepository(SchemaEvent, conn, { dialect: "postgres" });

		await repo.truncate();

		const truncate = sql.find((s) => s.startsWith("TRUNCATE"));
		expect(truncate).toBeDefined();
		expect(truncate).toContain('"reporting"."events"');
		expect(truncate).not.toContain('"reporting.events"');
	});

	it("rejects an unsafe table identifier instead of emitting malformed SQL", async () => {
		class EvilTable extends BaseModel {
			static override table = 'x"; DROP TABLE users; --';
			@PrimaryKey() declare id: string;
		}
		const { conn } = recordingPg();
		const repo = new BaseRepository(EvilTable, conn, { dialect: "postgres" });
		await expect(repo.truncate()).rejects.toThrow(/Unsafe table identifier/i);
	});
});
