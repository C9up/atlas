import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

/**
 * `@Column({ type: 'jsonb' })` round-trips the VALUE, not its text.
 *
 * Knex gets the write half from the driver — node-postgres serialises an object
 * on its own — but only for an object: hand it an array and it builds a
 * Postgres array literal, which is why Lucid applications write
 * `prepare: JSON.stringify` for a list. Atlas is stricter still: the binder
 * refuses a top-level array outright, because the far more common cause is a
 * mis-built `IN` list. So a declared JSON column has to encode its own value,
 * and these pin both shapes and both directions.
 */
@Entity("instruments")
class Instrument extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column({ type: "jsonb" }) declare metadata: Record<string, unknown> | null;
	@Column({ type: "json" }) declare tags: string[] | null;
	@Column() declare label: string;
}

let conn: AsyncDatabaseConnection;
let repo: BaseRepository<Instrument>;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE instruments (id TEXT PRIMARY KEY, metadata TEXT, tags TEXT, label TEXT)",
	);
	setDb(conn);
	repo = new BaseRepository(Instrument, conn, { dialect: "sqlite" });
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > JSON columns", () => {
	it("stores an object and reads it back as an object", async () => {
		const entity = new Instrument();
		entity.id = "obj";
		entity.label = "Object";
		entity.metadata = { isin: "CH0012032048", sector: { code: 42 } };
		entity.tags = null;
		await repo.save(entity);

		const found = await repo.find("obj");
		expect(found?.metadata).toEqual({
			isin: "CH0012032048",
			sector: { code: 42 },
		});
	});

	it("stores an array — the shape the binder refuses everywhere else", async () => {
		const entity = new Instrument();
		entity.id = "arr";
		entity.label = "Array";
		entity.metadata = null;
		entity.tags = ["XSWX", "XVTX"];
		await repo.save(entity);

		const found = await repo.find("arr");
		expect(found?.tags).toEqual(["XSWX", "XVTX"]);
	});

	it("keeps a null null, rather than storing the string 'null'", async () => {
		const entity = new Instrument();
		entity.id = "nil";
		entity.label = "Null";
		entity.metadata = null;
		entity.tags = null;
		await repo.save(entity);

		const rows = await conn.query<{ metadata: unknown }>(
			"SELECT metadata FROM instruments WHERE id = ?",
			["nil"],
		);
		expect(rows[0]?.metadata).toBeNull();
		expect((await repo.find("nil"))?.metadata).toBeNull();
	});

	it("writes JSON text, so the column is queryable as JSON", async () => {
		const rows = await conn.query<{ metadata: string }>(
			"SELECT json_extract(metadata, '$.isin') AS isin FROM instruments WHERE id = ?",
			["obj"],
		);
		expect(rows[0]).toEqual({ isin: "CH0012032048" });
	});

	it("passes a string through — it is already JSON text", async () => {
		const entity = new Instrument();
		entity.id = "str";
		entity.label = "String";
		entity.metadata = null;
		entity.tags = null;
		await repo.save(entity);
		await repo.updateWhere("id", "str", { metadata: '{"pre":"encoded"}' });

		expect((await repo.find("str"))?.metadata).toEqual({ pre: "encoded" });
	});

	it("hands back text it cannot parse instead of failing the whole row", async () => {
		// Only reachable on SQLite, where the column is TEXT and nothing enforces
		// the shape. Failing here would make `label` unreadable too.
		await conn.execute(
			"INSERT INTO instruments (id, metadata, tags, label) VALUES ('bad', 'not json', NULL, 'Legacy')",
		);

		const found = await repo.find("bad");
		expect(found?.metadata).toBe("not json");
		expect(found?.label).toBe("Legacy");
	});
});
