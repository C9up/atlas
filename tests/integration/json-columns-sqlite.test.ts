import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { computeCastTypes } from "../../src/BaseRepository.js";
import { getColumnMetadata } from "../../src/decorators/entity.js";
import {
	BaseEntity,
	BaseRepository,
	Column,
	column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

/**
 * A JSON column round-trips the VALUE, not its text.
 *
 * Both ways of declaring one are exercised — `@Column.json()` and the
 * `type: 'jsonb'` it is shorthand for — because both have to reach the same
 * encoding. The array cases matter most: the binder refuses a top-level array
 * everywhere else, since the far more common cause is a mis-built `IN` list,
 * so a JSON column that could not encode its own value had no way down.
 */
@Entity("instruments")
class Instrument extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column.json() declare metadata: Record<string, unknown> | null;
	// The long form, which the decorator is shorthand for.
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

	it("exposes the sub-decorator on both spellings", () => {
		// `Column.date` worked at runtime but was unknown to TypeScript, so only
		// the lowercase alias type-checked. Both carry the three now.
		expect(typeof Column.json).toBe("function");
		expect(typeof column.json).toBe("function");
		expect(Column.json).toBe(column.json);
	});

	it("declares jsonb by default, and json when asked", () => {
		const columns = getColumnMetadata(Instrument);
		const typeOf = (property: string) =>
			columns.find((column) => column.propertyKey === property)?.type;

		expect(typeOf("metadata")).toBe("jsonb");
		expect(typeOf("tags")).toBe("json");
		// The type is what adds the Postgres cast, so the decorator has to set it.
		expect(computeCastTypes(Instrument).metadata).toBe("jsonb");
	});

	it("leaves a column with its own prepare alone", async () => {
		@Entity("encoded")
		class Encoded extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column.json({
				prepare: (value) => `custom:${JSON.stringify(value)}`,
			})
			declare payload: unknown;
		}

		await conn.execute(
			"CREATE TABLE encoded (id TEXT PRIMARY KEY, payload TEXT)",
		);
		const encodedRepo = new BaseRepository(Encoded, conn, {
			dialect: "sqlite",
		});
		const row = new Encoded();
		row.id = "one";
		row.payload = { a: 1 };
		await encodedRepo.save(row);

		const stored = await conn.query<{ payload: string }>(
			"SELECT payload FROM encoded WHERE id = ?",
			["one"],
		);
		expect(stored[0]?.payload).toBe('custom:{"a":1}');
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
