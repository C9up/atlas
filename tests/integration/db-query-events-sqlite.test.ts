/**
 * `db:query` observability, and the `.debug()` that never worked: the flag was
 * stored on ModelQuery and copied into clones, but never read by anything — so
 * calling it produced no output and no error.
 *
 * The first test here is the one that would have caught that: it asserts an
 * event actually arrives.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import {
	clearDbQueryListeners,
	type DbQueryEvent,
	onDbQuery,
	prettyPrintQuery,
} from "../../src/events.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService } from "../../src/services/db.js";

@Entity("widgets")
class Widget extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

async function connect(debug: boolean): Promise<AsyncDatabaseConnection> {
	const conn = await createNapiConnection(
		"sqlite::memory:",
		1,
		1,
		undefined,
		undefined,
		{ debug, connectionName: "primary" },
	);
	await conn.execute(
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)",
		[],
	);
	await conn.execute("INSERT INTO widgets (id, name) VALUES (1, 'bolt')", []);
	return conn;
}

describe("db:query events", () => {
	let events: DbQueryEvent[];

	beforeEach(() => {
		setAtlasDialect("sqlite");
		events = [];
	});

	afterEach(() => {
		clearDbQueryListeners();
	});

	it("emits every statement when the connection has debug on", async () => {
		const conn = await connect(true);
		onDbQuery((e) => events.push(e));

		await new BaseRepository(Widget, conn).query().exec();

		expect(events).toHaveLength(1);
		const [event] = events;
		expect(event?.sql).toContain("SELECT");
		expect(event?.connection).toBe("primary");
		expect(event?.model).toBe("Widget");
		expect(event?.method).toBe("exec");
		expect(event?.duration).toBeGreaterThanOrEqual(0);
	});

	it("stays silent when debug is off", async () => {
		const conn = await connect(false);
		onDbQuery((e) => events.push(e));

		await new BaseRepository(Widget, conn).query().exec();

		expect(events).toEqual([]);
	});

	/** The regression: `.debug()` stored a flag nothing ever read. */
	it("emits for a single query when .debug() asks, with debug off globally", async () => {
		const conn = await connect(false);
		onDbQuery((e) => events.push(e));
		const repo = new BaseRepository(Widget, conn);

		await repo.query().exec();
		expect(events).toEqual([]);

		await repo.query().debug().exec();
		expect(events).toHaveLength(1);
		expect(events[0]?.method).toBe("exec");
	});

	it("reporterData() attaches metadata to the event + forces emission (Lucid)", async () => {
		// debug is OFF globally — reporterData must still make the event fire.
		const conn = await connect(false);
		onDbQuery((e) => events.push(e));
		const repo = new BaseRepository(Widget, conn);

		await repo.query().reporterData({ userId: 42, source: "feed" }).exec();

		expect(events).toHaveLength(1);
		expect(events[0]?.reporterData).toEqual({ userId: 42, source: "feed" });
	});

	it("db builder reporterData() reaches the event (Lucid)", async () => {
		const conn = await connect(false);
		onDbQuery((e) => events.push(e));
		const db = createDbService(() => conn);

		await db.from("widgets").reporterData({ requestId: "abc" }).exec();

		expect(events).toHaveLength(1);
		expect(events[0]?.reporterData).toEqual({ requestId: "abc" });
		expect(events[0]?.method).toBe("exec");
	});

	it("reports bindings without interpolating them into the SQL", async () => {
		const conn = await connect(true);
		onDbQuery((e) => events.push(e));

		await new BaseRepository(Widget, conn).query().where("name", "bolt").exec();

		const [event] = events;
		expect(event?.bindings).toEqual(["bolt"]);
		expect(event?.sql).not.toContain("bolt");
	});

	it("emits a failing statement too, carrying the error", async () => {
		const conn = await connect(true);
		onDbQuery((e) => events.push(e));

		await expect(conn.query("SELECT * FROM nope", [])).rejects.toThrow();

		expect(events).toHaveLength(1);
		expect(events[0]?.error).toBeInstanceOf(Error);
	});

	it("does not let a throwing listener break the query", async () => {
		const conn = await connect(true);
		onDbQuery(() => {
			throw new Error("listener is broken");
		});
		onDbQuery((e) => events.push(e));

		const rows = await new BaseRepository(Widget, conn).query().exec();

		expect(rows).toHaveLength(1);
		// The healthy listener still ran.
		expect(events).toHaveLength(1);
	});

	it("stops emitting once unsubscribed", async () => {
		const conn = await connect(true);
		const off = onDbQuery((e) => events.push(e));
		const repo = new BaseRepository(Widget, conn);

		await repo.query().exec();
		off();
		await repo.query().exec();

		expect(events).toHaveLength(1);
	});
});

describe("prettyPrintQuery", () => {
	it("appends bindings as JSON rather than interpolating them", () => {
		const line = prettyPrintQuery({
			sql: "SELECT * FROM widgets WHERE name = ?",
			bindings: ["bolt"],
			duration: 1.234,
			connection: "primary",
			model: "Widget",
			method: "exec",
		});

		expect(line).toContain("1.23ms");
		expect(line).toContain("primary Widget exec");
		expect(line).toContain("SELECT * FROM widgets WHERE name = ?");
		// The bindings are reported beside the SQL, never spliced into it — an
		// interpolated line reads like runnable SQL without the escaping that
		// made the real statement safe.
		expect(line).toContain(`-- ["bolt"]`);
	});

	it("survives a binding JSON cannot serialise", () => {
		const circular: Record<string, unknown> = {};
		circular.self = circular;

		expect(
			prettyPrintQuery({
				sql: "SELECT 1",
				bindings: [circular],
				duration: 0,
			}),
		).toContain("[unserialisable bindings]");
	});

	it("renders a bigint binding instead of throwing", () => {
		expect(
			prettyPrintQuery({
				sql: "SELECT 1",
				bindings: [123n],
				duration: 0,
			}),
		).toContain("123n");
	});
});
