/**
 * Integration test — `@Column({ prepare, consume })` opt-in column wiring
 * (Adonis Lucid parity).
 *
 * Drives the full INSERT → SELECT → UPDATE → SELECT round-trip through a
 * sync in-memory recording mock that emulates the better-sqlite3 prepare /
 * run / get / all surface BaseRepository expects, using a stub adapter
 * shaped like `@c9up/atom/atlas`'s `decimalAtlasAdapter` — atlas itself has
 * no atom dependency.
 *
 * The 18-digit value (`'1234567890123456.789'`) is the precision-pressure
 * point: passing through `Number()` would silently truncate to
 * `1234567890123457000`. The adapter contract preserves the exact string.
 *
 * @implements Story 35.10
 */
import "reflect-metadata";
import { beforeEach, describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
	setAtlasDialect,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

/** Minimal stand-in for `@c9up/atom`'s Decimal — string-backed, lossless. */
class FakeDecimal {
	constructor(public readonly value: string) {}
	toString(): string {
		return this.value;
	}
}

const fakeDecimalAdapter = {
	consume: (raw: unknown): FakeDecimal | null => {
		if (raw === null || raw === undefined) return null;
		if (raw instanceof FakeDecimal) return raw;
		return new FakeDecimal(String(raw));
	},
	prepare: (value: unknown): string | null => {
		if (value === null || value === undefined) return null;
		if (!(value instanceof FakeDecimal)) {
			throw new TypeError(`fakeDecimalAdapter.prepare: expected FakeDecimal`);
		}
		return value.toString();
	},
};

@Entity("accounts")
class Account extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column(fakeDecimalAdapter) declare balance: FakeDecimal | null;
	@Column() declare label: string;
}

/**
 * Sync recording mock — emulates just enough of the better-sqlite3
 * prepare/run/get/all surface for BaseRepository's hot paths. Stores rows
 * keyed by primary key so subsequent SELECTs see what was INSERTed.
 */
function syncSqliteMock() {
	type Row = Record<string, unknown>;
	const tables = new Map<string, Map<unknown, Row>>();
	const captured: { sql: string; params: unknown[] }[] = [];

	const tableOf = (sql: string): string | null => {
		const m = sql.match(/(?:INTO|FROM|UPDATE)\s+"(\w+)"/i);
		return m ? m[1] : null;
	};
	const whereCol = (sql: string): string | null => {
		const m = sql.match(/WHERE\s+"(\w+)"/i);
		return m ? m[1] : null;
	};

	return {
		captured,
		tables,
		prepare(sql: string) {
			return {
				run: (...params: unknown[]) => {
					captured.push({ sql, params });
					const table = tableOf(sql);
					if (!table) return { changes: 0, lastInsertRowid: 0 };
					if (!tables.has(table)) tables.set(table, new Map());
					const t = tables.get(table) as Map<unknown, Row>;

					if (/^\s*INSERT/i.test(sql)) {
						const colMatch = sql.match(/\(([^)]+)\)\s*VALUES/i);
						if (!colMatch) return { changes: 0, lastInsertRowid: 0 };
						const cols = colMatch[1]
							.split(",")
							.map((c) => c.trim().replace(/"/g, ""));
						const row: Row = {};
						cols.forEach((c, i) => {
							row[c] = params[i];
						});
						const id = row.id;
						t.set(id, row);
						return {
							changes: 1,
							lastInsertRowid: typeof id === "number" ? id : 1,
						};
					}
					if (/^\s*UPDATE/i.test(sql)) {
						const setMatch = sql.match(/SET\s+(.+?)\s+WHERE/i);
						if (!setMatch) return { changes: 0, lastInsertRowid: 0 };
						const setCols = setMatch[1].split(",").map((s) =>
							s
								.trim()
								.split(/\s*=\s*/)[0]
								.replace(/"/g, ""),
						);
						const whereVal = params[params.length - 1];
						const row = t.get(whereVal);
						if (row) {
							setCols.forEach((c, i) => {
								row[c] = params[i];
							});
							return { changes: 1, lastInsertRowid: 0 };
						}
					}
					if (/^\s*DELETE/i.test(sql)) {
						t.delete(params[0]);
						return { changes: 1, lastInsertRowid: 0 };
					}
					return { changes: 0, lastInsertRowid: 0 };
				},
				get: (...params: unknown[]) => {
					captured.push({ sql, params });
					const table = tableOf(sql);
					if (!table) return undefined;
					const t = tables.get(table);
					if (!t) return undefined;
					const wcol = whereCol(sql);
					if (!wcol) return [...t.values()][0];
					for (const row of t.values()) {
						if (row[wcol] === params[0]) return row;
					}
					return undefined;
				},
				all: (...params: unknown[]) => {
					captured.push({ sql, params });
					const table = tableOf(sql);
					if (!table) return [];
					if (!tables.has(table)) tables.set(table, new Map());
					const t = tables.get(table) as Map<unknown, Row>;
					// sqlite/postgres call `.all()` for `INSERT ... RETURNING`. Mirror
					// the side-effect of `.run()` (insert into the table) and surface
					// the freshly-written row so the repository hydrates auto-id /
					// default columns.
					if (/^\s*INSERT/i.test(sql)) {
						const colMatch = sql.match(/\(([^)]+)\)\s*VALUES/i);
						if (!colMatch) return [];
						const cols = colMatch[1]
							.split(",")
							.map((c) => c.trim().replace(/"/g, ""));
						const row: Row = {};
						cols.forEach((c, i) => {
							row[c] = params[i];
						});
						const id = row.id ?? t.size + 1;
						t.set(id, row);
						return [row];
					}
					const wcol = whereCol(sql);
					if (!wcol) return [...t.values()];
					return [...t.values()].filter((r) => r[wcol] === params[0]);
				},
			};
		},
	};
}

describe("atlas > integration > Decimal column round-trip", () => {
	let db: ReturnType<typeof syncSqliteMock>;

	beforeEach(() => {
		setAtlasDialect("sqlite");
		db = syncSqliteMock();
	});

	it("preserves an 18-digit decimal across INSERT → SELECT", async () => {
		const repo = new BaseRepository(Account, wrapPrepareMock(db));
		await repo.create({
			id: 1,
			balance: new FakeDecimal("1234567890123456.789"),
			label: "big",
		});

		const insert = db.captured.find((c) => /^\s*INSERT/i.test(c.sql));
		expect(insert?.params).toContain("1234567890123456.789");

		const found = await repo.find(1);
		expect(found).not.toBeNull();
		expect(found?.balance).toBeInstanceOf(FakeDecimal);
		expect(found?.balance?.toString()).toBe("1234567890123456.789");
		expect(found?.label).toBe("big");
	});

	it("null balance is delegated to the consume callback", async () => {
		const repo = new BaseRepository(Account, wrapPrepareMock(db));
		await repo.create({ id: 2, balance: null, label: "empty" });

		const found = await repo.find(2);
		expect(found?.balance).toBeNull();
		expect(found?.label).toBe("empty");
	});

	it("UPDATE binds the lossless string", async () => {
		const repo = new BaseRepository(Account, wrapPrepareMock(db));
		await repo.create({
			id: 3,
			balance: new FakeDecimal("1.0"),
			label: "starting",
		});

		const entity = await repo.find(3);
		if (!entity) throw new Error("precondition: row must exist");
		entity.balance = new FakeDecimal("999999999999999.99");
		await repo.save(entity);

		const update = db.captured.find((c) => /^\s*UPDATE/i.test(c.sql));
		expect(update?.params).toContain("999999999999999.99");

		const stored = db.tables.get("accounts")?.get(3);
		expect(stored?.balance).toBe("999999999999999.99");

		const refreshed = await repo.find(3);
		expect(refreshed?.balance).toBeInstanceOf(FakeDecimal);
		expect(refreshed?.balance?.toString()).toBe("999999999999999.99");
	});

	// 35-10-1-A4 — createMany binds the adapter `prepare` on every row, and
	// a subsequent SELECT round-trips back through `consume`. The "post-
	// insert" hydration that `create()` performs does NOT invoke consume on
	// RETURNING values — that's the documented contract — so the decode step
	// is observed via find().
	it("createMany binds adapter values and a subsequent find() consumes them", async () => {
		const repo = new BaseRepository(Account, wrapPrepareMock(db));
		await repo.createMany([
			{ id: 10, balance: new FakeDecimal("1.10"), label: "one" },
		]);
		const insert = db.captured.find((c) => /^\s*INSERT/i.test(c.sql));
		expect(insert?.params).toContain("1.10");
		const found = await repo.find(10);
		expect(found?.balance).toBeInstanceOf(FakeDecimal);
		expect(found?.balance?.toString()).toBe("1.10");
	});

	// 35-10-1-A5 — auto-increment PK + adapter encode path.
	// Driven by omitting the PK on insert: the local mock surfaces a
	// synthesised id via `lastInsertRowid` AND folds it back into the row
	// RETURNING payload so the repository hydrates the new id onto the
	// entity, while the adapter-encoded balance round-trips through
	// prepare → bind → consume.
	it("auto-increments the PK while the adapter encodes the balance", async () => {
		setAtlasDialect("sqlite");
		let nextId = 100;
		const captured: { sql: string; params: unknown[] }[] = [];
		const tracker = (sql: string, params: unknown[]) => {
			captured.push({ sql, params });
			if (!/^\s*INSERT/i.test(sql)) return null;
			const colMatch = sql.match(/\(([^)]+)\)\s*VALUES/i);
			if (!colMatch) return null;
			const cols = colMatch[1]
				.split(",")
				.map((c) => c.trim().replace(/"/g, ""));
			const row: Record<string, unknown> = {};
			cols.forEach((c, i) => {
				row[c] = params[i];
			});
			const id = nextId++;
			row.id = id;
			return { row, id };
		};
		const localDb = {
			prepare(sql: string) {
				return {
					run: (...params: unknown[]) => {
						const result = tracker(sql, params);
						return {
							changes: 1,
							lastInsertRowid: result ? (result.id as number) : 1,
						};
					},
					all: (...params: unknown[]) => {
						const result = tracker(sql, params);
						return result ? [result.row] : [];
					},
					get: () => undefined,
				};
			},
		};
		const repo = new BaseRepository(Account, wrapPrepareMock(localDb));
		const account = await repo.create({
			balance: new FakeDecimal("42.50"),
			label: "auto-id",
		});
		expect(typeof account.id).toBe("number");
		expect(account.id).toBe(100);
		// After RETURNING-hydration the balance is consumed back into a FakeDecimal,
		// consistent with the find()/all() hydration paths (post-insert hydration now
		// runs the same consume as a fresh read). The adapter `prepare` still ran on
		// the INSERT bind, observed via the captured params below.
		expect(account.balance).toBeInstanceOf(FakeDecimal);
		if (account.balance instanceof FakeDecimal) {
			expect(account.balance.value).toBe("42.50");
		}
		const insert = captured.find((c) => /^\s*INSERT/i.test(c.sql));
		expect(insert?.params).toContain("42.50");
	});
});
