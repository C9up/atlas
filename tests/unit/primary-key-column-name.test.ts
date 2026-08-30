/**
 * A primary key whose column is not its property name.
 *
 * `@PrimaryKey({ columnName: 'instrument_id' })` on a property called
 * `instrumentId` is ordinary, and every write path has to honour it. The WHERE
 * clause is the one that was not: `save()` emitted `WHERE "instrumentId" = ?`,
 * a column that does not exist, so an UPDATE on any multi-word primary key
 * failed outright.
 *
 * It stayed invisible because a single-word key (`id`, `code`) has the same
 * spelling either way.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("instrument_prices")
class InstrumentPrice extends BaseEntity {
	@PrimaryKey({ columnName: "instrument_id" })
	declare instrumentId: string;

	@Column({ columnName: "last_price" })
	declare lastPrice: number;
}

interface Captured {
	sql: string;
	params: unknown[];
}

/** A db that records the statements it is handed. */
function capturingDb(row?: Record<string, unknown>) {
	const captured: Captured[] = [];
	return {
		captured,
		prepare(sql: string) {
			return {
				run: (...params: unknown[]) => {
					captured.push({ sql, params });
					return { changes: 1, lastInsertRowid: 1 };
				},
				get: (...params: unknown[]) => {
					captured.push({ sql, params });
					return row;
				},
				all: (...params: unknown[]) => {
					captured.push({ sql, params });
					return row ? [row] : [];
				},
			};
		},
	};
}

const persisted = () => {
	const db = capturingDb();
	const repo = new BaseRepository(InstrumentPrice, wrapPrepareMock(db));
	const entity = new InstrumentPrice();
	entity.setProp("instrumentId", "AAPL");
	entity.setProp("lastPrice", 100);
	entity.markAsPersisted();
	return { db, repo, entity };
};

describe("atlas > a primary key with its own column name", () => {
	it("puts the COLUMN in save()'s WHERE, not the property", async () => {
		const { db, repo, entity } = persisted();
		entity.setProp("lastPrice", 200);

		await repo.save(entity);

		const update = db.captured.find((c) => /update/i.test(c.sql));
		expect(update, "save() must issue an UPDATE").toBeDefined();
		expect(update?.sql).toContain("instrument_id");
		// `"instrumentId"` is not a column in this table; the database refuses it.
		expect(update?.sql).not.toContain("instrumentId");
	});

	it("does the same for delete()", async () => {
		const { db, repo, entity } = persisted();

		await repo.delete(entity);

		const statement = db.captured.at(-1);
		expect(statement?.sql).toContain("instrument_id");
		expect(statement?.sql).not.toContain("instrumentId");
	});

	it("and for updateById, which takes the key directly", async () => {
		const db = capturingDb();
		const repo = new BaseRepository(InstrumentPrice, wrapPrepareMock(db));

		await repo.updateById("AAPL", { lastPrice: 300 });

		const update = db.captured.find((c) => /update/i.test(c.sql));
		expect(update?.sql).toContain("instrument_id");
		expect(update?.sql).not.toContain("instrumentId");
	});

	it("and for restore, on a soft-deleted row", async () => {
		const { db, repo, entity } = persisted();

		await repo.forceDelete(entity);

		const statement = db.captured.at(-1);
		expect(statement?.sql).toContain("instrument_id");
		expect(statement?.sql).not.toContain("instrumentId");
	});

	it("keeps the key out of the SET, where it never belonged", async () => {
		const { db, repo, entity } = persisted();
		entity.setProp("lastPrice", 200);

		await repo.save(entity);

		const update = db.captured.find((c) => /update/i.test(c.sql));
		const set = update?.sql.split(/where/i)[0] ?? "";
		expect(set).toContain("last_price");
		expect(set).not.toContain("instrument_id");
	});

	it("binds the key's value, so the row matched is the right one", async () => {
		const { db, repo, entity } = persisted();
		entity.setProp("lastPrice", 200);

		await repo.save(entity);

		const update = db.captured.find((c) => /update/i.test(c.sql));
		expect(update?.params).toContain("AAPL");
	});
});
