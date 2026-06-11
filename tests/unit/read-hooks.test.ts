import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	afterFetch,
	afterFind,
	afterPaginate,
	BaseEntity,
	BaseRepository,
	beforeFetch,
	beforeFind,
	beforePaginate,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import type { ModelQuery } from "../../src/ModelQuery.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

// Shared recorder — each hook pushes its name (and, for after-hooks, a tag of
// what it received) so the tests can assert exactly which hooks fired.
const log: string[] = [];

@Entity("widgets")
class Widget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@Column() declare status: string;

	@beforeFind()
	static bf(q: ModelQuery<Widget>): void {
		log.push("beforeFind");
		void q;
	}
	@afterFind()
	static af(row: Widget | null): void {
		log.push(`afterFind:${row === null ? "null" : "entity"}`);
	}
	@beforeFetch()
	static bfe(q: ModelQuery<Widget>): void {
		log.push("beforeFetch");
		void q;
	}
	@afterFetch()
	static afe(rows: Widget[]): void {
		log.push(`afterFetch:${rows.length}`);
	}
	@beforePaginate()
	static bp(q: ModelQuery<Widget>): void {
		log.push("beforePaginate");
		void q;
	}
	@afterPaginate()
	static ap(rows: Widget[]): void {
		log.push(`afterPaginate:${rows.length}`);
	}
}

// A second entity whose beforeFetch MUTATES the query — proves the hook can
// scope reads, not just observe them.
@Entity("scoped_widgets")
class ScopedWidget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare status: string;

	@beforeFetch()
	static scope(q: ModelQuery<ScopedWidget>): void {
		q.where("status", "active");
	}
}

/** Mock that records the SQL it's asked to run and returns canned rows. */
function makeDb(rows: Record<string, unknown>[]) {
	const seenSql: string[] = [];
	const db = wrapPrepareMock({
		prepare(sql: string) {
			seenSql.push(sql);
			return {
				run() {
					return { changes: 1, lastInsertRowid: 1 };
				},
				all() {
					if (/count\(\*\)/i.test(sql)) return [{ count: rows.length }];
					return rows;
				},
			};
		},
	});
	return { db, seenSql };
}

beforeEach(() => {
	log.length = 0;
});
afterEach(() => {
	log.length = 0;
});

describe("atlas > read hooks (Lucid parity)", () => {
	it("find() fires beforeFind + afterFind, not the fetch hooks", async () => {
		const { db } = makeDb([{ id: "1", name: "a", status: "x" }]);
		const repo = new BaseRepository(Widget, db);
		await repo.find("1");
		expect(log).toEqual(["beforeFind", "afterFind:entity"]);
	});

	it("find() miss passes null to afterFind", async () => {
		const { db } = makeDb([]);
		const repo = new BaseRepository(Widget, db);
		await repo.find("404");
		expect(log).toEqual(["beforeFind", "afterFind:null"]);
	});

	it("findBy() fires the find hooks", async () => {
		const { db } = makeDb([{ id: "1", name: "a", status: "x" }]);
		const repo = new BaseRepository(Widget, db);
		await repo.findBy("name", "a");
		expect(log).toEqual(["beforeFind", "afterFind:entity"]);
	});

	it("all() fires beforeFetch + afterFetch, not the find hooks", async () => {
		const { db } = makeDb([
			{ id: "1", name: "a", status: "x" },
			{ id: "2", name: "b", status: "y" },
		]);
		const repo = new BaseRepository(Widget, db);
		await repo.all();
		expect(log).toEqual(["beforeFetch", "afterFetch:2"]);
	});

	it("where() fires the fetch hooks", async () => {
		const { db } = makeDb([{ id: "1", name: "a", status: "x" }]);
		const repo = new BaseRepository(Widget, db);
		await repo.where("status", "x");
		expect(log).toEqual(["beforeFetch", "afterFetch:1"]);
	});

	it("query().first() fires only the find hooks", async () => {
		const { db } = makeDb([{ id: "1", name: "a", status: "x" }]);
		const repo = new BaseRepository(Widget, db);
		await repo.query().first();
		expect(log).toEqual(["beforeFind", "afterFind:entity"]);
	});

	it("awaiting query() (exec) fires only the fetch hooks", async () => {
		const { db } = makeDb([{ id: "1", name: "a", status: "x" }]);
		const repo = new BaseRepository(Widget, db);
		await repo.query();
		expect(log).toEqual(["beforeFetch", "afterFetch:1"]);
	});

	it("paginate() fires the paginate hooks, not the fetch hooks", async () => {
		const { db } = makeDb([
			{ id: "1", name: "a", status: "x" },
			{ id: "2", name: "b", status: "y" },
		]);
		const repo = new BaseRepository(Widget, db);
		await repo.query().paginate(1, 10);
		expect(log).toEqual(["beforePaginate", "afterPaginate:2"]);
	});

	it("a beforeFetch hook that mutates the query affects the SQL", async () => {
		const { db, seenSql } = makeDb([{ id: "1", status: "active" }]);
		const repo = new BaseRepository(ScopedWidget, db);
		await repo.all();
		// The hook added `where status = 'active'` — the compiled SELECT must carry it.
		const selects = seenSql.filter((s) => /^SELECT/i.test(s));
		expect(selects.some((s) => /status/i.test(s))).toBe(true);
	});
});
