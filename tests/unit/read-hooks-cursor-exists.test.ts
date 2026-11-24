/**
 * `cursorPaginate()` fires `beforeFetch` / `afterFetch`, `exists()` and
 * `doesntExist()` fire `beforeFind`.
 *
 * Both terminals ran without a hook: a tenant filter written in a
 * `@beforeFetch()` / `@beforeFind()` hook held for `exec()`, `first()` and
 * `paginate()`, and every tenant's rows came back through `cursorPaginate()`;
 * `where('id', id).exists()` answered `true` for a row `first()` did not return.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	afterFetch,
	BaseModel,
	beforeFetch,
	beforeFind,
	Column,
	PrimaryKey,
} from "../../src/index.js";
import type { ModelQuery } from "../../src/ModelQuery.js";
import { clearDb, setDb } from "../../src/services/db.js";

const log: string[] = [];

class RhInvoice extends BaseModel {
	static override table = "rh_invoices";
	@PrimaryKey() declare id: number;
	@Column() declare tenantId: number;
	@Column() declare label: string;

	@beforeFind()
	static mineOne(q: ModelQuery<RhInvoice>): void {
		log.push("beforeFind");
		q.where("tenantId", 1);
	}

	@beforeFetch()
	static mineMany(q: ModelQuery<RhInvoice>): void {
		log.push("beforeFetch");
		q.where("tenantId", 1);
	}

	@afterFetch()
	static fetched(rows: RhInvoice[]): void {
		log.push(`afterFetch:${rows.map((r) => r.id).join(",")}`);
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE rh_invoices (id INTEGER PRIMARY KEY, tenant_id INTEGER, label TEXT)",
	);
	await conn.execute(
		"INSERT INTO rh_invoices VALUES (1, 1, 'a'), (2, 2, 'other'), (3, 1, 'b'), (4, 2, 'other'), (5, 1, 'c')",
	);
	setDb(conn);
});

beforeEach(() => {
	log.length = 0;
});

afterAll(async () => {
	clearDb(conn);
	await conn.close();
});

describe("cursorPaginate()", () => {
	it("fires beforeFetch on every page, so the hook's filter holds", async () => {
		const first = await RhInvoice.query().cursorPaginate({
			limit: 2,
			orderBy: "id",
		});
		expect(first.items.map((i) => i.id)).toEqual([1, 3]);
		expect(first.hasMore).toBe(true);
		const next = await RhInvoice.query().cursorPaginate({
			limit: 2,
			orderBy: "id",
			cursor: first.nextCursor ?? undefined,
		});
		expect(next.items.map((i) => i.id)).toEqual([5]);
		expect(next.hasMore).toBe(false);
	});

	it("an orWhere of the caller does not escape the hook's filter", async () => {
		const page = await RhInvoice.query()
			.where("label", "other")
			.orWhere("id", 1)
			.cursorPaginate({ limit: 10, orderBy: "id" });
		expect(page.items.map((i) => i.id)).toEqual([1]);
	});

	it("fires afterFetch on the page's items, not on the look-ahead row", async () => {
		await RhInvoice.query().cursorPaginate({ limit: 2, orderBy: "id" });
		expect(log).toEqual(["beforeFetch", "afterFetch:1,3"]);
	});
});

describe("exists() / doesntExist()", () => {
	it("fire beforeFind, so they answer what first() answers", async () => {
		const other = (): ModelQuery<RhInvoice> => RhInvoice.query().where("id", 2);
		expect(await other().first()).toBeNull();
		expect(await other().exists()).toBe(false);
		expect(await other().doesntExist()).toBe(true);
		expect(await RhInvoice.query().where("id", 1).exists()).toBe(true);
		expect(log.filter((l) => l === "beforeFind")).toHaveLength(4);
		expect(log.some((l) => l.startsWith("afterFetch"))).toBe(false);
	});

	it("leave the query they were called on unchanged", async () => {
		const q = RhInvoice.query().where("id", 2);
		await q.exists();
		expect(q.toSQL().bindings).toEqual([2]);
	});
});
