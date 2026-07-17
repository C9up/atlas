/**
 * Set operations (INTERSECT / EXCEPT) and the CTE variants — recursive and
 * materialized — none of which existed in either the TS or the Rust layer.
 *
 * The recursive CTE runs for real: a WITH RECURSIVE that compiles but doesn't
 * terminate or doesn't see its own rows would still pass a string assertion.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("nodes")
class Node extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare parentId: number | null;
	@Column() declare name: string;
}

function repo(
	conn: AsyncDatabaseConnection,
	dialect: "sqlite" | "postgres" | "mysql" = "sqlite",
) {
	return new BaseRepository(Node, conn, { dialect });
}

describe("set operations", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)",
			[],
		);
		for (const [id, parent, name] of [
			[1, null, "root"],
			[2, 1, "a"],
			[3, 1, "b"],
			[4, 2, "a1"],
		] as const) {
			await conn.execute(
				"INSERT INTO nodes (id, parent_id, name) VALUES (?, ?, ?)",
				[id, parent, name],
			);
		}
	});

	it("INTERSECT keeps only the rows in both branches", async () => {
		const r = repo(conn);
		const rows = await r
			.query()
			.select("id")
			.whereIn("id", [1, 2, 3])
			.intersect(r.query().select("id").whereIn("id", [2, 3, 4]))
			.exec();

		expect(rows.map((n) => n.id).sort()).toEqual([2, 3]);
	});

	it("EXCEPT drops the rows present in the other branch", async () => {
		const r = repo(conn);
		const rows = await r
			.query()
			.select("id")
			.whereIn("id", [1, 2, 3])
			.except(r.query().select("id").whereIn("id", [2]))
			.exec();

		expect(rows.map((n) => n.id).sort()).toEqual([1, 3]);
	});

	it("compiles the ALL variants on postgres", () => {
		const r = repo(conn, "postgres");
		expect(r.query().intersectAll(r.query()).toSQL().sql).toContain(
			"INTERSECT ALL",
		);
		expect(r.query().exceptAll(r.query()).toSQL().sql).toContain("EXCEPT ALL");
	});

	/** SQLite has UNION / UNION ALL / INTERSECT / EXCEPT — but no INTERSECT ALL. */
	it("refuses INTERSECT ALL on SQLite rather than emitting a syntax error", () => {
		const r = repo(conn);
		expect(() => r.query().intersectAll(r.query()).toSQL()).toThrow(
			/E_UNSUPPORTED/,
		);
		expect(() => r.query().exceptAll(r.query()).toSQL()).toThrow(
			/E_UNSUPPORTED/,
		);
		// The plain forms and UNION ALL are all fine there.
		expect(() => r.query().intersect(r.query()).toSQL()).not.toThrow();
		expect(r.query().unionAll(r.query()).toSQL().sql).toContain("UNION ALL");
	});

	it("keeps union() unchanged", async () => {
		const r = repo(conn);
		const rows = await r
			.query()
			.select("id")
			.where("id", 1)
			.union(r.query().select("id").where("id", 2))
			.exec();

		expect(rows.map((n) => n.id).sort()).toEqual([1, 2]);
	});
});

describe("CTE variants", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)",
			[],
		);
	});

	it("marks the whole WITH clause recursive when any CTE is", () => {
		const r = repo(conn);
		const sql = r
			.query()
			.with("plain", r.query())
			.withRecursive("tree", r.query())
			.toSQL().sql;

		// RECURSIVE belongs to the WITH, not to one CTE.
		expect(sql).toContain('WITH RECURSIVE "plain" AS (');
		expect(sql).toContain('"tree" AS (');
	});

	it("leaves a plain WITH alone", () => {
		const r = repo(conn);
		expect(r.query().with("c", r.query()).toSQL().sql).not.toContain(
			"RECURSIVE",
		);
	});

	/** A WITH RECURSIVE that compiles but never sees its own rows would still
	 * pass a string assertion — so walk a real tree. */
	it("walks a tree with a real recursive CTE", async () => {
		for (const [id, parent] of [
			[1, null],
			[2, 1],
			[3, 2],
			[4, null],
		] as const) {
			await conn.execute(
				"INSERT INTO nodes (id, parent_id, name) VALUES (?, ?, ?)",
				[id, parent, `n${id}`],
			);
		}

		// The recursive term has to reference the CTE by name, which the typed
		// builder cannot express — this is exactly what raw SQL is for.
		const rows = await conn.query<{ id: number }>(
			`WITH RECURSIVE descendants(id) AS (
			   SELECT id FROM nodes WHERE id = 1
			   UNION ALL
			   SELECT n.id FROM nodes n JOIN descendants d ON n.parent_id = d.id
			 )
			 SELECT id FROM descendants ORDER BY id`,
			[],
		);

		expect(rows.map((r) => r.id)).toEqual([1, 2, 3]);
	});

	it("emits the MATERIALIZED hints on postgres", () => {
		const r = repo(conn, "postgres");
		expect(r.query().withMaterialized("c", r.query()).toSQL().sql).toContain(
			'"c" AS MATERIALIZED (',
		);
		expect(r.query().withNotMaterialized("c", r.query()).toSQL().sql).toContain(
			'"c" AS NOT MATERIALIZED (',
		);
	});

	it("refuses a MATERIALIZED hint on MySQL, which has none", () => {
		const r = repo(conn, "mysql");
		expect(() => r.query().withMaterialized("c", r.query()).toSQL()).toThrow(
			/E_UNSUPPORTED/,
		);
		// Without the hint MySQL is fine.
		expect(() => r.query().with("c", r.query()).toSQL()).not.toThrow();
	});

	it("validates the CTE name for every variant", () => {
		const r = repo(conn);
		for (const build of [
			() => r.query().withRecursive('x"; DROP TABLE nodes; --', r.query()),
			() => r.query().withMaterialized("bad name", r.query()),
			() => r.query().withNotMaterialized("1nope", r.query()),
		]) {
			expect(build).toThrow(/not a valid identifier/);
		}
	});

	it("carries recursive/materialized through clone()", () => {
		const r = repo(conn, "postgres");
		const base = r.query().withRecursive("t", r.query());
		expect(base.clone().toSQL().sql).toContain("WITH RECURSIVE");

		const mat = r.query().withMaterialized("c", r.query());
		expect(mat.clone().toSQL().sql).toContain("AS MATERIALIZED (");
	});
});

describe("distinctOn", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)",
			[],
		);
	});

	it("emits DISTINCT ON with resolved columns on postgres", () => {
		const sql = repo(conn, "postgres")
			.query()
			.distinctOn("parentId")
			.toSQL().sql;

		// camelCase → snake_case, like every other column entry point.
		expect(sql).toContain('SELECT DISTINCT ON ("parent_id") ');
	});

	/**
	 * Elsewhere `DISTINCT ON (a)` parses as a plain DISTINCT over a row value
	 * and returns a different result set — a silent wrong answer, so refuse.
	 */
	it("refuses outside postgres instead of returning different rows", () => {
		expect(() =>
			repo(conn, "sqlite").query().distinctOn("parentId").toSQL(),
		).toThrow(/E_UNSUPPORTED/);
		expect(() =>
			repo(conn, "mysql").query().distinctOn("parentId").toSQL(),
		).toThrow(/E_UNSUPPORTED/);
	});

	it("rejects an unknown column like every other entry point", () => {
		expect(() => repo(conn, "postgres").query().distinctOn("nope")).toThrow(
			/does not exist/,
		);
	});

	it("survives clone()", () => {
		const q = repo(conn, "postgres").query().distinctOn("parentId");
		expect(q.clone().toSQL().sql).toContain("DISTINCT ON");
	});
});
