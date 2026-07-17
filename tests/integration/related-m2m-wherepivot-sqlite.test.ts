/**
 * `related('roles').query().wherePivot('active', true)` must filter on the pivot
 * column. The lazy m2m query is an EXISTS subquery; before the fix the EXISTS was
 * baked at proxy-return time (only the two FK correlations), so a `.wherePivot()`
 * chained afterwards was recorded but never emitted — a silent filter bypass.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, ManyToMany, PrimaryKey } from "../../src/index.js";
import { ModelQuery } from "../../src/ModelQuery.js";
import { clearDb, setDb } from "../../src/services/db.js";

// related().query() is typed as the agnostic proxy return (unknown). Narrow to
// the concrete ModelQuery so the pivot builder methods are callable in the test.
function pivotQuery(user: WpUser) {
	const q = user.related("roles").query();
	if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
	return q;
}

class WpRole extends BaseModel {
	static override table = "wp_roles";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

class WpUser extends BaseModel {
	static override table = "wp_users";
	@PrimaryKey() declare id: string;
	@ManyToMany(() => WpRole, {
		pivotTable: "wp_user_roles",
		foreignKey: "user_id",
		otherKey: "role_id",
	})
	declare roles: WpRole[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE wp_roles (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute("CREATE TABLE wp_users (id TEXT PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE wp_user_roles (user_id TEXT, role_id TEXT, active INTEGER)",
	);
	await conn.execute(
		"INSERT INTO wp_roles (id, name) VALUES ('r1','admin'),('r2','editor'),('r3','viewer')",
	);
	await conn.execute("INSERT INTO wp_users (id) VALUES ('u1')");
	// u1 → r1 (active), r2 (inactive), r3 (active)
	await conn.execute(
		"INSERT INTO wp_user_roles (user_id, role_id, active) VALUES " +
			"('u1','r1',1),('u1','r2',0),('u1','r3',1)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > related() m2m wherePivot filters the pivot (Lucid parity)", () => {
	it("applies wherePivot('active', 1) to the lazy EXISTS", async () => {
		const user = await WpUser.$repo().findOrFail("u1");
		const activeRoles = await pivotQuery(user)
			.wherePivot("active", 1)
			.orderBy("id")
			.exec();
		expect(activeRoles.map((r) => r.id)).toEqual(["r1", "r3"]);
	});

	it("without wherePivot returns all related rows", async () => {
		const user = await WpUser.$repo().findOrFail("u1");
		const all = await pivotQuery(user).orderBy("id").exec();
		expect(all.map((r) => r.id)).toEqual(["r1", "r2", "r3"]);
	});

	it("supports wherePivotIn", async () => {
		const user = await WpUser.$repo().findOrFail("u1");
		const rows = await pivotQuery(user).wherePivotIn("role_id", ["r2"]).exec();
		expect(rows.map((r) => r.id)).toEqual(["r2"]);
	});
});
