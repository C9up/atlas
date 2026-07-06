/**
 * Real-SQLite coverage for the m2m pivot projection + wherePivot filtering.
 *
 * Exercises the load-side that unit mocks can't reach: `pivotColumns` projected
 * into `$extras.pivot_<col>` (running each column's `consume` adapter) and
 * `wherePivot` / `wherePivotIn` constraining the pivot lookup.
 */
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
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";

class Role {
	constructor(readonly raw: string) {}
}

const roleAdapter = {
	consume: (v: unknown): Role | null =>
		v === null || v === undefined ? null : new Role(String(v)),
};

@Entity("piv_roles")
class PRole extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare label: string;
}

@Entity("piv_users")
class PUser extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@ManyToMany(() => PRole, {
		pivotTable: "piv_user_role",
		foreignKey: "user_id",
		otherKey: "role_id",
		pivotColumns: ["scope", "grant"],
		pivotColumnAdapters: { grant: roleAdapter },
	})
	declare roles: PRole[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE piv_users (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE piv_roles (id TEXT PRIMARY KEY, label TEXT)",
	);
	await conn.execute(
		'CREATE TABLE piv_user_role (user_id TEXT, role_id TEXT, scope TEXT, "grant" TEXT)',
	);
	await conn.execute(
		"INSERT INTO piv_users VALUES ('u1', 'Ada'), ('u2', 'Linus')",
	);
	await conn.execute(
		"INSERT INTO piv_roles VALUES ('r1', 'admin'), ('r2', 'editor')",
	);
	await conn.execute(
		"INSERT INTO piv_user_role VALUES ('u1', 'r1', 'global', 'full'), ('u1', 'r2', 'team', 'partial'), ('u2', 'r2', 'global', 'full')",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas > m2m pivot projection (real SQLite)", () => {
	it("projects declared pivotColumns into $extras.pivot_<col>", async () => {
		const users = await new BaseRepository(PUser, conn)
			.query()
			.preload("roles")
			.exec();
		const ada = users.find((u) => u.id === "u1");
		const admin = ada?.roles.find((r) => r.id === "r1");
		expect(admin?.getExtra("pivot_scope")).toBe("global");
	});

	it("runs the pivotColumnAdapters.consume on the projected value", async () => {
		const users = await new BaseRepository(PUser, conn)
			.query()
			.preload("roles")
			.exec();
		const ada = users.find((u) => u.id === "u1");
		const admin = ada?.roles.find((r) => r.id === "r1");
		const grant = admin?.getExtra<Role>("pivot_grant");
		expect(grant).toBeInstanceOf(Role);
		expect(grant?.raw).toBe("full");
	});

	it("gives each parent-edge its own instance (no pivot clobber)", async () => {
		// r2 (editor) is attached to u1 with scope='team' and to u2 with scope='global'.
		const users = await new BaseRepository(PUser, conn)
			.query()
			.preload("roles")
			.exec();
		const ada = users.find((u) => u.id === "u1");
		const linus = users.find((u) => u.id === "u2");
		const adaEditor = ada?.roles.find((r) => r.id === "r2");
		const linusEditor = linus?.roles.find((r) => r.id === "r2");
		expect(adaEditor?.getExtra("pivot_scope")).toBe("team");
		expect(linusEditor?.getExtra("pivot_scope")).toBe("global");
	});

	it("wherePivot filters the pivot lookup", async () => {
		const users = await new BaseRepository(PUser, conn)
			.query()
			.preload("roles", (q) => q.wherePivot("scope", "global"))
			.exec();
		const ada = users.find((u) => u.id === "u1");
		// Only the global-scoped edge (r1) survives for u1.
		expect(ada?.roles.map((r) => r.id)).toEqual(["r1"]);
	});

	it("wherePivotIn filters the pivot lookup by a set", async () => {
		const users = await new BaseRepository(PUser, conn)
			.query()
			.preload("roles", (q) => q.wherePivotIn("scope", ["team"]))
			.exec();
		const ada = users.find((u) => u.id === "u1");
		expect(ada?.roles.map((r) => r.id)).toEqual(["r2"]);
	});
});
