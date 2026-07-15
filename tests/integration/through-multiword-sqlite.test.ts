import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	Column,
	HasManyThrough,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// The THROUGH model's PK property is multi-word (branchId → branch_id). The eager
// through-loader indexes the through rows by that key, so it must use the DB
// column name, not the raw property.
class Branch extends BaseModel {
	static override table = "branches";
	@PrimaryKey() declare branchId: string;
	@Column() declare regionId: string;
}
class Shop extends BaseModel {
	static override table = "shops";
	@PrimaryKey() declare id: string;
	@Column() declare branchId: string;
}
class Region extends BaseModel {
	static override table = "regions";
	@PrimaryKey() declare id: string;
	@HasManyThrough(
		() => Shop,
		() => Branch,
		{
			firstKey: "region_id",
			secondKey: "branch_id",
		},
	)
	declare shops: Shop[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE regions (id TEXT PRIMARY KEY)");
	await conn.execute(
		"CREATE TABLE branches (branch_id TEXT PRIMARY KEY, region_id TEXT)",
	);
	await conn.execute(
		"CREATE TABLE shops (id TEXT PRIMARY KEY, branch_id TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > HasManyThrough eager loader with a multi-word through PK", () => {
	it("indexes through rows by the DB column (branch_id), not the property", async () => {
		await Region.create({ id: "r1" });
		await Branch.create({ branchId: "b1", regionId: "r1" });
		await Shop.create({ id: "s1", branchId: "b1" });
		await Shop.create({ id: "s2", branchId: "b1" });

		const [region] = await Region.query().preload("shops").exec();
		// Before the fix `row['branchId']` was undefined → zero shops grouped.
		expect(region.shops.map((s) => s.id).sort()).toEqual(["s1", "s2"]);
	});
});
