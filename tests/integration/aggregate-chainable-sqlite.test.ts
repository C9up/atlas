import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { modelAggregateOf } from "../helpers/aggregate.js";

/**
 * Chainable aggregates — Lucid types them `count: Aggregate<this>` on
 * `ModelQueryBuilderContract` (types/model.d.ts): the call returns the BUILDER
 * and the value arrives in `$extras`, not a scalar.
 *
 * Same method, two shapes, exactly as `DatabaseQueryBuilder` already did in
 * this package: no alias → the scalar atlas has always returned; an alias →
 * the Lucid projection.
 */
class Sale extends BaseModel {
	static override table = "sales";
	@PrimaryKey() declare id: string;
	@Column() declare region: string;
	@Column() declare amount: number;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE sales (id TEXT PRIMARY KEY, region TEXT, amount INTEGER)",
	);
	await conn.execute(
		"INSERT INTO sales VALUES ('1','eu',10),('2','eu',30),('3','us',5)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > chainable aggregates (Lucid parity)", () => {
	it("count('* as total') keeps the builder and puts the value in $extras", async () => {
		const rows = await Sale.query().count("* as total");

		expect(rows).toHaveLength(1);
		expect(Number(rows[0]?.$extras.total)).toBe(3);
	});

	it("sum('col as alias') projects the same way", async () => {
		const rows = await Sale.query().sum("amount as total_amount");

		expect(Number(rows[0]?.$extras.total_amount)).toBe(45);
	});

	it("chains with the rest of the query", async () => {
		// The point of returning the builder: a where can follow the aggregate.
		const rows = await Sale.query()
			.sum("amount as eu_total")
			.where("region", "eu");

		expect(Number(rows[0]?.$extras.eu_total)).toBe(40);
	});

	it("supports several aggregates in one query", async () => {
		const rows = await Sale.query()
			.min("amount as smallest")
			.max("amount as largest");

		expect(Number(rows[0]?.$extras.smallest)).toBe(5);
		expect(Number(rows[0]?.$extras.largest)).toBe(30);
	});

	it("projects a bare column too, rather than running the query", async () => {
		// There is no terminal scalar form: `count` and `countDistinct` used to
		// disagree about which arguments had one, and the disagreement was the
		// bug. The value is read off the row, on every aggregate.
		expect(
			await modelAggregateOf(Sale.query().count("* as total"), "total"),
		).toBe(3);
		expect(
			await modelAggregateOf(Sale.query().sum("amount as total"), "total"),
		).toBe(45);
	});
});
