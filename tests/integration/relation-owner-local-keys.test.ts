/**
 * Relations joined on a key that is not the primary key, written as the
 * property or as the column.
 *
 * A `@BelongsTo` preload queried and indexed the owner rows by their primary
 * key and never read `ownerKey`: a user whose `countryCode` is 'CH' was matched
 * against `countries.id`, and got no country (or another one). `associate()`
 * read `ownerKey` as a property only, and a column-form `localKey` read off the
 * parent entity found nothing.
 *
 * SQLite always runs; Postgres runs when ATLAS_TEST_PG_URL is set.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import {
	BelongsTo,
	Column,
	Entity,
	HasMany,
	HasManyThrough,
	ManyToMany,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { ModelQuery } from "../../src/ModelQuery.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";

@Entity("ok_regions")
class Region extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
}

@Entity("ok_countries")
class Country extends BaseEntity {
	@PrimaryKey() declare id: number;
	// Mapped, so the property and the column are two different spellings.
	@Column({ columnName: "iso_code" }) declare code: string;
	@Column() declare name: string;
	@HasMany(() => Resident, { localKey: "code", foreignKey: "countryCode" })
	declare residentsByProperty: Resident[];
	@HasMany(() => Resident, { localKey: "iso_code", foreignKey: "country_code" })
	declare residentsByColumn: Resident[];
	@ManyToMany(() => Region, {
		pivotTable: "ok_country_regions",
		foreignKey: "country_code",
		otherKey: "region_id",
		localKey: "iso_code",
	})
	declare regions: Region[];
}

@Entity("ok_residents")
class Resident extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare countryCode: string | null;
	@Column() declare name: string;
	@BelongsTo(() => Country, { foreignKey: "countryCode", ownerKey: "code" })
	declare countryByProperty: Country | null;
	@BelongsTo(() => Country, {
		foreignKey: "country_code",
		ownerKey: "iso_code",
	})
	declare countryByColumn: Country | null;
	// The upstream spelling: on a belongs-to, `localKey` is the owner's key.
	@BelongsTo(() => Country, { foreignKey: "countryCode", localKey: "code" })
	declare countryByLocalKey: Country | null;
}

@Entity("ok_continents")
class Continent extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare tag: string;
	@HasManyThrough(
		() => Resident,
		() => Country,
		{
			localKey: "tag",
			firstKey: "continent_tag",
			secondKey: "countryCode",
			secondLocalKey: "iso_code",
		},
	)
	declare residents: Resident[];
}

function suite(label: string, url: string, dialect: AtlasDialect): void {
	(url ? describe : describe.skip)(`owner and local keys ${label}`, () => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			setAtlasDialect(dialect);
			conn = await createNapiConnection(url, 1, 2);
			for (const t of [
				"ok_country_regions",
				"ok_regions",
				"ok_residents",
				"ok_countries",
				"ok_continents",
			])
				await conn.execute(`DROP TABLE IF EXISTS ${t}`);
			await conn.execute(
				"CREATE TABLE ok_continents (id INTEGER PRIMARY KEY, tag VARCHAR(10))",
			);
			await conn.execute(
				"CREATE TABLE ok_countries (id INTEGER PRIMARY KEY, iso_code VARCHAR(10), name VARCHAR(20), continent_tag VARCHAR(10))",
			);
			await conn.execute(
				"CREATE TABLE ok_residents (id INTEGER PRIMARY KEY, country_code VARCHAR(10), name VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE ok_regions (id INTEGER PRIMARY KEY, label VARCHAR(20))",
			);
			await conn.execute(
				"CREATE TABLE ok_country_regions (country_code VARCHAR(10), region_id INTEGER)",
			);
			await conn.execute(
				"INSERT INTO ok_continents (id, tag) VALUES (1, 'EU'), (2, 'AS')",
			);
			// Ids and codes deliberately disagree in order: matching on the
			// primary key would pick the other country.
			await conn.execute(
				"INSERT INTO ok_countries (id, iso_code, name, continent_tag) VALUES " +
					"(1, 'CH', 'Switzerland', 'EU'), (2, 'FR', 'France', 'EU'), (3, 'JP', 'Japan', 'AS')",
			);
			await conn.execute(
				"INSERT INTO ok_residents (id, country_code, name) VALUES " +
					"(1, 'FR', 'Ana'), (2, 'CH', 'Bo'), (3, NULL, 'Cy')",
			);
			await conn.execute(
				"INSERT INTO ok_regions (id, label) VALUES (1, 'Alps'), (2, 'Coast')",
			);
			await conn.execute(
				"INSERT INTO ok_country_regions (country_code, region_id) VALUES ('CH', 1), ('FR', 1), ('FR', 2)",
			);
		});

		beforeEach(() => {
			setAtlasDialect(dialect);
		});

		afterAll(async () => {
			await conn?.close();
		});

		const residents = () => new BaseRepository(Resident, conn);
		const countries = () => new BaseRepository(Country, conn);

		it("@BelongsTo preloads by the owner key, in every spelling", async () => {
			const rows = await residents()
				.query()
				.preload("countryByProperty")
				.preload("countryByColumn")
				.preload("countryByLocalKey")
				.orderBy("id")
				.exec();
			const names = (key: string) =>
				rows.map((r) => {
					const country = r[key];
					return country instanceof Country ? country.name : null;
				});
			const expected = ["France", "Switzerland", null];
			expect(names("countryByProperty")).toEqual(expected);
			expect(names("countryByColumn")).toEqual(expected);
			expect(names("countryByLocalKey")).toEqual(expected);
		});

		it("@BelongsTo whereHas and related() join on the owner key", async () => {
			const swiss = await residents()
				.query()
				.whereHas("countryByColumn", (q) => q.where("name", "Switzerland"))
				.exec();
			expect(swiss.map((r) => r.name)).toEqual(["Bo"]);

			const ana = await residents().findOrFail(1);
			const q = ana.related("countryByProperty").query();
			if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
			expect((await q.exec()).map((c) => c.name)).toEqual(["France"]);
		});

		it("associate() writes the owner key, declared as the column", async () => {
			const cy = await residents().findOrFail(3);
			const japan = await countries().findOrFail(3);
			const proxy = cy.related("countryByColumn");
			if (proxy.type !== "belongsTo") throw new Error("expected a belongsTo");
			await proxy.associate(japan);
			const [row] = await conn.query<{ country_code: string | null }>(
				"SELECT country_code FROM ok_residents WHERE id = 3",
			);
			expect(row?.country_code).toBe("JP");
			await conn.execute(
				"UPDATE ok_residents SET country_code = NULL WHERE id = 3",
			);
		});

		it("@HasMany preloads and loads by a non-primary local key, in both forms", async () => {
			const rows = await countries()
				.query()
				.preload("residentsByProperty")
				.preload("residentsByColumn")
				.orderBy("id")
				.exec();
			const byProperty = rows.map((c) =>
				c.residentsByProperty.map((r) => r.name),
			);
			const byColumn = rows.map((c) => c.residentsByColumn.map((r) => r.name));
			expect(byProperty).toEqual([["Bo"], ["Ana"], []]);
			expect(byColumn).toEqual([["Bo"], ["Ana"], []]);

			const france = await countries().findOrFail(2);
			const q = france.related("residentsByColumn").query();
			if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
			expect((await q.exec()).map((r) => r.name)).toEqual(["Ana"]);
		});

		it("@ManyToMany preloads by a column-form local key", async () => {
			const rows = await countries()
				.query()
				.preload("regions")
				.orderBy("id")
				.exec();
			expect(rows.map((c) => c.regions.map((r) => r.label).sort())).toEqual([
				["Alps"],
				["Alps", "Coast"],
				[],
			]);
			const france = await countries().findOrFail(2);
			const q = france.related("regions").query();
			if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
			expect((await q.exec()).map((r) => r.label).sort()).toEqual([
				"Alps",
				"Coast",
			]);
		});

		it("@HasManyThrough preloads by non-primary keys at both hops", async () => {
			const rows = await new BaseRepository(Continent, conn)
				.query()
				.preload("residents")
				.orderBy("id")
				.exec();
			expect(rows.map((c) => c.residents.map((r) => r.name).sort())).toEqual([
				["Ana", "Bo"],
				[],
			]);
		});
	});
}

suite("SQLite", "sqlite::memory:", "sqlite");
suite("PG", process.env.ATLAS_TEST_PG_URL ?? "", "postgres");
