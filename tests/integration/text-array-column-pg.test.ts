/**
 * A Postgres array column read through the model and the query builder,
 * against a REAL PostgreSQL, gated on ATLAS_TEST_PG_URL.
 *
 * The driver had no decoder for array types: a `text[]` cell fell through to
 * the string decoder, which sqlx refuses for `TEXT[]`. A model query reads
 * `*` until `select()` names columns — as Lucid's does — so the column did
 * not have to be declared to break the read: every `find` of the table
 * failed. An array cell now reaches JS as an array, a NULL element as `null`,
 * which is what node-postgres returns for it.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService } from "../../src/services/db.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

@Entity("text_array_patients")
class Patient extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare familyName: string;
	@Column() declare tags: (string | null)[] | null;
}

describePg("atlas > Postgres array columns", () => {
	let conn: AsyncDatabaseConnection;
	let patients: BaseRepository<Patient>;

	beforeAll(async () => {
		setAtlasDialect("postgres");
		conn = await createNapiConnection(PG_URL, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS text_array_patients");
		// `family_name_prefix` is not declared on the entity, as in the report:
		// a prefix index the application maintains beside the model.
		await conn.execute(
			`CREATE TABLE text_array_patients (
				id integer PRIMARY KEY,
				family_name text NOT NULL,
				tags text[],
				family_name_prefix text[] NOT NULL,
				scores int4[]
			)`,
		);
		await conn.execute(
			`INSERT INTO text_array_patients VALUES
				(1, 'Durand', ARRAY['vip', NULL, 'a,"b'], ARRAY['d', 'du', 'dur'], ARRAY[3, NULL]),
				(2, 'Lee', NULL, '{}', NULL)`,
		);
		patients = new BaseRepository(Patient, conn);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS text_array_patients");
		await conn.close();
		setAtlasDialect("sqlite");
	});

	it("find() reads a table holding an undeclared text[] column", async () => {
		const patient = await patients.find(1);
		expect(patient?.familyName).toBe("Durand");
		expect(patient?.tags).toEqual(["vip", null, 'a,"b']);
		expect(patient?.$extras.family_name_prefix).toEqual(["d", "du", "dur"]);
		expect(patient?.$extras.scores).toEqual([3, null]);
	});

	it("query().first() reads a NULL array as null and an empty one as []", async () => {
		const patient = await patients.query().where("id", 2).first();
		expect(patient?.familyName).toBe("Lee");
		expect(patient?.tags).toBeNull();
		expect(patient?.$extras.family_name_prefix).toEqual([]);
		expect(patient?.$extras.scores).toBeNull();
	});

	it("the database query builder returns the arrays as they are", async () => {
		const db = createDbService(() => conn);
		const rows = await db
			.from("text_array_patients")
			.select("id", "family_name_prefix")
			.orderBy("id");
		expect(rows).toEqual([
			{ id: 1, family_name_prefix: ["d", "du", "dur"] },
			{ id: 2, family_name_prefix: [] },
		]);
	});
});
