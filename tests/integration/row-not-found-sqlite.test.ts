/**
 * Every "a row was required, none came back" path throws the same error:
 * E_ROW_NOT_FOUND, status 404.
 *
 * The model query's firstOrFail()/sole() and the db builder's firstOrFail()
 * threw a bare Error with no code, while the repository threw
 * E_ENTITY_NOT_FOUND: an exception handler could not map one situation to
 * one 404.
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
import { EntityNotFoundError } from "../../src/errors.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService } from "../../src/services/db.js";

@Entity("rnf_users")
class RnfUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

let conn: AsyncDatabaseConnection;
let users: BaseRepository<RnfUser>;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE rnf_users (id INTEGER PRIMARY KEY, name TEXT)",
	);
	users = new BaseRepository(RnfUser, conn);
});

afterAll(async () => {
	await conn.close();
});

describe("atlas > a missing row is E_ROW_NOT_FOUND everywhere", () => {
	it.each<[string, () => Promise<unknown>]>([
		["repo.findOrFail", () => users.findOrFail(1)],
		["repo.findByOrFail", () => users.findByOrFail("name", "ghost")],
		[
			"Model.query().firstOrFail",
			() => users.query().where("id", 1).firstOrFail(),
		],
		["Model.query().sole", () => users.query().where("id", 1).sole()],
		[
			"db.from().firstOrFail",
			() =>
				createDbService(() => conn)
					.from("rnf_users")
					.where("id", 1)
					.firstOrFail(),
		],
	])("%s", async (_label, run) => {
		const error = await run().then(
			() => undefined,
			(e: unknown) => e,
		);
		expect(error).toBeInstanceOf(EntityNotFoundError);
		expect(error).toMatchObject({ code: "E_ROW_NOT_FOUND", status: 404 });
	});

	it("names the model when there is one", async () => {
		await expect(users.query().firstOrFail()).rejects.toMatchObject({
			entityClass: "RnfUser",
		});
	});
});
