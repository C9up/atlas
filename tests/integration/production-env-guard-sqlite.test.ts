/**
 * The production guards read the environment the way ream does: `REAM_ENV`
 * as well as `NODE_ENV`, through ream's alias table (`prod` is production).
 * They compared `NODE_ENV === 'production'` only, so `REAM_ENV=production` or
 * `NODE_ENV=prod` let a reset run without `--force` and migration:run apply
 * without asking.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { migrationRunCommand } from "../../src/console/migrationCommands.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { MigrationRunner } from "../../src/schema/MigrationRunner.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { isProductionEnvironment } from "../../src/utils/environment.js";
import { runCommand } from "../helpers/runCommand.js";

const saved = {
	NODE_ENV: process.env.NODE_ENV,
	REAM_ENV: process.env.REAM_ENV,
};

function setEnv(
	name: "NODE_ENV" | "REAM_ENV",
	value: string | undefined,
): void {
	if (value === undefined) delete process.env[name];
	else process.env[name] = value;
}

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	setDb(conn);
	setEnv("NODE_ENV", undefined);
	setEnv("REAM_ENV", undefined);
	process.exitCode = 0;
});

afterEach(async () => {
	setEnv("NODE_ENV", saved.NODE_ENV);
	setEnv("REAM_ENV", saved.REAM_ENV);
	clearDb(conn);
	await conn.close();
	process.exitCode = 0;
});

describe("atlas > production detection", () => {
	it.each([
		[{ REAM_ENV: "production" }, true],
		[{ REAM_ENV: "PROD" }, true],
		[{ NODE_ENV: "prod" }, true],
		[{ NODE_ENV: "production" }, true],
		// A guard errs toward refusing: either variable is enough.
		[{ REAM_ENV: "development", NODE_ENV: "production" }, true],
		[{ REAM_ENV: "production", NODE_ENV: "development" }, true],
		[{ NODE_ENV: "development" }, false],
		[{ REAM_ENV: "staging" }, false],
		[{}, false],
	])("%o → %s", (env, expected) => {
		expect(isProductionEnvironment(env)).toBe(expected);
	});

	it("the rollback guard refuses reset under REAM_ENV=production", async () => {
		setEnv("REAM_ENV", "production");
		await expect(new MigrationRunner(conn).reset()).rejects.toMatchObject({
			code: "E_ROLLBACK_DISABLED_IN_PRODUCTION",
		});
		setEnv("REAM_ENV", undefined);
		setEnv("NODE_ENV", "prod");
		await expect(new MigrationRunner(conn).reset()).rejects.toMatchObject({
			code: "E_ROLLBACK_DISABLED_IN_PRODUCTION",
		});
	});

	it("migration:run asks under REAM_ENV=production", async () => {
		setEnv("REAM_ENV", "production");
		const asked: string[] = [];
		await runCommand(migrationRunCommand({ migrationsDir: "/nonexistent" }), {
			prompt: {
				async confirm(message: string) {
					asked.push(message);
					return false;
				},
			},
		});
		expect(asked).toHaveLength(1);
	});
});
