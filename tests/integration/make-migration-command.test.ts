/**
 * `make:migration` scaffolds a timestamped migration file. These tests invoke
 * the command's run() for real and assert the file on disk (name shape,
 * contents, no clobber, name validation), not console output.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { makeMigrationCommand } from "../../src/console/migrationCommands.js";
import { runCommand } from "../helpers/runCommand.js";

let tmpDir: string;

beforeEach(async () => {
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-make-"));
	process.exitCode = 0;
});

afterEach(async () => {
	await fsp.rm(tmpDir, { recursive: true, force: true });
	process.exitCode = 0;
});

async function onlyFile(): Promise<string> {
	const files = await fsp.readdir(tmpDir);
	expect(files).toHaveLength(1);
	return files[0] as string;
}

describe("make:migration", () => {
	it("writes a timestamp-prefixed file containing the Migration stub", async () => {
		await runCommand(makeMigrationCommand({ migrationsDir: tmpDir }), {
			name: "create_users",
		});

		const file = await onlyFile();
		expect(file).toMatch(/^\d+_create_users\.ts$/);
		const body = await fsp.readFile(path.join(tmpDir, file), "utf8");
		expect(body).toContain("import { Migration } from '@c9up/atlas'");
		expect(body).toContain("async up()");
		expect(body).toContain("async down()");
	});

	it("creates the migrations directory if it does not exist yet", async () => {
		const nested = path.join(tmpDir, "database", "migrations");
		await runCommand(makeMigrationCommand({ migrationsDir: nested }), {
			name: "init",
		});
		const files = await fsp.readdir(nested);
		expect(files).toHaveLength(1);
	});

	it("declares its name argument as required", async () => {
		// Rejecting a nameless run is the console kernel's job now: it validates
		// declared arguments before `run()` and names the missing one. What this
		// command owns is the declaration, so that is what is asserted here.
		// The kernel side is covered by ream's console parser/discovery tests.
		const Command = makeMigrationCommand({ migrationsDir: tmpDir });
		expect(Command.args).toEqual([
			expect.objectContaining({ propertyName: "name", required: true }),
		]);
		expect(await fsp.readdir(tmpDir)).toEqual([]);
	});

	it("rejects a name with path traversal and writes nothing", async () => {
		await runCommand(makeMigrationCommand({ migrationsDir: tmpDir }), {
			name: "../evil",
		});
		expect(process.exitCode).toBe(1);
		expect(await fsp.readdir(tmpDir)).toEqual([]);
	});

	it("rejects a name with a path separator and writes nothing", async () => {
		await runCommand(makeMigrationCommand({ migrationsDir: tmpDir }), {
			name: "sub/evil",
		});
		expect(process.exitCode).toBe(1);
		expect(await fsp.readdir(tmpDir)).toEqual([]);
	});
});
