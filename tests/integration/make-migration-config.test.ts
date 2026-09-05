/**
 * Where `make:migration` writes, in a project with more than one database.
 *
 * The command is `startApp: false` on purpose — scaffolding a migration must
 * not need a reachable database, which is the whole reason you are writing
 * one. But the directory comes from `config/database.ts`, and that config only
 * reaches memory when `AtlasProvider` boots. So the command meant to run
 * without the application was the one that could not see its configuration: it
 * wrote to `database/migrations` while the project's own directory sat in the
 * file next door, and said nothing.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { makeMigrationCommand } from "../../src/console/migrationCommands.js";
import { forgetProjectConfig } from "../../src/console/projectConfig.js";
import { runCommand } from "../helpers/runCommand.js";

let tmpDir: string;
let cwd: string;

/** A project whose two databases keep their migrations apart. */
const TWO_DATABASES = `export default {
  connection: 'pg',
  connections: {
    pg: { url: 'postgres://x/y', migrations: { paths: ['db/pg'] } },
    analytics: { url: 'mysql://x/y', migrations: { paths: ['db/analytics'] } },
  },
}
`;

async function writeConfig(contents: string): Promise<void> {
	await fsp.mkdir(path.join(tmpDir, "config"), { recursive: true });
	await fsp.writeFile(path.join(tmpDir, "config", "database.ts"), contents);
}

/** Every file written under the project, relative to it. */
async function tree(dir = tmpDir): Promise<string[]> {
	const out: string[] = [];
	const walk = async (at: string) => {
		for (const entry of await fsp.readdir(at, { withFileTypes: true })) {
			const full = path.join(at, entry.name);
			if (entry.isDirectory()) await walk(full);
			else out.push(path.relative(dir, full).replaceAll(path.sep, "/"));
		}
	};
	await walk(dir).catch(() => {});
	return out.filter((f) => !f.startsWith("config/"));
}

const command = () =>
	makeMigrationCommand({
		get migrationsDir() {
			return "database/migrations";
		},
	} as never);

beforeEach(async () => {
	cwd = process.cwd();
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-makecfg-"));
	process.chdir(tmpDir);
	forgetProjectConfig();
	process.exitCode = 0;
});

afterEach(async () => {
	process.chdir(cwd);
	forgetProjectConfig();
	await fsp.rm(tmpDir, { recursive: true, force: true });
	process.exitCode = 0;
});

describe("atlas > make:migration reads the project's own configuration", () => {
	it("writes where config/database.ts says, without booting the app", async () => {
		await writeConfig(TWO_DATABASES);
		await runCommand(command(), { name: "add_users" });

		const [written] = await tree();
		expect(written).toMatch(/^db\/pg\/\d+_add_users\.ts$/);
	});

	it("writes into the connection asked for, in a project with several", async () => {
		// The reason the flag exists: two databases, two directories, and no way
		// to say which one you are writing for.
		await writeConfig(TWO_DATABASES);
		await runCommand(command(), {
			name: "add_events",
			connection: "analytics",
		});

		const [written] = await tree();
		expect(written).toMatch(/^db\/analytics\/\d+_add_events\.ts$/);
	});

	it("refuses a connection the config does not declare", async () => {
		// Writing to the default here would look like it worked, and the
		// migration would run against a database nobody meant.
		await writeConfig(TWO_DATABASES);
		const error = vi.spyOn(console, "error").mockImplementation(() => {});

		await runCommand(command(), { name: "oops", connection: "warehouse" });

		expect(process.exitCode).toBe(1);
		expect(error.mock.calls.flat().join(" ")).toContain(
			'declares no connection named "warehouse"',
		);
		expect(await tree()).toEqual([]);
		error.mockRestore();
	});

	it("falls back to the default when the project has no config", async () => {
		await runCommand(command(), { name: "add_users" });
		const [written] = await tree();
		expect(written).toMatch(/^database\/migrations\/\d+_add_users\.ts$/);
	});

	it("says why when the config cannot be read, instead of writing elsewhere quietly", async () => {
		await writeConfig("export default (( broken\n");
		const error = vi.spyOn(console, "error").mockImplementation(() => {});

		await runCommand(command(), { name: "add_users" });

		expect(error.mock.calls.flat().join(" ")).toMatch(/\[atlas\]/);
		const [written] = await tree();
		expect(written).toMatch(/^database\/migrations\//);
		error.mockRestore();
	});

	it("takes the top-level paths of a single-database project", async () => {
		await writeConfig(
			"export default { url: 'sqlite:app.db', migrations: { paths: ['db/changes'] } }\n",
		);
		await runCommand(command(), { name: "add_users" });

		const [written] = await tree();
		expect(written).toMatch(/^db\/changes\//);
	});
});

describe("atlas > make:seeder reads it the same way", () => {
	it("writes where the connection's config says", async () => {
		// The same hole, in the command beside it: `startApp: false`, and the
		// directory only in memory once the provider had booted.
		await writeConfig(`export default {
  connection: 'pg',
  connections: {
    pg: { url: 'postgres://x/y', seeders: { paths: ['db/pg-seeds'] } },
    analytics: { url: 'mysql://x/y', seeders: { paths: ['db/analytics-seeds'] } },
  },
}
`);
		const { makeSeederCommand } = await import(
			"../../src/console/seederCommands.js"
		);
		const Command = makeSeederCommand({
			get seedersDir() {
				return "database/seeders";
			},
		} as never);

		await runCommand(Command, { name: "users", connection: "analytics" });

		const [written] = await tree();
		expect(written).toMatch(/^db\/analytics-seeds\/\d+_users\.ts$/);
	});
});
