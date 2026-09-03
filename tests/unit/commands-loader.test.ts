/**
 * `@c9up/atlas/commands` — the loader an application registers once, and the
 * config it reads its paths from.
 *
 * The point of these: a package must be able to add a command without a change
 * to the `ream` binary, and the command must find the same directories and the
 * same bookkeeping table the application itself booted with.
 */
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { AtlasDatabaseConfig } from "../../src/AtlasProvider.js";
import { getCommand, getMetaData } from "../../src/console/index.js";
import {
	clearDatabaseConfig,
	connectionConfigFor,
	primaryConnectionName,
	setDatabaseConfig,
} from "../../src/services/db.js";

/** The first row of a result the query is expected to return at least one of. */
function firstOf<T>(rows: readonly T[]): T {
	const [row] = rows;
	if (row === undefined) throw new Error("expected at least one row");
	return row;
}




const configured: AtlasDatabaseConfig[] = [];

function boot(config: AtlasDatabaseConfig, defaultName = "primary"): void {
	configured.push(config);
	setDatabaseConfig(config, defaultName);
}

afterEach(() => {
	for (const config of configured.splice(0)) clearDatabaseConfig(config);
});

describe("atlas > commands loader", () => {
	it("lists every shipped command with its namespace", async () => {
		const metadata = await getMetaData();
		const names = metadata.map((entry) => entry.commandName);

		expect(names).toContain("migration:run");
		expect(names).toContain("migration:rollback");
		expect(names).toContain("db:seed");
		expect(names).toContain("make:migration");
		expect(names).toContain("schema:generate");
		expect(names).toContain("atlas:check");
		expect(new Set(names).size).toBe(names.length);

		const run = metadata.find((entry) => entry.commandName === "migration:run");
		expect(run?.namespace).toBe("migration");
		expect(run?.description).toBeTruthy();
	});

	it("hands back the class for a listed command, and null for anything else", async () => {
		const first = firstOf(await getMetaData());
		const command = await getCommand(first);
		expect(command?.commandName).toBe(first.commandName);

		const missing = await getCommand({ ...first, commandName: "nope:nope" });
		expect(missing).toBeNull();
	});

	it("reads the migrations directory from the config, at run time", async () => {
		const directory = await mkdtemp(join(tmpdir(), "atlas-commands-"));
		try {
			boot({
				connection: "primary",
				connections: {
					primary: {
						url: "sqlite::memory:",
						migrations: { paths: [join(directory, "custom")] },
					},
				},
			});

			const MakeMigration = await getCommand({
				commandName: "make:migration",
				namespace: "make",
				description: "",
				aliases: [],
				options: {},
				args: [],
				flags: [],
			});
			expect(MakeMigration).not.toBeNull();
			if (MakeMigration === null) return;

			const command = new MakeMigration();
			Object.assign(command, { name: "create_users" });
			await command.run();

			const written = await readdir(join(directory, "custom"));
			expect(written).toHaveLength(1);
			expect(written[0]).toMatch(/_create_users\.ts$/);
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("prefers the connection's own keys over the top-level ones", () => {
		boot(
			{
				connection: "primary",
				migrations: { paths: ["top-level"] },
				connections: {
					primary: {
						url: "sqlite::memory:",
						migrations: { paths: ["per-connection"] },
					},
					// A connection that sets nothing inherits the top level.
					reporting: { url: "sqlite::memory:" },
				},
			},
			"primary",
		);

		expect(connectionConfigFor().migrations?.paths).toEqual(["per-connection"]);
		// A connection that declares nothing inherits the top level, which is the
		// single-connection form of the same config.
		expect(connectionConfigFor("reporting").migrations?.paths).toEqual([
			"top-level",
		]);
		expect(primaryConnectionName()).toBe("primary");
	});

	it("keeps a top-level key a connection did not override", () => {
		boot({
			connection: "primary",
			migrations: { tableName: "my_migrations" },
			connections: {
				primary: {
					url: "sqlite::memory:",
					migrations: { paths: ["database/migrations"] },
				},
			},
		});

		const migrations = connectionConfigFor().migrations;
		expect(migrations?.paths).toEqual(["database/migrations"]);
		// Dropping this is how a command ends up reading an empty history and
		// re-applying every migration the application already ran.
		expect(migrations?.tableName).toBe("my_migrations");
	});

	it("forgets the config only for the owner that set it", () => {
		const first: AtlasDatabaseConfig = { url: "sqlite::memory:" };
		const second: AtlasDatabaseConfig = { url: "sqlite::memory:" };
		setDatabaseConfig(first, "primary");
		setDatabaseConfig(second, "secondary");

		// The older provider shutting down must not unbind the newer binding.
		clearDatabaseConfig(first);
		expect(primaryConnectionName()).toBe("secondary");

		clearDatabaseConfig(second);
		expect(primaryConnectionName()).toBe("primary");
	});
});
