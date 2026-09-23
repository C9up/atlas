import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { configure } from "../../src/configure.js";

interface RecordedFile {
	path: string;
	content: string;
	options?: { force?: boolean };
}

interface FakeState {
	providers: string[];
	commands: string[];
	envVars: Record<string, string>;
	files: RecordedFile[];
}

/**
 * Read a stub the way `codemods.makeUsingStub` does.
 *
 * The real file, not a fixture: a test that stubbed this out would pass with
 * a stub that does not exist.
 */
function renderStub(
	stubsRoot: string,
	stubPath: string,
	state: Record<string, string | number | boolean>,
): { to: string; body: string } {
	const raw = readFileSync(resolve(stubsRoot, stubPath), "utf8");
	const [, front = "", body = ""] = raw.split(/^---\r?\n/m, 3);
	const declared = /^to:\s*(.+)$/m.exec(front)?.[1]?.trim() ?? "";
	const render = (text: string): string =>
		text.replace(/\{\{\s*([\w.]+)\s*\}\}/g, (match, key: string) =>
			state[key] === undefined ? match : String(state[key]),
		);
	return { to: render(declared), body: render(body) };
}

function createFakeCodemods(): {
	state: FakeState;
	codemods: {
		addProvider: (importPath: string) => Promise<void>;
		registerCommand: (importPath: string) => Promise<void>;
		addEnvVars: (vars: Record<string, string>) => Promise<void>;
		writeFile: (
			path: string,
			content: string,
			options?: { force?: boolean },
		) => Promise<void>;
		makeUsingStub: (
			stubsRoot: string,
			stubPath: string,
			state?: Record<string, string | number | boolean>,
			options?: { force?: boolean },
		) => Promise<{ path: string; contents: string }>;
	};
} {
	const state: FakeState = {
		providers: [],
		commands: [],
		envVars: {},
		files: [],
	};
	return {
		state,
		codemods: {
			async addProvider(importPath) {
				state.providers.push(importPath);
			},
			async registerCommand(importPath) {
				state.commands.push(importPath);
			},
			async addEnvVars(vars) {
				Object.assign(state.envVars, vars);
			},
			async makeUsingStub(
				stubsRoot: string,
				stubPath: string,
				state: Record<string, string | number | boolean> = {},
			) {
				const { to, body } = renderStub(stubsRoot, stubPath, state);
				await this.writeFile(to, body);
				return { path: to, contents: body };
			},
			async writeFile(path, content, options) {
				state.files.push({ path, content, options });
			},
		},
	};
}

describe("atlas > configure", () => {
	it("registers the provider, env vars and config/database.ts", async () => {
		const { state, codemods } = createFakeCodemods();
		await configure(codemods);

		expect(state.providers).toEqual(["@c9up/atlas/provider"]);
		// The commands come from the package, never from the `ream` binary.
		expect(state.commands).toEqual(["@c9up/atlas/commands"]);
		expect(state.envVars).toMatchObject({
			DB_HOST: "localhost",
			DB_PORT: "5432",
			DB_DATABASE: "ream",
			DB_USER: "postgres",
			DB_PASSWORD: "change-me",
		});
		expect(state.files).toHaveLength(1);
		expect(state.files[0]?.path).toBe("config/database.ts");
		expect(state.files[0]?.content).toContain("@c9up/atlas");
		expect(state.files[0]?.content).toContain("connections:");
		// The generated config must use the fields AtlasProvider actually reads:
		// `connection` (picks the connection) and `url` (ConnectionConfig requires it).
		expect(state.files[0]?.content).toContain("connection: 'postgres'");
		// The paths the shipped commands read, in the place Lucid keeps them.
		expect(state.files[0]?.content).toContain("migrations:");
		expect(state.files[0]?.content).toContain("seeders:");
		expect(state.files[0]?.content).toContain("url:");
	});
});
