import { describe, expect, it } from "vitest";
import { configure } from "../../src/configure.js";

interface RecordedFile {
	path: string;
	content: string;
	options?: { force?: boolean };
}

interface FakeState {
	providers: string[];
	envVars: Record<string, string>;
	files: RecordedFile[];
}

function createFakeCodemods(): {
	state: FakeState;
	codemods: {
		addProvider: (importPath: string) => Promise<void>;
		addEnvVars: (vars: Record<string, string>) => Promise<void>;
		writeFile: (
			path: string,
			content: string,
			options?: { force?: boolean },
		) => Promise<void>;
	};
} {
	const state: FakeState = { providers: [], envVars: {}, files: [] };
	return {
		state,
		codemods: {
			async addProvider(importPath) {
				state.providers.push(importPath);
			},
			async addEnvVars(vars) {
				Object.assign(state.envVars, vars);
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
		// `default` (picks the connection) and `url` (ConnectionConfig requires it).
		expect(state.files[0]?.content).toContain("default: 'postgres'");
		expect(state.files[0]?.content).toContain("url:");
	});
});
