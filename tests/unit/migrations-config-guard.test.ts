import { afterEach, describe, expect, it, vi } from "vitest";
import AtlasProvider from "../../src/AtlasProvider.js";

/**
 * Two ways boot-time migration went wrong in the field, both silent.
 *
 * A key that no longer exists is accepted and ignored, so a rename stopped the
 * migrations without a word: a deployment came up on an empty schema and
 * answered every request with `relation "x" does not exist`. And `autoRun`
 * fired on every dev restart, racing the instance that had not finished
 * shutting down for the migration lock — two failures per saved file.
 */

function appWith(migrations: Record<string, unknown>) {
	const config = { url: "sqlite::memory:", migrations };
	return {
		container: {
			has: () => false,
			bind: () => {},
			singleton: () => {},
			make: async () => undefined,
			resolve: async () => undefined,
		},
		config: { get: () => config, set: () => {} },
	};
}

describe("atlas > migrations config", () => {
	afterEach(() => vi.restoreAllMocks());

	it("names a key it does not know, instead of ignoring it in silence", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const provider = new AtlasProvider(
			appWith({ autoRunInProduction: true }) as never,
		);
		await provider.boot().catch(() => undefined);
		const said = warn.mock.calls.map((c) => String(c[0])).join("\n");
		expect(said).toContain("autoRunInProduction");
		// And points at the thing that would have caught it earlier.
		expect(said).toContain("defineConfig");
	});

	it("says nothing when every key is one it knows", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const provider = new AtlasProvider(
			appWith({ autoRun: true, naturalSort: true }) as never,
		);
		await provider.boot().catch(() => undefined);
		const said = warn.mock.calls.map((c) => String(c[0])).join("\n");
		expect(said).not.toContain("not a known option");
	});
});
