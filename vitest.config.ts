import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// This test audits OTHER packages' shipped migration templates (reads
		// ../nova/migrations/… etc.) — a monorepo-level cross-submodule check
		// that can't run in the standalone atlas repo.
		exclude: [
			"**/node_modules/**",
			"**/dist/**",
			"tests/unit/no-non-portable-helpers-in-templates.test.ts",
		],
		coverage: {
			provider: "v8",
			include: ["src/**"],
			exclude: ["src/**/*.d.ts"],
			reporter: ["text-summary", "json-summary"],
			// Just under what the suite reaches, and now actually run. They read
			// 54/53/41/56 — some thirty points below reality — while nothing
			// ever checked them, so the gate would have let most of the suite
			// disappear without a word.
			thresholds: {
				lines: 85,
				statements: 84,
				branches: 74,
				functions: 81,
			},
		},
	},
});
