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
			thresholds: {
				lines: 54,
				statements: 53,
				branches: 41,
				functions: 56,
			},
		},
	},
});
