import { existsSync } from "node:fs";
import { defineConfig } from "vitest/config";

/**
 * The sibling `@c9up/atom` source, present in the monorepo and absent from a
 * standalone checkout of atlas (which is what CI builds). The atom contract
 * suite needs it, so it is part of the run only where it can run: excluded
 * rather than skipped, it cannot read as coverage on a runner that never
 * executes it, and a zero-skipped gate still holds there.
 */
const hasAtomSource = existsSync(
	new URL("../atom/src/atlas.ts", import.meta.url),
);

export default defineConfig({
	test: {
		exclude: [
			"**/node_modules/**",
			"**/dist/**",
			// This test audits OTHER packages' shipped migration templates (reads
			// ../nova/migrations/… etc.) — a monorepo-level cross-submodule check
			// that can't run in the standalone atlas repo.
			"tests/unit/no-non-portable-helpers-in-templates.test.ts",
			...(hasAtomSource ? [] : ["tests/integration/atom-decimal-column.test.ts"]),
			// A manual bench through the BlackGate proxy, which no CI job stands
			// up: part of the run only when its URL is given.
			...(process.env.ATLAS_TEST_BLACKGATE_URL
				? []
				: ["tests/integration/blackgate.test.ts"]),
		],
		coverage: {
			provider: "v8",
			include: ["src/**"],
			// `src/vendor/**` is generated from scripts/vendor/ and identical in every
			// package that carries it, so measuring it here counts the same lines N
			// times and holds this package to a floor for code it cannot change. The
			// behaviour is pinned where it broke: bay's quasar-bridge suite covers the
			// two manager shapes the loader has to accept.
			exclude: ["src/**/*.d.ts", "src/vendor/**"],
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
