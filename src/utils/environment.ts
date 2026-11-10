/**
 * The process environment as ream reads it, for atlas's production guards.
 *
 * ream normalises `NODE_ENV` through an alias table (`prod` is production,
 * `dev` development…) and its tooling also reads `REAM_ENV`. atlas compared
 * `NODE_ENV === 'production'` only, so `REAM_ENV=production` or `NODE_ENV=prod`
 * slipped past the guards. Atlas does not import ream (it stays agnostic), so
 * the table is restated here; it mirrors `packages/ream/src/env/nodeEnv.ts`.
 */

const DEV_ENVS = ["dev", "develop", "development"];
const PROD_ENVS = ["prod", "production"];
const TEST_ENVS = ["test", "testing"];

/** The canonical name of an environment value (`'unknown'` when unset). */
export function normalizeEnvironment(value: string | undefined): string {
	if (!value) return "unknown";
	const env = value.toLowerCase();
	if (DEV_ENVS.includes(env)) return "development";
	if (PROD_ENVS.includes(env)) return "production";
	if (TEST_ENVS.includes(env)) return "test";
	return env;
}

/**
 * Whether the process runs in production for a guard: when EITHER `REAM_ENV`
 * or `NODE_ENV` says so. A guard errs toward refusing, so one variable left
 * at `development` does not switch it off.
 */
export function isProductionEnvironment(
	env: Readonly<Record<string, string | undefined>> = process.env,
): boolean {
	return (
		normalizeEnvironment(env.REAM_ENV) === "production" ||
		normalizeEnvironment(env.NODE_ENV) === "production"
	);
}
