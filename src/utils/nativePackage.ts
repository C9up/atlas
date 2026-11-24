/**
 * Where atlas's two native binaries come from, and what to do when one is
 * missing.
 *
 * Both binaries of a platform, the query compiler (`index.<suffix>.node`) and
 * the database driver (`db.<suffix>.node`), ship in one npm package per
 * platform, `@c9up/atlas-<suffix>`, each an optional dependency of
 * `@c9up/atlas` pinned to its exact version. A checkout builds them at the
 * package root instead (`pnpm build:napi`), and that local file wins.
 */

import { arch, platform } from "node:process";
import { muslHint, supportedTargets } from "../vendor/nativeBinary.js";

/** The npm name the platform packages derive from: `@c9up/atlas-<suffix>`. */
export const ATLAS_PACKAGE_NAME = "@c9up/atlas";

/**
 * What to do about a binary that did not load, phrased for whoever has to fix
 * it.
 *
 * An application installs the platform package; a checkout of atlas builds the
 * local binary. The load error itself is not in here: it names paths on the
 * host, so it travels as the refusal's `cause` instead of its message.
 *
 * @param stem - which binary was looked for: `index` (the query compiler) or
 *   `db` (the database driver)
 * @param suffix - the napi-rs suffix of this host, `undefined` when none is built
 * @param platformPackage - set when the local file was absent and the platform
 *   package was looked for instead
 */
export function nativeRemedy(
	stem: "index" | "db",
	suffix: string | undefined,
	platformPackage: string | undefined,
): string {
	if (suffix === undefined) {
		return `no prebuilt binary for ${platform}-${arch}; atlas ships one for ${supportedTargets().join(", ")}.`;
	}
	if (platformPackage === undefined) {
		return `${stem}.${suffix}.node failed to load. Rebuild it: cd packages/atlas && pnpm build:napi`;
	}
	return (
		`install ${platformPackage} at the exact version of ${ATLAS_PACKAGE_NAME}: it is the optional dependency that carries the ${suffix} binaries, ` +
		"and an install that omits optional dependencies skips it. " +
		`In a checkout of atlas, build them instead: pnpm build:napi.${muslHint()}`
	);
}
