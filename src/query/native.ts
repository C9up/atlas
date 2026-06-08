/**
 * Native query compiler loader — loads the Rust NAPI binary.
 *
 * @implements FR36
 */

import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { arch, platform } from "node:process";
import { fileURLToPath } from "node:url";

const require2 = createRequire(import.meta.url);
const __dirname2 = dirname(fileURLToPath(import.meta.url));

const platformMap: Record<string, string> = {
	"linux-x64": "linux-x64-gnu",
	"darwin-x64": "darwin-x64",
	"darwin-arm64": "darwin-arm64",
	"win32-x64": "win32-x64-msvc",
	"linux-arm64": "linux-arm64-gnu",
};

interface NativeBinding {
	compileStatement: (specJson: string, dialect: string) => string;
	quoteIdent: (name: string) => string;
}

let native: NativeBinding | undefined;

let loadError: unknown;

try {
	const suffix = platformMap[`${platform}-${arch}`];
	if (suffix) {
		native = require2(join(__dirname2, `../../index.${suffix}.node`));
	}
} catch (e) {
	loadError = e;
}

export type AtlasDialect = "sqlite" | "postgres" | "mysql";

/** Module-level default dialect. Set by AtlasProvider at boot. */
let defaultDialect: AtlasDialect = "sqlite";

export function setAtlasDialect(dialect: AtlasDialect): void {
	defaultDialect = dialect;
}

export function getAtlasDialect(): AtlasDialect {
	return defaultDialect;
}

export interface CompiledStatement {
	statements: string[];
	params: unknown[];
}

/**
 * Compile any statement (SELECT/INSERT/UPDATE/DELETE/DDL) via the Rust compiler.
 * `spec` is a tagged object: `{ kind: 'select' | 'insert' | ..., ... }`.
 */
export function compileStatementNative(
	spec: object,
	dialect: AtlasDialect = defaultDialect,
): CompiledStatement {
	if (!native) {
		throw new Error(
			`[ATLAS_NAPI_NOT_FOUND] Rust query compiler not available: ${loadError ?? "binary not found"}`,
		);
	}
	const json = native.compileStatement(JSON.stringify(spec), dialect);
	return JSON.parse(json);
}
