/**
 * Where atlas's two binaries come from.
 *
 * A checkout builds `index.<suffix>.node` (the query compiler) and
 * `db.<suffix>.node` (the database driver) at the package root; a published
 * `@c9up/atlas` carries neither, and finds both in the platform package
 * `@c9up/atlas-<suffix>` its package manager installed for the host. The local
 * file wins, the package is looked for only when that file is absent, and a
 * refusal names the package to install, with the load error as its cause.
 */

import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";

/**
 * The host the loaders are told they run on. Stubbed like `require` is, so
 * the suite asserts the same names on every platform CI runs it on.
 */
const HOST = { platform: "linux", arch: "x64", suffix: "linux-x64-gnu" };

const PLATFORM_PACKAGE = `@c9up/atlas-${HOST.suffix}`;
const DB_IN_PACKAGE = `${PLATFORM_PACKAGE}/db.${HOST.suffix}.node`;

/** Where a checkout builds the two binaries: the package root. */
const ROOT = fileURLToPath(new URL("../../", import.meta.url));
const LOCAL_INDEX = join(ROOT, `index.${HOST.suffix}.node`);
const LOCAL_DB = join(ROOT, `db.${HOST.suffix}.node`);

/**
 * What atlas asked `require` for, in order. Its dependencies load their own
 * binaries through the same stub (chronos does), and those are not atlas's.
 */
function atlasAsks(asked: readonly string[]): string[] {
	const own = [LOCAL_INDEX, LOCAL_DB, PLATFORM_PACKAGE, DB_IN_PACKAGE];
	return asked.filter((id) => own.includes(id));
}

/** What `require` throws for a module that is not there. */
function notFound(id: string): Error {
	return Object.assign(new Error(`Cannot find module '${id}'`), {
		code: "MODULE_NOT_FOUND",
	});
}

/** A query compiler that answers `namedToPositional`. */
const compiler = () => ({
	checkArithmeticTail: () => undefined,
	compileStatement: () => null,
	namedToPositional: () => ({ sql: "SELECT ?", bindings: ["kept"] }),
});

/** A database driver whose `connect` proves it was reached. */
const driver = () => ({
	ReamDatabase: {
		connect: async () => {
			throw new Error("reached connect");
		},
	},
});

/** Load atlas with `require` running `load` for every module it asks for. */
async function withRequire(
	load: (id: string) => unknown,
	host: { platform: string; arch: string } = HOST,
) {
	vi.resetModules();
	vi.doMock("node:process", async (importOriginal) => ({
		...(await importOriginal<typeof import("node:process")>()),
		platform: host.platform,
		arch: host.arch,
	}));
	vi.doMock("node:module", async (importOriginal) => ({
		...(await importOriginal<typeof import("node:module")>()),
		createRequire: () => load,
	}));
	return {
		native: await import("../../src/query/native.js"),
		adapter: await import("../../src/adapters/NapiDbAdapter.js"),
		errors: await import("../../src/errors.js"),
	};
}

/** What compiling through the query compiler throws, or `undefined`. */
function compileError(native: typeof import("../../src/query/native.js")) {
	try {
		native.namedToPositionalNative("SELECT :a", ["a"], "postgres");
		return undefined;
	} catch (error) {
		return error;
	}
}

/** What connecting through the database driver rejects with. */
function connectError(
	adapter: typeof import("../../src/adapters/NapiDbAdapter.js"),
) {
	return adapter
		.createNapiConnection("sqlite::memory:", 1, 1)
		.catch((error: unknown) => error);
}

afterEach(() => {
	vi.doUnmock("node:process");
	vi.doUnmock("node:module");
	vi.resetModules();
});

describe("atlas > the query compiler comes from the local build or the platform package", () => {
	it("loads the platform package when no local binary is built", async () => {
		const asked: string[] = [];
		const { native } = await withRequire((id) => {
			asked.push(id);
			if (id === PLATFORM_PACKAGE) return compiler();
			throw notFound(id);
		});

		expect(
			native.namedToPositionalNative("SELECT :a", ["a"], "postgres"),
		).toEqual({ sql: "SELECT ?", bindings: ["kept"] });
		expect(atlasAsks(asked)).toEqual([LOCAL_INDEX, PLATFORM_PACKAGE]);
	});

	it("prefers the local build, and never asks for the package then", async () => {
		const asked: string[] = [];
		const { native } = await withRequire((id) => {
			asked.push(id);
			return compiler();
		});

		expect(compileError(native)).toBeUndefined();
		expect(atlasAsks(asked)).toEqual([LOCAL_INDEX]);
	});

	it("names the platform package to install when neither is there", async () => {
		const { native, errors } = await withRequire((id) => {
			throw notFound(id);
		});

		const refusal = compileError(native);

		expect(refusal).toBeInstanceOf(errors.E_ATLAS_NAPI_NOT_FOUND);
		expect(String(refusal)).toContain(
			`Rust query compiler not available. Fix: install ${PLATFORM_PACKAGE} at the exact version of @c9up/atlas`,
		);
		expect(String(refusal)).toContain("pnpm build:napi");
		// The load error travels as the cause, not in the message: it carries host paths.
		expect(refusal).toMatchObject({ cause: { code: "MODULE_NOT_FOUND" } });
		expect(String(refusal)).not.toContain("Cannot find module");
	});

	it("reports a local build that fails to load, instead of falling back to the package", async () => {
		const asked: string[] = [];
		const { native } = await withRequire((id) => {
			asked.push(id);
			throw new Error("invalid ELF header");
		});

		const refusal = compileError(native);

		expect(atlasAsks(asked)).toEqual([LOCAL_INDEX]);
		expect(String(refusal)).toContain(
			"Fix: index.linux-x64-gnu.node failed to load. Rebuild it: cd packages/atlas && pnpm build:napi",
		);
		expect(refusal).toMatchObject({ cause: { message: "invalid ELF header" } });
	});

	it("rejects a platform package from another version, naming it in the cause", async () => {
		const { native } = await withRequire((id) => {
			// The binary of an earlier version: it compiles, and has no
			// expression check.
			if (id === PLATFORM_PACKAGE) {
				return {
					compileStatement: () => null,
					namedToPositional: () => null,
				};
			}
			throw notFound(id);
		});

		const refusal = compileError(native);

		expect(String(refusal)).toContain(`Fix: install ${PLATFORM_PACKAGE}`);
		expect(refusal).toMatchObject({
			cause: {
				message: `${PLATFORM_PACKAGE} loaded, but it lacks checkArithmeticTail. The binary is stale or from another version: install the version the package that loads it pins.`,
			},
		});
	});

	it("lists the supported targets on a host no binary is built for", async () => {
		const asked: string[] = [];
		const { native } = await withRequire(
			(id) => {
				asked.push(id);
				return compiler();
			},
			{ platform: "linux", arch: "riscv64" },
		);

		const refusal = compileError(native);

		expect(atlasAsks(asked)).toEqual([]);
		expect(String(refusal)).toContain(
			"Fix: no prebuilt binary for linux-riscv64; atlas ships one for linux-x64, linux-arm64, darwin-x64, darwin-arm64, win32-x64.",
		);
	});
});

describe("atlas > the database driver comes from the local build or the platform package", () => {
	it("loads db.<suffix>.node from the platform package when no local one is built", async () => {
		const asked: string[] = [];
		const { adapter } = await withRequire((id) => {
			asked.push(id);
			if (id === PLATFORM_PACKAGE) return compiler();
			if (id === DB_IN_PACKAGE) return driver();
			throw notFound(id);
		});

		const error = await connectError(adapter);

		// Past the loader: the stand-in's own connect is what answered.
		expect(String(error)).toMatch(/reached connect/);
		expect(atlasAsks(asked)).toEqual([
			LOCAL_INDEX,
			PLATFORM_PACKAGE,
			LOCAL_DB,
			DB_IN_PACKAGE,
		]);
	});

	it("prefers the local build, and never asks for the package then", async () => {
		const asked: string[] = [];
		const { adapter } = await withRequire((id) => {
			asked.push(id);
			return id === LOCAL_DB ? driver() : compiler();
		});

		const error = await connectError(adapter);

		expect(String(error)).toMatch(/reached connect/);
		expect(atlasAsks(asked)).toEqual([LOCAL_INDEX, LOCAL_DB]);
	});

	it("names the platform package to install when neither is there", async () => {
		const { adapter, errors } = await withRequire((id) => {
			throw notFound(id);
		});

		const refusal = await connectError(adapter);

		expect(refusal).toBeInstanceOf(errors.E_ATLAS_DB_DRIVER_NOT_FOUND);
		expect(String(refusal)).toContain(
			`Rust database driver not available. Fix: install ${PLATFORM_PACKAGE} at the exact version of @c9up/atlas`,
		);
		expect(refusal).toMatchObject({
			cause: {
				code: "MODULE_NOT_FOUND",
				message: `Cannot find module '${DB_IN_PACKAGE}'`,
			},
		});
		expect(String(refusal)).not.toContain("Cannot find module");
	});

	it("reports a local build that fails to load, instead of falling back to the package", async () => {
		const asked: string[] = [];
		const { adapter } = await withRequire((id) => {
			asked.push(id);
			if (id === LOCAL_DB) throw new Error("invalid ELF header");
			return compiler();
		});

		const refusal = await connectError(adapter);

		expect(atlasAsks(asked)).toEqual([LOCAL_INDEX, LOCAL_DB]);
		expect(String(refusal)).toContain(
			"Fix: db.linux-x64-gnu.node failed to load. Rebuild it: cd packages/atlas && pnpm build:napi",
		);
		expect(refusal).toMatchObject({ cause: { message: "invalid ELF header" } });
	});

	it("rejects a platform package from another version, naming it", async () => {
		const { adapter } = await withRequire((id) => {
			if (id === DB_IN_PACKAGE) return { ReamDatabase: {} };
			if (id === PLATFORM_PACKAGE) return compiler();
			throw notFound(id);
		});

		const refusal = await connectError(adapter);

		expect(String(refusal)).toContain(
			`Rust database driver ${PLATFORM_PACKAGE} (db.linux-x64-gnu.node) loaded, but it lacks ReamDatabase.connect: ` +
				`it is stale or from another version. Fix: install the version of ${PLATFORM_PACKAGE} that @c9up/atlas pins.`,
		);
	});

	it("lists the supported targets on a host no binary is built for", async () => {
		const asked: string[] = [];
		const { adapter } = await withRequire(
			(id) => {
				asked.push(id);
				return driver();
			},
			{ platform: "win32", arch: "ia32" },
		);

		const refusal = await connectError(adapter);

		expect(atlasAsks(asked)).toEqual([]);
		expect(String(refusal)).toContain(
			"Rust database driver not available. Fix: no prebuilt binary for win32-ia32; atlas ships one for",
		);
	});
});
