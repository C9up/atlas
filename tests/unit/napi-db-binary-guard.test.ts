/**
 * A `db.<platform>.node` that loads is not necessarily the driver atlas calls.
 *
 * A stale build, or one from another version, loads just as well. It used to
 * be handed over through a cast, so a missing `ReamDatabase.connect` surfaced
 * as "connect is not a function" on the first connect. Now the load itself
 * refuses it and names what is missing.
 */

import { afterEach, describe, expect, it, vi } from "vitest";

/** Connect with `require` answering `binary` for the `.node` file. */
async function connectWith(binary: unknown): Promise<unknown> {
	vi.resetModules();
	vi.doMock("node:module", async (importOriginal) => ({
		...(await importOriginal<typeof import("node:module")>()),
		createRequire: () => () => binary,
	}));
	const { createNapiConnection } = await import(
		"../../src/adapters/NapiDbAdapter.js"
	);
	return createNapiConnection("sqlite::memory:", 1, 1).catch(
		(error: unknown) => error,
	);
}

afterEach(() => {
	vi.doUnmock("node:module");
	vi.resetModules();
});

describe("atlas > the native DB driver is checked at load", () => {
	it("refuses a driver whose ReamDatabase has no connect", async () => {
		const error = await connectWith({ ReamDatabase: class {} });

		expect(error).toBeInstanceOf(Error);
		expect(String(error)).toMatch(
			/db\.[\w-]+\.node' loaded, but it lacks ReamDatabase\.connect: it is stale/,
		);
	});

	it("refuses a driver without ReamDatabase at all", async () => {
		const error = await connectWith({ somethingElse: () => undefined });

		expect(String(error)).toMatch(/it lacks ReamDatabase:/);
	});

	it("refuses a module that is not an object", async () => {
		const error = await connectWith(undefined);

		expect(String(error)).toMatch(/it lacks ReamDatabase:/);
	});

	it("hands over a driver that carries connect", async () => {
		const connect = vi.fn(async () => {
			throw new Error("reached connect");
		});
		const error = await connectWith({ ReamDatabase: { connect } });

		// Past the guard: the stand-in's own connect is what answered.
		expect(connect).toHaveBeenCalled();
		expect(String(error)).toMatch(/reached connect/);
	});
});
