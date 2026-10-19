import { describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";
import { AtlasError } from "../../src/errors.js";

/**
 * The PostgreSQL `sslmode` rule, through the NAPI boundary. No database is
 * needed: a mode that falls back to plaintext is refused before any attempt,
 * so the port below (which refuses every connection) is never reached by it.
 */
describe("atlas > PostgreSQL sslmode rule", () => {
	it("refuses prefer as a coded AtlasError, without retrying", async () => {
		const start = Date.now();
		const error = await createNapiConnection(
			"postgres://u:p@127.0.0.1:1/none?sslmode=prefer",
			1,
			1,
			undefined,
			{ retries: 3, backoffMs: 500, timeoutMs: 150 },
		).catch((e: unknown) => e);

		expect(error).toBeInstanceOf(AtlasError);
		expect(error).toMatchObject({ code: "E_ATLAS_INSECURE_SSLMODE" });
		// Refused up front: three retries with a 500ms backoff would take seconds.
		expect(Date.now() - start).toBeLessThan(400);
	});

	it("lets a URL without sslmode through to the connection attempt", async () => {
		const error = await createNapiConnection(
			"postgres://u:p@127.0.0.1:1/none",
			1,
			1,
			undefined,
			{ timeoutMs: 150 },
		).catch((e: unknown) => e);

		expect(error).toBeInstanceOf(Error);
		expect(error).not.toBeInstanceOf(AtlasError);
	});
});
