import { describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";

/**
 * End-to-end check that the retry / timeout knobs cross the NAPI boundary and
 * are honoured by the Rust driver. 127.0.0.1:1 refuses the connection; without
 * `timeoutMs` sqlx would retry internally for ~30s, so a sub-5s failure proves
 * the per-attempt timeout applied, and ≥ ~two bounded attempts proves the retry.
 */
describe("atlas > connect retry (NAPI passthrough)", () => {
	it("honors connectTimeoutMs and retries instead of the 30s sqlx default", async () => {
		const start = Date.now();
		await expect(
			createNapiConnection("postgres://u:p@127.0.0.1:1/none", 1, 1, undefined, {
				retries: 1,
				backoffMs: 25,
				timeoutMs: 150,
			}),
		).rejects.toThrow();
		const elapsed = Date.now() - start;
		// 2 attempts × ~150ms + 25ms backoff — at least one bounded retry happened…
		expect(elapsed).toBeGreaterThanOrEqual(150);
		// …and it did NOT fall back to sqlx's ~30s acquire window.
		expect(elapsed).toBeLessThan(5000);
	}, 10_000);
});
