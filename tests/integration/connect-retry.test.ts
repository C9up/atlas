import { describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";

/**
 * End-to-end check that the retry / timeout knobs cross the NAPI boundary and
 * are honoured by the Rust driver.
 *
 * We key the timing assertion on the BACKOFF, not the per-attempt timeout: a
 * refused connection (127.0.0.1:1 sends an instant RST on most hosts) fails in
 * microseconds WITHOUT consuming `timeoutMs`, so the only deterministic delay is
 * the backoff slept between the two attempts. With `retries: 1` that backoff
 * elapses exactly once regardless of refuse-vs-hang, so `elapsed >= backoffMs`
 * robustly proves the retry crossed NAPI (a non-retrying driver returns ~0ms).
 * `timeoutMs` is still passed to prove it's accepted and the failure never falls
 * back to sqlx's ~30s acquire window.
 */
describe("atlas > connect retry (NAPI passthrough)", () => {
	it("honors the retry backoff and does not fall back to the 30s sqlx default", async () => {
		const backoffMs = 200;
		const start = Date.now();
		await expect(
			createNapiConnection("postgres://u:p@127.0.0.1:1/none", 1, 1, undefined, {
				retries: 1,
				backoffMs,
				timeoutMs: 150,
			}),
		).rejects.toThrow();
		const elapsed = Date.now() - start;
		// The single retry's backoff slept — proves the retry ran (allow timer slack).
		expect(elapsed).toBeGreaterThanOrEqual(backoffMs - 40);
		// …and it did NOT fall back to sqlx's ~30s acquire window.
		expect(elapsed).toBeLessThan(5000);
	}, 10_000);
});
