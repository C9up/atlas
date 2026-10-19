/**
 * atlas through BlackGate, the PostgreSQL proxy that requires a client
 * certificate (mTLS) and encrypts configured columns. Gated on two URLs:
 *
 *   ATLAS_TEST_BLACKGATE_URL     through the proxy, sslmode=verify-full with
 *                                sslrootcert, sslcert and sslkey
 *   ATLAS_TEST_BLACKGATE_PG_URL  the PostgreSQL behind it, direct, to see what
 *                                is stored
 *
 * The proxy must encrypt `users.email` (deterministic) and `users.ssn`
 * (randomized), on a table `users (id serial, name text, email text, ssn text)`.
 * ATLAS_TEST_BLACKGATE_POOL_SIZE, when given, is the proxy's `pool_size`.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

const PROXY_URL = process.env.ATLAS_TEST_BLACKGATE_URL ?? "";
const BACKEND_URL = process.env.ATLAS_TEST_BLACKGATE_PG_URL ?? "";
const POOL_SIZE = Number(process.env.ATLAS_TEST_BLACKGATE_POOL_SIZE ?? "0");
const describeBlackGate = PROXY_URL && BACKEND_URL ? describe : describe.skip;

/** The proxy URL with some of its query parameters replaced or removed. */
function variant(changes: Record<string, string | null>): string {
	const target = new URL(PROXY_URL);
	for (const [key, value] of Object.entries(changes)) {
		if (value === null) target.searchParams.delete(key);
		else target.searchParams.set(key, value);
	}
	return target.toString();
}

async function failure(target: string, poolSize = 1) {
	const error = await createNapiConnection(
		target,
		poolSize,
		poolSize,
		undefined,
		{
			timeoutMs: 8000,
		},
	).catch((e: unknown) => e);
	if (!(error instanceof Error)) {
		throw new Error("the connection was expected to fail");
	}
	return error.message;
}

describeBlackGate("atlas > through BlackGate", () => {
	let proxy: AsyncDatabaseConnection;
	let backend: AsyncDatabaseConnection;

	beforeAll(async () => {
		proxy = await createNapiConnection(PROXY_URL, 1, 2);
		backend = await createNapiConnection(BACKEND_URL, 1, 1);
		await backend.execute("DELETE FROM users", []);
	});

	afterAll(async () => {
		await backend?.execute("DELETE FROM users", []);
		await proxy?.close();
		await backend?.close();
	});

	it("connects with the client certificate and authenticates to PostgreSQL", async () => {
		const rows = await proxy.query("SELECT current_user AS who", []);
		expect(rows).toEqual([{ who: "app" }]);
	});

	it("writes and reads encrypted columns in plaintext, stored encrypted", async () => {
		await proxy.execute(
			"INSERT INTO users (name, email, ssn) VALUES ($1, $2, $3)",
			["Ada", "ada@example.com", "756.1234.5678.97"],
		);

		const through = await proxy.query(
			"SELECT name, email, ssn FROM users WHERE email = $1",
			["ada@example.com"],
		);
		expect(through).toEqual([
			{ name: "Ada", email: "ada@example.com", ssn: "756.1234.5678.97" },
		]);

		const stored = await backend.query(
			"SELECT name, email, ssn FROM users WHERE name = $1",
			["Ada"],
		);
		expect(stored).toHaveLength(1);
		expect(stored[0]?.name).toBe("Ada");
		expect(stored[0]?.email).not.toBe("ada@example.com");
		expect(String(stored[0]?.email)).not.toContain("ada@");
		expect(stored[0]?.ssn).not.toBe("756.1234.5678.97");
	});

	it("is refused without a client certificate", async () => {
		expect(await failure(variant({ sslcert: null, sslkey: null }))).toMatch(
			/certificate/i,
		);
	});

	it("is refused in plaintext", async () => {
		expect(
			await failure(
				variant({
					sslmode: "disable",
					sslrootcert: null,
					sslcert: null,
					sslkey: null,
				}),
			),
		).toMatch(/28000|TLS|SSL/);
	});

	it.skipIf(!POOL_SIZE)(
		"says the proxy is at its limit when the pool asks for more sessions",
		async () => {
			// Every session of the pool holds one PostgreSQL session behind the
			// proxy, which does not share them: one more than `pool_size` waits,
			// then is refused. The pool keeps retrying that refusal and reports a
			// timeout, so the message has to name the limit itself.
			const message = await failure(PROXY_URL, POOL_SIZE + 1);
			expect(message).toContain(`its ${POOL_SIZE + 1} connection(s)`);
			expect(message).toContain("connection limit");
		},
		30_000,
	);
});
