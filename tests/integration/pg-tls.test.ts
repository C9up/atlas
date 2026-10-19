/**
 * PostgreSQL over TLS with a client certificate, against a REAL server that
 * speaks TLS only and authenticates with the certificate (`pg_hba`: `cert`).
 * Gated on ATLAS_TEST_PG_TLS_DIR, the directory `scripts/pg-tls-stack.sh`
 * filled:
 *
 *   scripts/pg-tls-stack.sh start /tmp/atlas-pg-tls
 *   ATLAS_TEST_PG_TLS_DIR=/tmp/atlas-pg-tls \
 *     pnpm exec vitest run tests/integration/pg-tls.test.ts
 *
 * The server certificate names `localhost` only, so `127.0.0.1` is the
 * name-mismatch case.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createNapiConnection } from "../../src/adapters/NapiDbAdapter.js";

const DIR = process.env.ATLAS_TEST_PG_TLS_DIR ?? "";
const describeTls = DIR ? describe : describe.skip;

function port(): number {
	const stack: unknown = JSON.parse(
		readFileSync(join(DIR, "stack.json"), "utf8"),
	);
	if (typeof stack !== "object" || stack === null || !("port" in stack)) {
		throw new Error("stack.json has no port");
	}
	return Number(stack.port);
}

function url(host: string, params: Record<string, string>): string {
	const query = new URLSearchParams(params).toString();
	return `postgres://app@${host}:${port()}/app?${query}`;
}

function certs(root = "ca.crt", cert = "client.crt", key = "client.key") {
	return {
		sslrootcert: join(DIR, root),
		sslcert: join(DIR, cert),
		sslkey: join(DIR, key),
	};
}

async function failure(target: string): Promise<string> {
	const error = await createNapiConnection(target, 1, 1, undefined, {
		timeoutMs: 3000,
	}).catch((e: unknown) => e);
	if (!(error instanceof Error))
		throw new Error("the connection was expected to fail");
	return error.message;
}

describeTls("atlas > PostgreSQL over TLS with a client certificate", () => {
	it("connects in verify-full, presenting the client certificate", async () => {
		const db = await createNapiConnection(
			url("localhost", { sslmode: "verify-full", ...certs() }),
			1,
			1,
		);
		try {
			const rows = await db.query(
				"SELECT ssl, client_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
				[],
			);
			expect(rows).toEqual([{ ssl: true, client_dn: "/CN=app" }]);
		} finally {
			await db.close();
		}
	});

	it("refuses a server whose certificate does not name the host", async () => {
		expect(
			await failure(url("127.0.0.1", { sslmode: "verify-full", ...certs() })),
		).toMatch(/not valid for name "127\.0\.0\.1"/);
	});

	it("refuses a server signed by an authority it was not given", async () => {
		expect(
			await failure(
				url("localhost", { sslmode: "verify-full", ...certs("other-ca.crt") }),
			),
		).toMatch(/UnknownIssuer/);
	});

	it("is refused by the server without a client certificate", async () => {
		expect(
			await failure(
				url("localhost", {
					sslmode: "verify-full",
					sslrootcert: join(DIR, "ca.crt"),
				}),
			),
		).toMatch(/requires a valid client certificate/);
	});

	it("is refused by the server with a certificate from another authority", async () => {
		expect(
			await failure(
				url("localhost", {
					sslmode: "verify-full",
					...certs("ca.crt", "stranger.crt", "stranger.key"),
				}),
			),
		).toMatch(/UnknownCA/);
	});

	it("is refused by the server in plaintext", async () => {
		expect(await failure(url("localhost", { sslmode: "disable" }))).toMatch(
			/no pg_hba\.conf entry/,
		);
	});

	it("names a certificate file it cannot read", async () => {
		const message = await failure(
			url("localhost", {
				sslmode: "verify-full",
				...certs("absent-ca.crt"),
			}),
		);
		expect(message).toContain("root certificate");
		expect(message).toContain(join(DIR, "absent-ca.crt"));
	});

	it("refuses an encrypted client key, saying how to decrypt it", async () => {
		const key = join(DIR, "client-encrypted.key");
		const message = await failure(
			url("localhost", {
				sslmode: "verify-full",
				...certs("ca.crt", "client.crt", "client-encrypted.key"),
			}),
		);
		expect(message).toContain(key);
		expect(message).toContain("openssl pkey");
	});
});
