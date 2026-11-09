/**
 * PostgreSQL LISTEN / NOTIFY. The SQLite case always runs; the rest runs when
 * ATLAS_TEST_PG_URL is set.
 *
 * The listening connection is a connection of its own: with a pool of ONE,
 * listening must leave that connection free for queries, and `NOTIFY` goes
 * through it while the subscription waits.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Notification } from "../../src/adapters/listen.js";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { AtlasError } from "../../src/errors.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";

function listenOf(conn: AsyncDatabaseConnection) {
	if (!conn.listen) throw new Error("a napi connection provides listen()");
	return conn.listen;
}

/** Resolves with the first `count` notifications a handler receives. */
function collector(count: number) {
	const seen: Notification[] = [];
	let done: (value: Notification[]) => void = () => {};
	const all = new Promise<Notification[]>((resolve) => {
		done = resolve;
	});
	return {
		seen,
		all,
		handler: (_payload: string, n: Notification) => {
			seen.push(n);
			if (seen.length === count) done(seen);
		},
	};
}

/** Give a notification that should NOT arrive the time to arrive anyway. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 150));

describe("listen on a dialect without LISTEN", () => {
	it("rejects with E_ATLAS_LISTEN_UNSUPPORTED on SQLite", async () => {
		const conn = await createNapiConnection("sqlite::memory:", 1, 1);
		try {
			const error = await listenOf(conn)("jobs", () => {}).catch(
				(e: unknown) => e,
			);
			expect(error).toBeInstanceOf(AtlasError);
			expect(error).toMatchObject({ code: "E_ATLAS_LISTEN_UNSUPPORTED" });
		} finally {
			await conn.close();
		}
	});
});

(PG_URL ? describe : describe.skip)("listen / notify on PostgreSQL", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		conn = await createNapiConnection(PG_URL, 1, 1);
	});

	afterEach(async () => {
		await conn.close();
	});

	it("delivers a NOTIFY sent through the pool of one it does not hold", async () => {
		const got = collector(2);
		const stop = await listenOf(conn)("atlas_jobs", got.handler);

		await conn.execute("NOTIFY atlas_jobs, 'job-1'");
		await conn.execute("SELECT pg_notify('atlas_jobs', $1)", ["job-2"]);

		const [first, second] = await got.all;
		expect([first?.payload, second?.payload]).toEqual(["job-1", "job-2"]);
		expect(first?.channel).toBe("atlas_jobs");
		expect(first?.processId).toBeGreaterThan(0);
		await stop();
	});

	it("routes each channel to its own handlers, case kept", async () => {
		const upper = collector(1);
		const lower = collector(1);
		const listen = listenOf(conn);
		const stopUpper = await listen("Atlas.Done", upper.handler);
		const stopLower = await listen("atlas_other", lower.handler);

		await conn.execute(`NOTIFY "Atlas.Done", 'a'`);
		await conn.execute("NOTIFY atlas_other, 'b'");

		expect((await upper.all).map((n) => n.payload)).toEqual(["a"]);
		expect((await lower.all).map((n) => n.payload)).toEqual(["b"]);
		await stopUpper();
		await stopLower();
	});

	it("stops one subscription without stopping the others on the channel", async () => {
		const kept = collector(2);
		const dropped = collector(1);
		const listen = listenOf(conn);
		const stopKept = await listen("atlas_shared", kept.handler);
		const stopDropped = await listen("atlas_shared", dropped.handler);

		await conn.execute("NOTIFY atlas_shared, 'one'");
		await dropped.all;
		await stopDropped();
		await conn.execute("NOTIFY atlas_shared, 'two'");

		expect((await kept.all).map((n) => n.payload)).toEqual(["one", "two"]);
		await settle();
		expect(dropped.seen.map((n) => n.payload)).toEqual(["one"]);
		await stopKept();
	});

	it("sends UNLISTEN with the last subscription, and can listen again", async () => {
		const before = collector(1);
		const listen = listenOf(conn);
		const stop = await listen("atlas_again", before.handler);
		await stop();
		await stop(); // idempotent
		await conn.execute("NOTIFY atlas_again, 'missed'");
		await settle();
		expect(before.seen).toEqual([]);

		const after = collector(1);
		const stopAfter = await listen("atlas_again", after.handler);
		await conn.execute("NOTIFY atlas_again, 'seen'");
		expect((await after.all).map((n) => n.payload)).toEqual(["seen"]);
		await stopAfter();
	});

	it("survives a handler that throws, and reports it", async () => {
		const errors: Error[] = [];
		const got = collector(2);
		const stop = await listenOf(conn)(
			"atlas_throws",
			(payload) => {
				if (payload === "x") throw new Error(`bad ${payload}`);
			},
			{ onError: (e) => errors.push(e) },
		);
		const stopOther = await listenOf(conn)("atlas_throws", got.handler);

		await conn.execute("NOTIFY atlas_throws, 'x'");
		await conn.execute("NOTIFY atlas_throws, 'y'");

		// The other subscription got both, and so the loop outlived the throw.
		expect((await got.all).map((n) => n.payload)).toEqual(["x", "y"]);
		expect(errors.map((e) => e.message)).toEqual(["bad x"]);
		await stop();
		await stopOther();
	});

	it("reconnects and subscribes again when its connection is killed", async () => {
		const got = collector(1);
		let reconnected: () => void = () => {};
		const back = new Promise<void>((resolve) => {
			reconnected = resolve;
		});
		const stop = await listenOf(conn)("atlas_reconnect", got.handler, {
			onReconnect: () => reconnected(),
		});

		// Find the listening backend and terminate it, as a failover would.
		const [row] = await conn.query<{ pid: number }>(
			"SELECT pid FROM pg_stat_activity WHERE query = 'LISTEN \"atlas_reconnect\"'",
		);
		expect(row?.pid).toBeGreaterThan(0);
		await conn.query("SELECT pg_terminate_backend($1)", [row?.pid]);
		await back;

		await conn.execute("NOTIFY atlas_reconnect, 'after'");
		expect((await got.all).map((n) => n.payload)).toEqual(["after"]);
		await stop();
	});

	it("rejects a channel name the server would truncate", async () => {
		const error = await listenOf(conn)("x".repeat(64), () => {}).catch(
			(e: unknown) => e,
		);
		expect(error).toMatchObject({ code: "E_ATLAS_LISTEN_CHANNEL" });
	});

	it("closing the connection stops the listener", async () => {
		const got = collector(1);
		await listenOf(conn)("atlas_close", got.handler);
		const other = await createNapiConnection(PG_URL, 1, 1);
		try {
			await conn.close();
			await other.execute("NOTIFY atlas_close, 'late'");
			await settle();
			expect(got.seen).toEqual([]);
		} finally {
			await other.close();
			// afterEach closes `conn` again: closing twice must be harmless.
		}
	});
});
