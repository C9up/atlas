/**
 * `connect()` awaited the factory BEFORE marking the node open, so two callers
 * at startup each built a native pool — one then unreachable, holding its
 * connections for the life of the process.
 */
import { describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/ConnectionManager.js";

/** The first row of a result the query is expected to return at least one of. */
function firstOf<T>(rows: readonly T[]): T {
	const [row] = rows;
	if (row === undefined) throw new Error("expected at least one row");
	return row;
}




const fakeConnection = () =>
	({
		dialect: "sqlite" as const,
		query: async () => [],
		execute: async () => ({ rowsAffected: 0 }),
		close: async () => {},
		ping: async () => {},
	}) as never;

describe("atlas > concurrent connect", () => {
	it("opens one connection however many callers arrive", async () => {
		const factory = vi.fn(async () => {
			await new Promise((resolve) => setTimeout(resolve, 10));
			return fakeConnection();
		});
		const manager = new ConnectionManager(factory);
		manager.add("primary", { url: "sqlite::memory:" });

		const opened = await Promise.all(
			Array.from({ length: 6 }, () => manager.connect("primary")),
		);
		expect(factory).toHaveBeenCalledTimes(1);
		expect(new Set(opened).size).toBe(1);
	});

	it("hands a later caller the same connection", async () => {
		const manager = new ConnectionManager(async () => fakeConnection());
		manager.add("primary", { url: "sqlite::memory:" });
		const first = firstOf(await Promise.all([
			manager.connect("primary"),
			manager.connect("primary"),
		]));
		expect(await manager.connect("primary")).toBe(first);
	});

	it("lets a failed connect be retried", async () => {
		let attempt = 0;
		const manager = new ConnectionManager(async () => {
			attempt++;
			if (attempt === 1) throw new Error("ECONNREFUSED");
			return fakeConnection();
		});
		manager.add("primary", { url: "sqlite::memory:" });
		await expect(
			Promise.all([manager.connect("primary"), manager.connect("primary")]),
		).rejects.toThrow("ECONNREFUSED");
		await expect(manager.connect("primary")).resolves.toBeDefined();
	});
});
