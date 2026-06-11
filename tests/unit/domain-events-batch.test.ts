import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	beforeCreate,
	Column,
	type DomainEvent,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("de_widgets")
class DeWidget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	// Accumulate a domain event at persist time so every create path has
	// something to dispatch.
	@beforeCreate()
	static stamp(w: DeWidget): void {
		w.addDomainEvent("widget.created", { name: w.name });
	}
}

/** Mock whose RETURNING reads come back empty — enough to exercise dispatch. */
function makeDb() {
	return wrapPrepareMock({
		prepare() {
			return {
				run() {
					return { changes: 1, lastInsertRowid: 1 };
				},
				all() {
					return [];
				},
			};
		},
	});
}

describe("atlas > domain events on every persistence path", () => {
	it("create() dispatches accumulated domain events", async () => {
		const dispatched: DomainEvent[] = [];
		const repo = new BaseRepository(DeWidget, makeDb());
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		await repo.create({ id: "1", name: "a" });
		expect(dispatched.map((e) => e.name)).toEqual(["widget.created"]);
	});

	it("createMany() dispatches domain events for EVERY entity (was silently lost)", async () => {
		const dispatched: DomainEvent[] = [];
		const repo = new BaseRepository(DeWidget, makeDb());
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		await repo.createMany([
			{ id: "1", name: "a" },
			{ id: "2", name: "b" },
		]);
		// Before the fix, createMany never flushed — these two events vanished.
		expect(dispatched.map((e) => e.name)).toEqual([
			"widget.created",
			"widget.created",
		]);
		expect(dispatched.map((e) => e.data.name)).toEqual(["a", "b"]);
	});
});
