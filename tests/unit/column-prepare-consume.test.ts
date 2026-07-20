import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

class StubBox {
	constructor(public readonly raw: string) {}
}

const stubAdapter = {
	consume: (raw: unknown): StubBox | null => {
		if (raw === null || raw === undefined) return null;
		return new StubBox(String(raw));
	},
	prepare: (value: unknown): string | null => {
		if (value === null || value === undefined) return null;
		if (!(value instanceof StubBox)) {
			throw new TypeError(
				`stubAdapter.prepare: expected StubBox, got ${typeof value}`,
			);
		}
		return value.raw;
	},
};

@Entity("items")
class Item extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column(stubAdapter) declare payload: StubBox | null;
	@Column() declare name: string;
}

@Entity("plain_things")
class PlainThing extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
}

function rowDb(rows: Record<string, unknown>[]) {
	return {
		prepare(_sql: string) {
			return {
				run: () => ({ changes: 1, lastInsertRowid: 1 }),
				get: () => rows[0],
				all: () => rows,
			};
		},
	};
}

interface Captured {
	sql: string;
	params: unknown[];
}

function capturingDb(
	getRow: (sql: string) => Record<string, unknown> | undefined,
) {
	const captured: Captured[] = [];
	return {
		captured,
		prepare(sql: string) {
			return {
				run: (...params: unknown[]) => {
					captured.push({ sql, params });
					return { changes: 1, lastInsertRowid: 1 };
				},
				get: (...params: unknown[]) => {
					captured.push({ sql, params });
					return getRow(sql);
				},
				all: (...params: unknown[]) => {
					captured.push({ sql, params });
					const r = getRow(sql);
					return r ? [r] : [];
				},
			};
		},
	};
}

describe("atlas > BaseRepository#hydrate consume", () => {
	it("applies @Column({ consume }) on hydrate", async () => {
		const db = rowDb([{ id: 1, payload: "hello", name: "item-a" }]);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		const found = await repo.find(1);
		expect(found).not.toBeNull();
		expect(found?.payload).toBeInstanceOf(StubBox);
		expect(found?.payload?.raw).toBe("hello");
		// Untagged column passes through untouched.
		expect(found?.name).toBe("item-a");
	});

	it("delegates null-handling to the consume callback (Adonis contract)", async () => {
		// The user's consume is responsible for null behavior — atlas does NOT
		// short-circuit on null. stubAdapter.consume(null) returns null.
		const db = rowDb([{ id: 2, payload: null, name: "item-b" }]);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		const found = await repo.find(2);
		expect(found?.payload).toBeNull();
		expect(found?.name).toBe("item-b");
	});

	it("does not wrap entities without any prepare/consume column", async () => {
		const db = rowDb([{ id: 7, label: "plain" }]);
		const repo = new BaseRepository(PlainThing, wrapPrepareMock(db));
		const found = await repo.find(7);
		expect(found?.label).toBe("plain");
	});

	it("a plain @Column() without prepare/consume round-trips values as-is (identity)", async () => {
		// Defense-in-depth: even a value that LOOKS like it could be transformed
		// (a string that could be coerced to a number, an object) should reach
		// the entity untouched when no callback is wired.
		const sentinel = { id: 9, label: "identity-test-payload" };
		const db = rowDb([sentinel]);
		const repo = new BaseRepository(PlainThing, wrapPrepareMock(db));
		const found = await repo.find(9);
		// Same string reference (no clone, no wrap, no coerce).
		expect(found?.label).toBe("identity-test-payload");
		// Sanity: the entity carries the exact id we put in the row.
		expect(found?.id).toBe(9);
	});
});

describe("atlas > BaseRepository prepare (write paths)", () => {
	it("emits prepared value as the bind parameter on INSERT (via repo.create)", async () => {
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		await repo.create({ payload: new StubBox("xyz"), name: "first" });

		const insert = db.captured.find((c) => /INSERT/i.test(c.sql));
		expect(insert).toBeDefined();
		expect(insert?.params).toContain("xyz");
		for (const p of insert?.params ?? []) {
			expect(p).not.toBeInstanceOf(StubBox);
		}
	});

	it("emits prepared value as the bind parameter on UPDATE (via repo.save)", async () => {
		const existing = { id: 1, payload: "old", name: "first" };
		const db = capturingDb((sql) =>
			/^SELECT/i.test(sql) ? existing : undefined,
		);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));

		const entity = await repo.find(1);
		expect(entity?.payload).toBeInstanceOf(StubBox);
		if (!entity) throw new Error("precondition: entity must be hydrated");

		entity.payload = new StubBox("new-value");
		db.captured.length = 0;
		await repo.save(entity);

		const update = db.captured.find((c) => /UPDATE/i.test(c.sql));
		expect(update).toBeDefined();
		expect(update?.params).toContain("new-value");
		for (const p of update?.params ?? []) {
			expect(p).not.toBeInstanceOf(StubBox);
		}
	});

	it("delegates null-handling to the prepare callback on INSERT", async () => {
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		await repo.create({ payload: null, name: "second" });

		const insert = db.captured.find((c) => /INSERT/i.test(c.sql));
		expect(insert).toBeDefined();
		// stubAdapter.prepare(null) returns null, so the bind is null.
		expect(insert?.params).toContain(null);
	});

	it("does not transform untagged columns", async () => {
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		await repo.create({ payload: new StubBox("y"), name: "plain-string" });
		const insert = db.captured.find((c) => /INSERT/i.test(c.sql));
		expect(insert?.params).toContain("plain-string");
	});

	it("applies prepare through #plainToRowPairs (upsert path)", async () => {
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		await repo.upsert(
			{ id: 1, payload: new StubBox("upserted"), name: "u" },
			["id"],
			["payload", "name"],
		);

		const upsert = db.captured.find((c) =>
			/INSERT|ON CONFLICT|ON DUPLICATE/i.test(c.sql),
		);
		expect(upsert).toBeDefined();
		expect(upsert?.params).toContain("upserted");
		for (const p of upsert?.params ?? []) {
			expect(p).not.toBeInstanceOf(StubBox);
		}
	});

	it("applies prepare through #buildSetPairs (updateById path)", async () => {
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		await repo.updateById(1, { payload: new StubBox("via-updateById") });

		const update = db.captured.find((c) => /UPDATE/i.test(c.sql));
		expect(update).toBeDefined();
		expect(update?.params).toContain("via-updateById");
		for (const p of update?.params ?? []) {
			expect(p).not.toBeInstanceOf(StubBox);
		}
	});

	it("throws via the prepare callback when given the wrong value type, annotated with the column key", async () => {
		// The user's prepare guards the type; atlas surfaces the throw on save
		// wrapped in a column-context error (deferred-work 35-10-2-A4) — the
		// original adapter exception is preserved on `cause`.
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		let caught: unknown;
		try {
			await repo.create({ payload: "not-a-StubBox", name: "bad" });
		} catch (err) {
			caught = err;
		}
		expect(caught).toBeInstanceOf(Error);
		if (!(caught instanceof Error)) throw caught;
		expect(caught.message).toMatch(/@Column\.prepare threw on 'payload'/);
		expect(Reflect.get(caught, "cause")).toBeInstanceOf(TypeError);
	});

	// 35-10-2-A4 — adapter consume throws are wrapped with the column key too.
	it("annotates a consume callback throw with the column key on hydrate", async () => {
		const angryAdapter = {
			consume: (_raw: unknown): StubBox => {
				throw new RangeError("consume blew up");
			},
			prepare: (value: unknown): string | null =>
				value instanceof StubBox ? value.raw : null,
		};
		@Entity("angry_items")
		class AngryItem extends BaseEntity {
			@PrimaryKey() declare id: number;
			@Column(angryAdapter) declare payload: StubBox;
		}
		const db = rowDb([{ id: 1, payload: "whatever" }]);
		const repo = new BaseRepository(AngryItem, wrapPrepareMock(db));
		await expect(repo.find(1)).rejects.toThrow(
			/@Column\.consume threw on 'payload'/,
		);
		try {
			await repo.find(1);
		} catch (err) {
			expect(Reflect.get(err as object, "cause")).toBeInstanceOf(RangeError);
		}
	});

	// 35-10-1-A1 — non-`id` camelCase PK no longer double-writes the row dict.
	it("does not double-emit a non-`id` camelCase primary key into the INSERT row", async () => {
		@Entity("user_sessions")
		class UserSession extends BaseEntity {
			@PrimaryKey() declare userId: string;
			@Column() declare label: string;
		}
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(UserSession, wrapPrepareMock(db));
		await repo.create({ userId: "u-1", label: "home" });
		const insert = db.captured.find((c) => /^\s*INSERT/i.test(c.sql));
		expect(insert).toBeDefined();
		// The row dict is rendered into a column list — we assert by inspecting
		// the SQL surface itself: only the snake_case `user_id` should appear,
		// never the raw camelCase `userId` token.
		expect(insert?.sql).toMatch(/user_id/);
		expect(insert?.sql).not.toMatch(/"userId"/);
	});

	// Re-snapshot when every dirty entry is `undefined`: without this the
	// entity stays dirty forever and the next save() loops on a no-op.
	it("clears $dirty after a save where all dirty entries are undefined", async () => {
		const existing = { id: 1, name: "before" };
		const db = capturingDb((sql) =>
			/^SELECT/i.test(sql) ? existing : undefined,
		);
		const repo = new BaseRepository(PlainThing, wrapPrepareMock(db));
		const entity = await repo.find(1);
		expect(entity).not.toBeNull();
		if (!entity) throw new Error("precondition: entity must be hydrated");
		// Clear the captured SELECT so we can assert no UPDATE was emitted.
		db.captured.length = 0;
		// Explicit undefined assignment — Adonis convention treats it as "no
		// change", which under the diff's contract means SET pairs are empty.
		entity.setProp("label", undefined);
		await repo.save(entity);
		const update = db.captured.find((c) => /^\s*UPDATE/i.test(c.sql));
		expect(update).toBeUndefined();
		// Entity must NOT remain dirty — re-snapshot fired despite no SQL.
		expect(entity.isDirty()).toBe(false);
	});

	// Async (Promise-returning) adapter is rejected synchronously with a
	// column-annotated error rather than landing as a Promise on the bind list.
	it("throws synchronously when @Column.prepare returns a Promise", async () => {
		const asyncAdapter = {
			consume: (raw: unknown): StubBox | null =>
				raw === null ? null : new StubBox(String(raw)),
			prepare: (_value: unknown): Promise<string> =>
				Promise.resolve("eventually"),
		};
		@Entity("async_items")
		class AsyncItem extends BaseEntity {
			@PrimaryKey() declare id: number;
			@Column(asyncAdapter) declare payload: StubBox;
		}
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(AsyncItem, wrapPrepareMock(db));
		await expect(repo.create({ payload: new StubBox("x") })).rejects.toThrow(
			/prepare on 'payload' returned a Promise/,
		);
	});

	// wrapAdapterError: the wrapped Error keeps its own `stack` (pointing at
	// the wrap site) so default `console.error(err)` shows the column-
	// annotated header rather than the raw original.
	it("wrapAdapterError keeps the wrap-site stack and surfaces the column header", async () => {
		const db = capturingDb(() => undefined);
		const repo = new BaseRepository(Item, wrapPrepareMock(db));
		let caught: unknown;
		try {
			await repo.create({ payload: "not-a-StubBox", name: "bad" });
		} catch (err) {
			caught = err;
		}
		expect(caught).toBeInstanceOf(Error);
		if (!(caught instanceof Error)) throw caught;
		// The stack's first line carries the wrap message — what `console.error`
		// and most error reporters print as the headline.
		expect(caught.stack).toMatch(/^Error: @Column\.prepare threw on 'payload'/);
	});
});

describe("atlas > column adapters receive (value, attribute, model) — Lucid parity", () => {
	interface AdapterCall {
		value: unknown;
		attribute: string | undefined;
		model: unknown;
	}
	const prepareCalls: AdapterCall[] = [];
	const consumeCalls: AdapterCall[] = [];
	const serializeCalls: AdapterCall[] = [];

	@Entity("widgets")
	class Widget extends BaseEntity {
		@PrimaryKey() declare id: number;
		@Column({
			prepare: (value, attribute, model) => {
				prepareCalls.push({ value, attribute, model });
				return value;
			},
			consume: (value, attribute, model) => {
				consumeCalls.push({ value, attribute, model });
				return value;
			},
			serialize: (value, attribute, model) => {
				serializeCalls.push({ value, attribute, model });
				return value;
			},
		})
		declare label: string;
	}

	@Entity("legacy_widgets")
	class LegacyWidget extends BaseEntity {
		@PrimaryKey() declare id: number;
		@Column({ consume: (value) => `seen:${String(value)}` })
		declare label: string;
	}

	it("consume on hydrate gets the property name and the entity instance", async () => {
		consumeCalls.length = 0;
		const db = rowDb([{ id: 1, label: "x" }]);
		const repo = new BaseRepository(Widget, wrapPrepareMock(db));
		const found = await repo.find(1);

		const call = consumeCalls.find((c) => c.value === "x");
		expect(call?.attribute).toBe("label");
		expect(call?.model).toBeInstanceOf(Widget);
		expect(call?.model).toBe(found);
	});

	it("prepare on create gets the property name and the persisted entity", async () => {
		prepareCalls.length = 0;
		const db = capturingDb(() => ({ id: 2, label: "y" }));
		const repo = new BaseRepository(Widget, wrapPrepareMock(db));
		await repo.create({ label: "y" });

		const call = prepareCalls.find((c) => c.value === "y");
		expect(call?.attribute).toBe("label");
		expect(call?.model).toBeInstanceOf(Widget);
	});

	it("serialize on toJSON gets the property name and the model", async () => {
		serializeCalls.length = 0;
		const db = rowDb([{ id: 3, label: "z" }]);
		const repo = new BaseRepository(Widget, wrapPrepareMock(db));
		const found = await repo.find(3);
		found?.toJSON();

		const call = serializeCalls.find((c) => c.value === "z");
		expect(call?.attribute).toBe("label");
		expect(call?.model).toBe(found);
	});

	it("a one-argument adapter still works unchanged (backward compatible)", async () => {
		const db = rowDb([{ id: 1, label: "x" }]);
		const repo = new BaseRepository(LegacyWidget, wrapPrepareMock(db));
		const found = await repo.find(1);
		expect(found?.label).toBe("seen:x");
	});
});
