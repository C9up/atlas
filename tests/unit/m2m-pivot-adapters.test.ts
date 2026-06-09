/**
 * Unit tests — `pivotColumnAdapters` on `@ManyToMany` relations.
 *
 * Verifies the encode-side contract introduced by Story 52-A3: extras passed
 * to `attach()` / `sync()` route through the per-extra-column adapter's
 * `prepare` callback before binding into the pivot INSERT, mirroring the
 * existing `@Column({ prepare })` shape on entity columns. Synchronicity is
 * enforced — a Promise-returning adapter throws a column-annotated error.
 *
 * Load-side `consume` is intentionally NOT exercised here: the
 * `$extras.pivot_<col>` projection mechanism doesn't currently exist in the
 * codebase (Story 31.8 landed only the `pivotColumns` decorator stub),
 * silently adapted per `feedback_no_drift_noise`. The adapter map's
 * `consume` callbacks lie dormant on the relation metadata until that
 * projection lands.
 */
import "reflect-metadata";
import { afterEach, describe, expect, it } from "vitest";
import type { ManyToManyRelationProxy } from "../../src/BaseEntity.js";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	ManyToMany,
	PrimaryKey,
	setAtlasDialect,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

function m2m(
	repo: BaseRepository<BaseEntity>,
	parent: BaseEntity,
	relationName: string,
): ManyToManyRelationProxy {
	const proxy = repo.relatedProxy(parent, relationName);
	if (proxy.type !== "manyToMany") {
		throw new Error(`expected manyToMany proxy, got ${proxy.type}`);
	}
	return proxy;
}

class Money {
	constructor(public readonly raw: string) {}
	toString(): string {
		return this.raw;
	}
}

const moneyAdapter = {
	prepare: (value: unknown): string | null => {
		if (value === null || value === undefined) return null;
		if (!(value instanceof Money)) {
			throw new TypeError(
				`moneyAdapter.prepare: expected Money, got ${typeof value}`,
			);
		}
		return value.raw;
	},
	consume: (raw: unknown): Money | null => {
		if (raw === null || raw === undefined) return null;
		return new Money(String(raw));
	},
};

@Entity("orders")
class Order extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare label: string;
}

@Entity("users")
class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;

	@ManyToMany(() => Order, {
		pivotTable: "users_orders",
		pivotColumns: ["amount"],
		pivotColumnAdapters: { amount: moneyAdapter },
	})
	declare orders: Order[];
}

@Entity("plain_users")
class PlainUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;

	@ManyToMany(() => Order, {
		pivotTable: "plain_users_orders",
	})
	declare orders: Order[];
}

@Entity("uuid_tags")
class UuidTag extends BaseEntity {
	@PrimaryKey({ generated: "uuid" }) declare id: string;
	@Column() declare label: string;
}

@Entity("uuid_accounts")
class UuidAccount extends BaseEntity {
	@PrimaryKey({ generated: "uuid" }) declare id: string;
	@Column() declare name: string;

	@ManyToMany(() => UuidTag, { pivotTable: "uuid_accounts_tags" })
	declare tags: UuidTag[];
}

@Entity("timestamped_users")
class TimestampedUser extends BaseEntity {
	@PrimaryKey() declare id: number;

	@ManyToMany(() => Order, {
		pivotTable: "timestamped_users_orders",
		pivotTimestamps: true,
		pivotColumnAdapters: {
			created_at: { prepare: () => "SHOULD_NOT_RUN" },
		},
	})
	declare orders: Order[];
}

interface Captured {
	sql: string;
	params: unknown[];
}

function capturingDb() {
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
					return undefined;
				},
				all: (...params: unknown[]) => {
					captured.push({ sql, params });
					return [];
				},
			};
		},
	};
}

afterEach(() => {
	setAtlasDialect("sqlite");
});

describe("atlas > @ManyToMany pivot Postgres casts", () => {
	it("casts both pivot FK columns to ::uuid on Postgres (sqlx text-bind fix)", async () => {
		setAtlasDialect("postgres");
		const db = capturingDb();
		const repo = new BaseRepository(UuidAccount, wrapPrepareMock(db));
		const account = new UuidAccount();
		account.id = "0191aaaa-acc";

		await m2m(repo, account, "tags").attach(["0191bbbb-tag"]);

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"uuid_accounts_tags"/i.test(c.sql),
		);
		expect(insert).toBeDefined();
		// Both FK columns reference uuid PKs (parent + related) → both cast.
		expect((insert?.sql.match(/::uuid/g) ?? []).length).toBe(2);
	});

	it("emits no casts for the same pivot on SQLite (driver coerces)", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(UuidAccount, wrapPrepareMock(db));
		const account = new UuidAccount();
		account.id = "0191aaaa-acc";

		await m2m(repo, account, "tags").attach(["0191bbbb-tag"]);

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"uuid_accounts_tags"/i.test(c.sql),
		);
		expect(insert?.sql).not.toContain("::");
	});
});

describe("atlas > @ManyToMany pivotColumnAdapters > encode", () => {
	it("routes an attach() extra through the adapter's prepare callback", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(User, wrapPrepareMock(db));
		const user = new User();
		user.id = 1;
		user.name = "Alice";

		await m2m(repo, user, "orders").attach({
			42: { amount: new Money("1234567890123456.789") },
		});

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"users_orders"/i.test(c.sql),
		);
		expect(insert).toBeDefined();
		// The Money instance MUST have been encoded to its raw string before
		// reaching the bind layer — otherwise the driver would either stringify
		// to `[object Object]` (postgres pg) or reject the bind (mysql2).
		expect(insert?.params).toContain("1234567890123456.789");
		for (const p of insert?.params ?? []) {
			expect(p).not.toBeInstanceOf(Money);
		}
	});

	it("annotates a prepare() throw with the pivot extra-column key", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(User, wrapPrepareMock(db));
		const user = new User();
		user.id = 2;
		user.name = "Bob";

		let caught: unknown;
		try {
			await m2m(repo, user, "orders").attach({
				7: { amount: "not-a-Money" },
			});
		} catch (err) {
			caught = err;
		}
		if (!(caught instanceof Error)) {
			throw new Error("expected attach() to throw an Error");
		}
		expect(caught.message).toMatch(/@Column\.prepare threw on 'amount'/);
		expect(Reflect.get(caught, "cause")).toBeInstanceOf(TypeError);
		// Stack header parity with column-prepare-consume.test.ts: the wrapped
		// stack starts with the column-annotated message, the cause's stack is
		// preserved underneath via Error.cause.
		expect(caught.stack ?? "").toMatch(
			/^Error: @Column\.prepare threw on 'amount'/,
		);
	});

	it("rejects a prepare() that returns a Promise (sync-contract)", async () => {
		setAtlasDialect("sqlite");

		@Entity("async_users")
		class AsyncUser extends BaseEntity {
			@PrimaryKey() declare id: number;
			@ManyToMany(() => Order, {
				pivotTable: "async_users_orders",
				pivotColumnAdapters: {
					amount: { prepare: (_v: unknown) => Promise.resolve("eventually") },
				},
			})
			declare orders: Order[];
		}

		const db = capturingDb();
		const repo = new BaseRepository(AsyncUser, wrapPrepareMock(db));
		const u = new AsyncUser();
		u.id = 3;

		await expect(
			m2m(repo, u, "orders").attach({ 9: { amount: "x" } }),
		).rejects.toThrow(/prepare on 'amount' returned a Promise/);
	});

	it("leaves unadapted extras as-is (no-adapter regression pin)", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(PlainUser, wrapPrepareMock(db));
		const u = new PlainUser();
		u.id = 4;
		u.name = "Carol";

		// extras with no declared adapter — value reaches the bind without
		// running through any adapter callback (the Rust compiler may serialize
		// it on the way through, but the *value shape* is preserved).
		await m2m(repo, u, "orders").attach({
			11: { meta: "raw-string-not-touched" },
		});

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"plain_users_orders"/i.test(c.sql),
		);
		expect(insert).toBeDefined();
		// AC5 contract: exact param order (parent_fk, other_fk, meta), no
		// reordering, exactly 3 params for a single-row attach with one extra.
		// (Object key '11' arrives as a string — JS coerces numeric object keys
		// to strings; that's the documented call shape, not a bug.)
		expect(insert?.params).toEqual([4, "11", "raw-string-not-touched"]);
	});

	it("emits a no-extras INSERT with exactly 2 columns when attach() is given the array form", async () => {
		// AC5 contract: the no-adapter, no-extras code path is unchanged by
		// this story. Array-form attach() produces an INSERT with exactly
		// 2 columns per row (FK + other-FK), zero extras, zero adapter calls.
		// Regression pin against any future refactor that accidentally widens
		// the pivot row shape.
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(PlainUser, wrapPrepareMock(db));
		const u = new PlainUser();
		u.id = 4;
		u.name = "Carol";

		await m2m(repo, u, "orders").attach([10, 20, 30]);

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"plain_users_orders"/i.test(c.sql),
		);
		expect(insert).toBeDefined();
		// 3 rows × 2 columns (FK + other-FK) = 6 params, in row-major order.
		expect(insert?.params).toEqual([4, 10, 4, 20, 4, 30]);
		// The column list mentions only the two FK columns — no extras leaked.
		expect(insert?.sql).toMatch(/"plain_user_id"\s*,\s*"order_id"/i);
	});

	it("invokes prepare(null) when an extra is explicitly null", async () => {
		// Pins the documented null-handling contract: adapters MUST be null-safe
		// because atlas calls them unconditionally for every value, including
		// null (and null-coerced undefined / back-filled missing keys).
		setAtlasDialect("sqlite");

		const seen: unknown[] = [];
		const recordingAdapter = {
			prepare: (value: unknown): unknown => {
				seen.push(value);
				return value;
			},
		};

		@Entity("nullable_users")
		class NullableUser extends BaseEntity {
			@PrimaryKey() declare id: number;
			@ManyToMany(() => Order, {
				pivotTable: "nullable_users_orders",
				pivotColumnAdapters: { amount: recordingAdapter },
			})
			declare orders: Order[];
		}

		const db = capturingDb();
		const repo = new BaseRepository(NullableUser, wrapPrepareMock(db));
		const u = new NullableUser();
		u.id = 13;

		await m2m(repo, u, "orders").attach({ 71: { amount: null } });

		expect(seen).toEqual([null]);
		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"nullable_users_orders"/i.test(c.sql),
		);
		expect(insert?.params).toEqual([13, "71", null]);
	});

	it("does NOT route pivotTimestamps values through pivotColumnAdapters", async () => {
		// Pins the documented anti-pattern (spec line 198): timestamps are
		// managed by atlas and bypass any user adapter — even when the user
		// declares an adapter keyed by the timestamp column name. The collision
		// guard at attach() catches the user-side path; this test pins the
		// internal side (atlas-emitted ISO string lands raw in the bind, not
		// the adapter's output).
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(TimestampedUser, wrapPrepareMock(db));
		const u = new TimestampedUser();
		u.id = 21;

		await m2m(repo, u, "orders").attach([55]);

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"timestamped_users_orders"/i.test(c.sql),
		);
		expect(insert).toBeDefined();
		expect(insert?.params).not.toContain("SHOULD_NOT_RUN");
		// The timestamp params are real ISO date strings; assert shape, not
		// exact value (Date.now() drift between test setup and the call).
		const isoLike = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
		const isoCount = (insert?.params ?? []).filter(
			(p) => typeof p === "string" && isoLike.test(p),
		).length;
		expect(isoCount).toBe(2);
	});

	it("rejects an extras key colliding with a pivotTimestamps column", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(TimestampedUser, wrapPrepareMock(db));
		const u = new TimestampedUser();
		u.id = 22;

		await expect(
			m2m(repo, u, "orders").attach({
				56: { created_at: "user-supplied-timestamp" },
			}),
		).rejects.toThrow(/collides with a pivotTimestamps column/);
	});

	it("rejects an extras key colliding with the pivot foreignKey column", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(PlainUser, wrapPrepareMock(db));
		const u = new PlainUser();
		u.id = 23;
		u.name = "Erin";

		await expect(
			m2m(repo, u, "orders").attach({
				57: { plain_user_id: 999 },
			}),
		).rejects.toThrow(/collides with the foreignKey column/);
	});

	it("propagates the adapter through sync() via its internal attach() call", async () => {
		setAtlasDialect("sqlite");
		const db = capturingDb();
		const repo = new BaseRepository(User, wrapPrepareMock(db));
		const user = new User();
		user.id = 5;
		user.name = "Dave";

		// `sync({...}, true)` (additive mode) skips the diff/detach path and
		// goes straight through `attach()` — so we exercise the encode path.
		await m2m(repo, user, "orders").sync(
			{ 99: { amount: new Money("0.01") } },
			true,
		);

		const insert = db.captured.find((c) =>
			/INSERT\s+INTO\s+"users_orders"/i.test(c.sql),
		);
		expect(insert).toBeDefined();
		expect(insert?.params).toContain("0.01");
		for (const p of insert?.params ?? []) {
			expect(p).not.toBeInstanceOf(Money);
		}
	});
});
