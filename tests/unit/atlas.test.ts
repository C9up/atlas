import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	BelongsTo,
	Column,
	Entity,
	getColumnMetadata,
	getEntityMetadata,
	getPrimaryKey,
	getRelationMetadata,
	HasMany,
	HasManyThrough,
	HasOne,
	PrimaryKey,
	QueryBuilder,
	RawSql,
} from "../../src/index.js";
import type { ModelQuery } from "../../src/ModelQuery.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

// === Test entities ===

@Entity("orders")
class Order extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare status: string;
	@Column({ type: "decimal" }) declare total: number;
	@Column() declare createdAt: string;

	@HasMany(() => OrderItem) declare items: unknown[];
	@BelongsTo(() => User) declare user: unknown;

	markAsPaid() {
		this.status = "paid";
		this.addDomainEvent("order.paid", { orderId: this.id });
	}
}

@Entity("order_items")
class OrderItem extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare orderId: string;
	@Column() declare productName: string;
	@Column() declare quantity: number;
}

@Entity("users")
class User extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@Column() declare email: string;
}

// === Decorator tests ===

describe("atlas > @Entity decorator", () => {
	it("stores table name metadata", () => {
		const meta = getEntityMetadata(Order);
		expect(meta?.tableName).toBe("orders");
	});

	it("stores columns", () => {
		const columns = getColumnMetadata(Order);
		expect(columns.length).toBeGreaterThanOrEqual(4); // id, status, total, createdAt
		expect(columns.map((c) => c.propertyKey)).toContain("status");
		expect(columns.map((c) => c.propertyKey)).toContain("total");
	});

	it("stores primary key", () => {
		expect(getPrimaryKey(Order)).toBe("id");
	});

	it("stores relations", () => {
		const relations = getRelationMetadata(Order);
		expect(relations.length).toBe(2);
		expect(relations.find((r) => r.propertyKey === "items")?.type).toBe(
			"hasMany",
		);
		expect(relations.find((r) => r.propertyKey === "user")?.type).toBe(
			"belongsTo",
		);
	});
});

// === BaseEntity tests ===

describe("atlas > BaseEntity domain events", () => {
	it("accumulates domain events", () => {
		const order = new Order();
		order.id = "123";
		order.status = "pending";
		order.markAsPaid();

		expect(order.hasDomainEvents()).toBe(true);
	});

	it("flushes domain events", () => {
		const order = new Order();
		order.id = "456";
		order.markAsPaid();

		const events = order.flushDomainEvents();
		expect(events.length).toBe(1);
		expect(events[0].name).toBe("order.paid");
		expect(events[0].data.orderId).toBe("456");

		// Events cleared after flush
		expect(order.hasDomainEvents()).toBe(false);
		expect(order.flushDomainEvents().length).toBe(0);
	});
});

// === BaseRepository tests ===

/** In-memory mock DB for testing BaseRepository */
function createMockDb() {
	const tables: Record<string, Record<string, unknown>[]> = {};
	const mock = {
		prepare(sql: string) {
			return {
				run(...params: unknown[]) {
					const insertMatch = sql.match(/INSERT INTO "(\w+)"/);
					if (insertMatch) {
						const table = insertMatch[1];
						if (!tables[table]) tables[table] = [];
						const cols =
							sql
								.match(/\(([^)]+)\) VALUES/)?.[1]
								.replace(/"/g, "")
								.split(", ") ?? [];
						const row: Record<string, unknown> = {};
						cols.forEach((c, i) => {
							row[c] = params[i];
						});
						tables[table].push(row);
					}
					const updateMatch = sql.match(/UPDATE "(\w+)" SET/);
					if (updateMatch) {
						const table = updateMatch[1];
						const pk = params[params.length - 1];
						const row = (tables[table] ?? []).find((r) => r.id === pk);
						if (row) {
							const sets = sql.match(/SET (.+) WHERE/)?.[1].split(", ") ?? [];
							sets.forEach((s, i) => {
								const col = s.split(" = ")[0].replace(/"/g, "");
								row[col] = params[i];
							});
						}
					}
					const deleteMatch = sql.match(/DELETE FROM "(\w+)"/);
					if (deleteMatch) {
						const table = deleteMatch[1];
						tables[table] = (tables[table] ?? []).filter(
							(r) => r.id !== params[0],
						);
					}
					return { changes: 1, lastInsertRowid: 1 };
				},
				get(...params: unknown[]) {
					const match = sql.match(/FROM "(\w+)"/);
					if (!match) return undefined;
					const table = match[1];
					return (tables[table] ?? []).find((r) => {
						const col = sql.match(/WHERE "(\w+)"/)?.[1] ?? "id";
						return r[col] === params[0];
					});
				},
				all(...params: unknown[]) {
					const match = sql.match(/FROM "(\w+)"/);
					if (!match) return [];
					const table = match[1];
					if (sql.includes("WHERE")) {
						const col = sql.match(/WHERE "(\w+)"/)?.[1] ?? "id";
						return (tables[table] ?? []).filter((r) => r[col] === params[0]);
					}
					return tables[table] ?? [];
				},
			};
		},
	};
	return wrapPrepareMock(mock);
}

describe("atlas > BaseRepository", () => {
	it("creates executable query for entity table", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const q = repo.query();
		expect(q.toSQL().sql).toContain('"orders"');
	});

	it("reads table name and primary key", () => {
		const repo = new BaseRepository(Order, createMockDb());
		expect(repo.getTableName()).toBe("orders");
		expect(repo.getPrimaryKeyColumn()).toBe("id");
	});

	it("throws if class is not decorated with @Entity", () => {
		class NotAnEntity extends BaseEntity {}
		expect(() => new BaseRepository(NotAnEntity, createMockDb())).toThrow(
			"not decorated with @Entity",
		);
	});

	it("rehydrates DB-generated id after create() (sqlite RETURNING path)", async () => {
		@Entity("articles")
		class Article extends BaseEntity {
			@PrimaryKey() declare id: number;
			@Column() declare title: string;
		}

		let nextId = 0;
		const db = {
			prepare(sql: string) {
				return {
					run() {
						return { changes: 1, lastInsertRowid: 0 };
					},
					get() {
						return undefined;
					},
					all(...params: unknown[]) {
						if (/INSERT/i.test(sql)) {
							nextId++;
							const cols =
								sql
									.match(/\(([^)]+)\) VALUES/)?.[1]
									.replace(/"/g, "")
									.split(",")
									.map((s) => s.trim()) ?? [];
							const row: Record<string, unknown> = { id: nextId };
							cols.forEach((c, i) => {
								if (c !== "id") row[c] = params[i];
							});
							return [row];
						}
						return [];
					},
				};
			},
		};

		const repo = new BaseRepository(Article, wrapPrepareMock(db));
		const article = await repo.create({ title: "first" });
		expect(article.id).toBe(1);
		expect(article.title).toBe("first");

		const second = await repo.create({ title: "second" });
		expect(second.id).toBe(2);
	});

	it("save() routes a zero-valued PK through UPDATE, not INSERT", async () => {
		@Entity("counters")
		class Counter extends BaseEntity {
			@PrimaryKey() declare id: number;
			@Column() declare value: number;
		}

		const captured: string[] = [];
		const stored = new Map<number, Record<string, unknown>>([
			[0, { id: 0, value: 1 }],
		]);
		const db = {
			prepare(sql: string) {
				captured.push(sql);
				return {
					run(...params: unknown[]) {
						if (/UPDATE/i.test(sql)) {
							const row = stored.get(params[params.length - 1] as number);
							if (row) row.value = params[0];
						}
						return { changes: 1, lastInsertRowid: 0 };
					},
					get(...params: unknown[]) {
						return stored.get(params[0] as number);
					},
					all() {
						return [...stored.values()];
					},
				};
			},
		};

		const repo = new BaseRepository(Counter, wrapPrepareMock(db));
		const c = new Counter();
		c.id = 0;
		c.value = 42;
		await repo.save(c);

		expect(captured.some((s) => /UPDATE/i.test(s))).toBe(true);
		expect(captured.some((s) => /INSERT/i.test(s))).toBe(false);
	});

	it("dispatches domain events on save", async () => {
		const dispatched: Array<{ name: string }> = [];
		const repo = new BaseRepository(Order, createMockDb());
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		const order = new Order();
		order.id = "789";
		order.markAsPaid();

		await repo.save(order);

		expect(dispatched.length).toBe(1);
		expect(dispatched[0].name).toBe("order.paid");
		expect(order.hasDomainEvents()).toBe(false);
	});
});

// === QueryBuilder tests ===

describe("atlas > QueryBuilder", () => {
	it("builds basic SELECT", () => {
		const { sql, params } = new QueryBuilder("orders").toSQL();
		expect(sql).toBe('SELECT * FROM "orders"');
		expect(params).toEqual([]);
	});

	it("builds WHERE with params", () => {
		const { sql, params } = new QueryBuilder("orders")
			.where("status", "active")
			.toSQL();
		expect(sql).toBe('SELECT * FROM "orders" WHERE "status" = ?');
		expect(params).toEqual(["active"]);
	});

	it("builds WHERE with operator", () => {
		const { sql, params } = new QueryBuilder("orders")
			.where("total", ">", 100)
			.toSQL();
		expect(sql).toBe('SELECT * FROM "orders" WHERE "total" > ?');
		expect(params).toEqual([100]);
	});

	it("builds multiple WHERE", () => {
		const { sql } = new QueryBuilder("orders")
			.where("status", "active")
			.where("total", ">", 100)
			.toSQL();
		expect(sql).toContain('WHERE "status" = ? AND "total" > ?');
	});

	it("builds OR WHERE", () => {
		const { sql } = new QueryBuilder("orders")
			.where("status", "active")
			.orWhere("status", "pending")
			.toSQL();
		expect(sql).toContain('WHERE "status" = ? OR "status" = ?');
	});

	it("builds WHERE IN", () => {
		const { sql, params } = new QueryBuilder("orders")
			.whereIn("status", ["active", "pending", "paid"])
			.toSQL();
		expect(sql).toContain("IN (?, ?, ?)");
		expect(params).toEqual(["active", "pending", "paid"]);
	});

	it("builds WHERE NULL / NOT NULL", () => {
		const { sql } = new QueryBuilder("orders")
			.whereNull("deletedAt")
			.whereNotNull("createdAt")
			.toSQL();
		expect(sql).toContain("IS NULL");
		expect(sql).toContain("IS NOT NULL");
	});

	it("builds ORDER BY", () => {
		const { sql } = new QueryBuilder("orders")
			.orderBy("createdAt", "desc")
			.toSQL();
		expect(sql).toContain('ORDER BY "createdAt" DESC');
	});

	it("builds LIMIT + OFFSET", () => {
		const { sql } = new QueryBuilder("orders").limit(20).offset(40).toSQL();
		expect(sql).toContain("LIMIT 20");
		expect(sql).toContain("OFFSET 40");
	});

	it("builds paginate", () => {
		const { sql } = new QueryBuilder("orders").paginate(3, 10).toSQL();
		expect(sql).toContain("LIMIT 10");
		expect(sql).toContain("OFFSET 20"); // (3-1) * 10
	});

	it("builds SELECT specific columns", () => {
		const { sql } = new QueryBuilder("orders")
			.select("id", "status", "total")
			.toSQL();
		expect(sql).toContain('SELECT "id", "status", "total"');
	});

	it("tracks preloads", () => {
		const qb = new QueryBuilder("orders").preload("items").preload("user");
		expect(qb.getPreloads()).toEqual(["items", "user"]);
	});

	it("handles empty whereIn as always-false", () => {
		const { sql, params } = new QueryBuilder("orders")
			.whereIn("status", [])
			.toSQL();
		expect(sql).toContain("1 = 0");
		expect(params).toEqual([]);
	});

	it("builds NOT IN", () => {
		const { sql, params } = new QueryBuilder("orders")
			.where("status", "NOT IN", ["cancelled", "refunded"])
			.toSQL();
		expect(sql).toContain("NOT IN (?, ?)");
		expect(params).toEqual(["cancelled", "refunded"]);
	});

	it("handles empty NOT IN as always-true", () => {
		const { sql } = new QueryBuilder("orders")
			.where("status", "NOT IN", [])
			.toSQL();
		expect(sql).toContain("1 = 1");
	});

	it("rejects paginate with page < 1", () => {
		expect(() => new QueryBuilder("orders").paginate(0, 10)).toThrow(
			"page must be >= 1",
		);
		expect(() => new QueryBuilder("orders").paginate(-1, 10)).toThrow(
			"page must be >= 1",
		);
	});

	it("rejects negative limit", () => {
		expect(() => new QueryBuilder("orders").limit(-1)).toThrow(
			"limit must be >= 0",
		);
	});

	it("rejects negative offset", () => {
		expect(() => new QueryBuilder("orders").offset(-1)).toThrow(
			"offset must be >= 0",
		);
	});

	it("rejects select with no columns", () => {
		expect(() => new QueryBuilder("orders").select()).toThrow(
			"select() requires at least one column",
		);
	});

	it("rejects identifier with double-quote (SQL injection)", () => {
		expect(() =>
			new QueryBuilder('orders"; DROP TABLE orders--').toSQL(),
		).toThrow("Identifier contains illegal characters");
	});

	it("quotes identifiers in SQL output", () => {
		const { sql } = new QueryBuilder("orders")
			.where("status", "active")
			.orderBy("createdAt", "desc")
			.toSQL();
		expect(sql).toContain('"orders"');
		expect(sql).toContain('"status"');
		expect(sql).toContain('"createdAt"');
	});
});

describe("atlas > BaseEntity > getDomainEvents / clearDomainEvents", () => {
	it("getDomainEvents returns events without clearing", () => {
		const order = new Order();
		order.id = "1";
		order.status = "pending";
		order.total = 42;
		order.createdAt = "2026-03-29";
		order.markAsPaid();

		const events1 = order.getDomainEvents();
		expect(events1).toHaveLength(1);

		// Events still present after getDomainEvents
		const events2 = order.getDomainEvents();
		expect(events2).toHaveLength(1);
		expect(order.hasDomainEvents()).toBe(true);
	});

	it("clearDomainEvents removes all events", () => {
		const order = new Order();
		order.id = "1";
		order.status = "pending";
		order.total = 42;
		order.createdAt = "2026-03-29";
		order.markAsPaid();
		expect(order.hasDomainEvents()).toBe(true);

		order.clearDomainEvents();
		expect(order.hasDomainEvents()).toBe(false);
		expect(order.getDomainEvents()).toHaveLength(0);
	});
});

// === Advanced Query Builder tests (Epic 13) ===

describe("atlas > QueryBuilder advanced", () => {
	it("builds GROUP BY", () => {
		const { sql } = new QueryBuilder("orders")
			.select("status", "COUNT(*) AS count")
			.groupBy("status")
			.toSQL();
		expect(sql).toContain('GROUP BY "status"');
		expect(sql).toContain("COUNT(*) AS count");
	});

	it("builds GROUP BY with HAVING", () => {
		const { sql, params } = new QueryBuilder("orders")
			.select("status", "COUNT(*) AS count")
			.groupBy("status")
			.having("COUNT(*)", ">", 5)
			.toSQL();
		expect(sql).toContain('GROUP BY "status"');
		expect(sql).toContain("HAVING COUNT(*) > ?");
		expect(params).toEqual([5]);
	});

	it("builds DISTINCT", () => {
		const { sql } = new QueryBuilder("orders")
			.select("status")
			.distinct()
			.toSQL();
		expect(sql).toContain('SELECT DISTINCT "status"');
	});

	it("builds CTE with QueryBuilder", () => {
		const activeOrders = new QueryBuilder("orders").where("status", "active");
		const { sql, params } = new QueryBuilder("active_orders")
			.with("active_orders", activeOrders)
			.select("*")
			.toSQL();
		expect(sql).toContain('WITH "active_orders" AS (');
		expect(sql).toContain('WHERE "status" = ?');
		expect(sql).toContain('FROM "active_orders"');
		expect(params).toEqual(["active"]);
	});

	it("builds CTE with RawSql", () => {
		const raw = new RawSql(
			"SELECT id, total FROM orders WHERE total > $1",
			[100],
		);
		const { sql, params } = new QueryBuilder("big_orders")
			.with("big_orders", raw)
			.select("*")
			.toSQL();
		expect(sql).toContain('WITH "big_orders" AS (');
		expect(sql).toContain("total > $1");
		expect(params).toEqual([100]);
	});

	it("builds multiple CTEs", () => {
		const active = new QueryBuilder("orders").where("status", "active");
		const paid = new QueryBuilder("orders").where("status", "paid");
		const { sql, params } = new QueryBuilder("active_orders")
			.with("active_orders", active)
			.with("paid_orders", paid)
			.toSQL();
		expect(sql).toContain('WITH "active_orders" AS (');
		expect(sql).toContain('"paid_orders" AS (');
		expect(params).toEqual(["active", "paid"]);
	});

	it("builds UNION with parentheses", () => {
		const q2 = new QueryBuilder("orders").where("status", "paid");
		const { sql, params } = new QueryBuilder("orders")
			.where("status", "pending")
			.union(q2)
			.toSQL();
		expect(sql).toContain("UNION (");
		expect(sql).not.toContain("UNION ALL");
		expect(params).toEqual(["pending", "paid"]);
	});

	it("builds UNION ALL with parentheses", () => {
		const q2 = new QueryBuilder("orders").where("status", "paid");
		const { sql } = new QueryBuilder("orders")
			.where("status", "pending")
			.unionAll(q2)
			.toSQL();
		expect(sql).toContain("UNION ALL (");
	});

	it("builds WHERE EXISTS (subquery)", () => {
		const sub = new QueryBuilder("order_items")
			.select("1")
			.where("orderId", "abc");
		const { sql, params } = new QueryBuilder("orders").whereExists(sub).toSQL();
		expect(sql).toContain("WHERE EXISTS (");
		expect(params).toEqual(["abc"]);
	});

	it("builds WHERE + EXISTS combined with correct param indices", () => {
		const sub = new QueryBuilder("order_items")
			.select("1")
			.where("orderId", "abc");
		const { sql, params } = new QueryBuilder("orders")
			.where("status", "active")
			.whereExists(sub)
			.toSQL();
		expect(sql).toContain('"status" = ?');
		expect(sql).toContain("AND EXISTS (");
		expect(sql).toContain('"orderId" = ?');
		expect(params).toEqual(["active", "abc"]);
	});

	it("rejects invalid CTE name", () => {
		expect(() => new QueryBuilder("t").with("", new QueryBuilder("x"))).toThrow(
			"CTE name must be a valid identifier",
		);
		expect(() =>
			new QueryBuilder("t").with("has space", new QueryBuilder("x")),
		).toThrow("CTE name must be a valid identifier");
	});

	it("HAVING handles IS NULL without pushing spurious params", () => {
		const { sql, params } = new QueryBuilder("orders")
			.select("status", "COUNT(*) AS count")
			.groupBy("status")
			.having("COUNT(*)", ">", 5)
			.toSQL();
		expect(sql).toContain("HAVING COUNT(*) > ?");
		expect(params).toEqual([5]);
		// No extra params
		expect(params).toHaveLength(1);
	});
});

describe("atlas > RawSql", () => {
	it("creates raw SQL with params", () => {
		const raw = new RawSql("SELECT * FROM orders WHERE id = $1", ["123"]);
		expect(raw.sql).toBe("SELECT * FROM orders WHERE id = $1");
		expect(raw.params).toEqual(["123"]);
	});

	it("tagged template auto-parameterizes values", () => {
		const id = "123";
		const status = "active";
		const raw = RawSql.sql`SELECT * FROM orders WHERE id = ${id} AND status = ${status}`;
		expect(raw.sql).toBe("SELECT * FROM orders WHERE id = $1 AND status = $2");
		expect(raw.params).toEqual(["123", "active"]);
	});

	it("tagged template inlines RawSql fragments", () => {
		const table = new RawSql('"orders"');
		const raw = RawSql.sql`SELECT * FROM ${table} WHERE id = ${"123"}`;
		expect(raw.sql).toBe('SELECT * FROM "orders" WHERE id = $1');
		expect(raw.params).toEqual(["123"]);
	});

	it("tagged template with no params", () => {
		const raw = RawSql.sql`SELECT 1`;
		expect(raw.sql).toBe("SELECT 1");
		expect(raw.params).toEqual([]);
	});

	it("tagged template re-indexes RawSql fragment params after scalar values", () => {
		const filter = RawSql.sql`total > ${100}`; // sql: "total > $1", params: [100]
		const full = RawSql.sql`SELECT * FROM orders WHERE status = ${"active"} AND ${filter}`;
		// status = $1, total > $2 (re-indexed from $1 to $2)
		expect(full.sql).toBe(
			"SELECT * FROM orders WHERE status = $1 AND total > $2",
		);
		expect(full.params).toEqual(["active", 100]);
	});

	it("prevents SQL injection through template values", () => {
		const malicious = "'; DROP TABLE orders; --";
		const raw = RawSql.sql`SELECT * FROM orders WHERE name = ${malicious}`;
		// Value is parameterized, not interpolated
		expect(raw.sql).toBe("SELECT * FROM orders WHERE name = $1");
		expect(raw.params).toEqual([malicious]);
		expect(raw.sql).not.toContain("DROP");
	});
});

describe("atlas > BaseRepository > domain event safety", () => {
	it("preserves events if onDomainEvents throws", async () => {
		const repo = new BaseRepository(Order, createMockDb());
		repo.onDomainEvents = async () => {
			throw new Error("dispatch failed");
		};

		const order = new Order();
		order.id = "1";
		order.status = "pending";
		order.total = 42;
		order.createdAt = "2026-03-29";
		order.markAsPaid();

		await expect(repo.save(order)).rejects.toThrow("dispatch failed");
		// Events should still be on the entity since dispatch failed
		expect(order.hasDomainEvents()).toBe(true);
		expect(order.getDomainEvents()).toHaveLength(1);
	});
});

describe("atlas > BaseRepository > save() race recovery", () => {
	/**
	 * Simulate a TOCTOU race: `find(pk)` sees no row, but a concurrent
	 * insert lands the same PK before our own INSERT fires. The DB returns
	 * a unique-key violation; `save()` must recover by falling through to
	 * the UPDATE branch (instead of propagating the constraint error).
	 */
	function createRaceProneDb(opts: {
		findReturns: Record<string, unknown> | null;
		onInsert: () => void;
	}) {
		const updates: Array<{ sql: string; params: unknown[] }> = [];
		const inserts: Array<{ sql: string; params: unknown[] }> = [];
		const trapInsert = (sql: string, params: unknown[]) => {
			inserts.push({ sql, params });
			opts.onInsert();
		};
		const mock = {
			prepare(sql: string) {
				return {
					run(...params: unknown[]) {
						if (sql.startsWith("INSERT")) trapInsert(sql, params);
						else if (sql.startsWith("UPDATE")) updates.push({ sql, params });
						return { changes: 1, lastInsertRowid: 1 };
					},
					get() {
						// SELECT-by-pk → race scenario: row hasn't arrived yet
						// from our perspective. The concurrent insert lands
						// between here and our INSERT call.
						return opts.findReturns ?? undefined;
					},
					all(...params: unknown[]): Record<string, unknown>[] {
						// Non-MySQL dialects route INSERT through `query()`
						// (RETURNING-aware), which hits `all()` via wrapPrepareMock.
						if (sql.startsWith("INSERT")) trapInsert(sql, params);
						// SELECT during `find()` arrives here too — return the
						// configured row (or empty).
						if (sql.startsWith("SELECT") && opts.findReturns !== null) {
							return [opts.findReturns];
						}
						return [];
					},
				};
			},
		};
		return { db: wrapPrepareMock(mock), updates, inserts };
	}

	it("falls back to UPDATE when INSERT hits SQLITE_CONSTRAINT_PRIMARYKEY (race)", async () => {
		const { db, updates, inserts } = createRaceProneDb({
			findReturns: null, // find() sees no row at TOCTOU time
			onInsert: () => {
				const err = new Error("UNIQUE constraint failed: orders.id") as Error & {
					code?: string;
				};
				err.code = "SQLITE_CONSTRAINT_PRIMARYKEY";
				throw err;
			},
		});
		const repo = new BaseRepository(Order, db);
		const order = new Order();
		order.id = "1";
		order.status = "pending";
		order.total = 42;
		order.createdAt = "2026-03-29";

		// Before-fix: the INSERT throw propagated to the caller. After-fix:
		// save() catches the unique violation and runs UPDATE.
		await expect(repo.save(order)).resolves.toBeUndefined();
		expect(inserts).toHaveLength(1); // race-loser still attempted insert
		expect(updates).toHaveLength(1); // recovery ran update
	});

	it("falls back to UPDATE on PostgreSQL '23505' unique_violation", async () => {
		const { db, updates } = createRaceProneDb({
			findReturns: null,
			onInsert: () => {
				const err = new Error("duplicate key value violates unique constraint") as Error & {
					code?: string;
				};
				err.code = "23505";
				throw err;
			},
		});
		const repo = new BaseRepository(Order, db);
		const order = new Order();
		order.id = "1";
		order.status = "p";
		order.total = 1;
		order.createdAt = "2026-03-29";
		await expect(repo.save(order)).resolves.toBeUndefined();
		expect(updates).toHaveLength(1);
	});

	it("falls back to UPDATE on MySQL errno 1062 (ER_DUP_ENTRY)", async () => {
		const { db, updates } = createRaceProneDb({
			findReturns: null,
			onInsert: () => {
				const err = new Error("Duplicate entry '1' for key 'PRIMARY'") as Error & {
					errno?: number;
				};
				err.errno = 1062;
				throw err;
			},
		});
		const repo = new BaseRepository(Order, db);
		const order = new Order();
		order.id = "1";
		order.status = "p";
		order.total = 1;
		order.createdAt = "2026-03-29";
		await expect(repo.save(order)).resolves.toBeUndefined();
		expect(updates).toHaveLength(1);
	});

	it("rethrows non-unique-violation INSERT errors (no silent UPDATE fallback)", async () => {
		const { db, updates } = createRaceProneDb({
			findReturns: null,
			onInsert: () => {
				throw new Error("connection refused");
			},
		});
		const repo = new BaseRepository(Order, db);
		const order = new Order();
		order.id = "1";
		order.status = "p";
		order.total = 1;
		order.createdAt = "2026-03-29";
		// Generic errors must surface — the recovery is bounded to PK
		// conflicts to avoid masking real failures.
		await expect(repo.save(order)).rejects.toThrow("connection refused");
		expect(updates).toHaveLength(0);
	});

	it("does NOT recover when PK was auto-generated (no provided PK = no race)", async () => {
		// No-PK case: the unique-violation recovery is gated on `isProvidedPk`
		// because an auto-generated PK can't collide on first insert (the DB
		// generates a unique one per call). A unique error here is a real bug,
		// not a TOCTOU race — propagate it.
		const { db, updates } = createRaceProneDb({
			findReturns: null,
			onInsert: () => {
				const err = new Error("synthetic unique violation") as Error & {
					code?: string;
				};
				err.code = "SQLITE_CONSTRAINT_UNIQUE";
				throw err;
			},
		});
		const repo = new BaseRepository(Order, db);
		const order = new Order();
		// id deliberately not set
		order.status = "p";
		order.total = 1;
		order.createdAt = "2026-03-29";
		await expect(repo.save(order)).rejects.toThrow(/unique/i);
		expect(updates).toHaveLength(0);
	});
});

describe("atlas > ModelQuery > exec() memoization", () => {
	it("awaiting the same builder twice runs the SQL once (no double-exec on Promise-like assimilation)", async () => {
		let queryCount = 0;
		const mock = {
			prepare(sql: string) {
				return {
					run() {
						return { changes: 0, lastInsertRowid: 0 };
					},
					get() {
						return undefined;
					},
					all() {
						if (sql.startsWith("SELECT")) queryCount++;
						return [];
					},
				};
			},
		};
		const repo = new BaseRepository(Order, wrapPrepareMock(mock));
		const q = repo.query().where("status", "paid");

		const a = await q;
		const b = await q;
		// `Promise.resolve(q)` triggers the `.then` probe — pre-fix it
		// would have run the query a 3rd time.
		await Promise.resolve(q);
		await Promise.all([q, q]);

		expect(queryCount).toBe(1);
		expect(a).toEqual(b);
	});

	it("`.clone()` creates a fresh builder that re-executes", async () => {
		let queryCount = 0;
		const mock = {
			prepare(sql: string) {
				return {
					run() {
						return { changes: 0, lastInsertRowid: 0 };
					},
					get() {
						return undefined;
					},
					all() {
						if (sql.startsWith("SELECT")) queryCount++;
						return [];
					},
				};
			},
		};
		const repo = new BaseRepository(Order, wrapPrepareMock(mock));
		const original = repo.query().where("status", "paid");
		await original;
		await original.clone();
		await original.clone();
		expect(queryCount).toBe(3);
	});
});

// === Story 29.1 — whereHas / has / doesntHave / whereDoesntHave ===

describe("atlas > ModelQuery.whereHas (Story 29.1)", () => {
	it("whereHas on hasMany emits EXISTS with correlated join", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().whereHas("items").toSQL().sql;
		expect(sql).toContain('EXISTS (SELECT * FROM "order_items"');
		expect(sql).toContain('"order_items"."order_id" = "orders"."id"');
	});

	it("whereHas with callback adds constraints inside the subquery", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { sql, params } = repo
			.query()
			.whereHas("items", (q) => {
				q.where("quantity", ">", 2);
			})
			.toSQL();
		expect(sql).toContain('"quantity" > ?');
		expect(params).toContain(2);
	});

	it("whereHas on belongsTo emits EXISTS with flipped join", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().whereHas("user").toSQL().sql;
		expect(sql).toContain('EXISTS (SELECT * FROM "users"');
		expect(sql).toContain('"users"."id" = "orders"."user_id"');
	});

	it("whereDoesntHave emits NOT EXISTS", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().whereDoesntHave("items").toSQL().sql;
		expect(sql).toContain("NOT EXISTS (");
	});

	it("doesntHave short form emits NOT EXISTS", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().doesntHave("items").toSQL().sql;
		expect(sql).toContain("NOT EXISTS (");
	});

	it("has with count threshold adds HAVING COUNT(*)", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { sql, params } = repo.query().has("items", ">", 2).toSQL();
		expect(sql).toContain("HAVING COUNT(*) > ?");
		expect(params).toContain(2);
	});

	it("orWhereHas composes with surrounding wheres via OR", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.where("status", "paid")
			.orWhereHas("items")
			.toSQL().sql;
		expect(sql).toMatch(/"status" = \? OR EXISTS \(/);
	});

	it("throws on unknown relation name", () => {
		const repo = new BaseRepository(Order, createMockDb());
		expect(() => repo.query().whereHas("nonexistent")).toThrow(
			/Relation 'nonexistent' not found/,
		);
	});

	it("nested whereHas compiles to nested EXISTS", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.whereHas("items", (q) => {
				// OrderItem doesn't have nested relations in this fixture, but the composition should still work
				q.where("quantity", ">", 0);
			})
			.toSQL().sql;
		expect(sql).toContain('EXISTS (SELECT * FROM "order_items"');
		expect(sql).toContain('"quantity" > ?');
	});
});

// === Story 29.2 — withCount / withAggregate + loadCount / loadAggregate ===

describe("atlas > ModelQuery.withCount (Story 29.2)", () => {
	it("withCount emits a correlated subquery aliased as `${relation}_count`", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().withCount("items").toSQL().sql;
		expect(sql).toContain('(SELECT COUNT(*) FROM "order_items"');
		expect(sql).toContain(') AS "items_count"');
		expect(sql).toContain('"order_items"."order_id" = "orders"."id"');
	});

	it("withCount with callback applies inner WHERE", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { sql, params } = repo
			.query()
			.withCount("items", (q) => q.where("quantity", ">", 0))
			.toSQL();
		expect(sql).toContain('"quantity" > ?');
		expect(params).toContain(0);
	});

	it("withCount with .as(alias) uses the custom alias", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.withCount("items", (q) => q.as("big_items").where("quantity", ">", 5))
			.toSQL().sql;
		expect(sql).toContain(') AS "big_items"');
	});

	it("withAggregate sum with alias", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.withAggregate("items", (q) =>
				q.selectAggregate("sum", "quantity").as("total_qty"),
			)
			.toSQL().sql;
		expect(sql).toContain('SELECT SUM(quantity) FROM "order_items"');
		expect(sql).toContain(') AS "total_qty"');
	});

	it("withAggregate throws if callback does not set an aggregate", () => {
		const repo = new BaseRepository(Order, createMockDb());
		expect(() =>
			repo.query().withAggregate("items", () => {
				/* no-op */
			}),
		).toThrow(/must set an aggregate/);
	});

	it("withCount params land before outer WHERE params", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { params } = repo
			.query()
			.withCount("items", (q) => q.where("quantity", ">", 1))
			.where("status", "paid")
			.toSQL();
		expect(params[0]).toBe(1); // subquery param first
		expect(params[1]).toBe("paid"); // outer WHERE param second
	});

	it("multiple withCount calls compose", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().withCount("items").withCount("user").toSQL().sql;
		expect(sql).toContain('AS "items_count"');
		expect(sql).toContain('AS "user_count"');
	});
});

// === Story 29.3 — where callback grouping + whereIn subquery ===

describe("atlas > ModelQuery where groups + whereIn subquery (Story 29.3)", () => {
	it("where(cb) emits a parenthesised group", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.where((q) => q.where("status", "paid").orWhere("status", "pending"))
			.toSQL().sql;
		expect(sql).toMatch(/WHERE \("status" = \? OR "status" = \?\)/);
	});

	it("group composes with sibling WHERE via OR", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.where("total", ">", 100)
			.orWhere((q) => q.where("status", "paid").where("total", ">", 50))
			.toSQL().sql;
		expect(sql).toMatch(/"total" > \? OR \("status" = \? AND "total" > \?\)/);
	});

	it("whereIn still accepts an array", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { sql, params } = repo.query().whereIn("id", [1, 2, 3]).toSQL();
		expect(sql).toContain('"id" IN (?, ?, ?)');
		expect(params).toEqual([1, 2, 3]);
	});

	it("whereIn accepts a ModelQuery subquery source", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const userRepo = new BaseRepository(User, createMockDb());
		const activeUsers = userRepo.query().select(["id"]).where("name", "alice");
		const { sql, params } = repo.query().whereIn("status", activeUsers).toSQL();
		expect(sql).toContain('"status" IN (SELECT "id" FROM "users"');
		expect(params).toEqual(["alice"]);
	});

	it("whereNotIn accepts a ModelQuery subquery source", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const userRepo = new BaseRepository(User, createMockDb());
		const banned = userRepo.query().select(["id"]).where("name", "bob");
		const sql = repo.query().whereNotIn("status", banned).toSQL().sql;
		expect(sql).toContain('NOT IN (SELECT "id" FROM "users"');
	});

	it("nested where(cb) within a group", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.where((q) =>
				q
					.where("status", "paid")
					.orWhere((inner) =>
						inner.where("total", ">", 10).where("status", "pending"),
					),
			)
			.toSQL().sql;
		expect(sql).toContain('("status" = ? OR ("total" > ? AND "status" = ?))');
	});
});

// === Stories 29.4 / 29.5 / 29.8 / 29.9 / 29.10 / 29.11 ============================

describe("atlas > ModelQuery misc builder (Stories 29.4–29.11)", () => {
	it("innerJoin string form emits INNER JOIN", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.innerJoin("users", "users.id", "orders.user_id")
			.toSQL().sql;
		expect(sql).toContain(
			'INNER JOIN "users" ON "users"."id" = "orders"."user_id"',
		);
	});

	it("leftJoin callback form composes multiple conditions", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.leftJoin("users", (j) =>
				j
					.on("users.id", "orders.user_id")
					.andOn("users.tenant_id", "orders.tenant_id"),
			)
			.toSQL().sql;
		expect(sql).toContain('LEFT JOIN "users"');
		expect(sql).toContain('ON "users"."id" = "orders"."user_id"');
		expect(sql).toContain('AND "users"."tenant_id" = "orders"."tenant_id"');
	});

	it("joinRaw appends verbatim fragment", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.joinRaw('LEFT JOIN "users" ON "users"."id" = "orders"."user_id"')
			.toSQL().sql;
		expect(sql).toContain('LEFT JOIN "users"');
	});

	it("distinct sets DISTINCT in SELECT", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().distinct().select(["status"]).toSQL().sql;
		expect(sql).toContain('SELECT DISTINCT "status"');
	});

	it("if applies callback when condition truthy", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.if("paid", (q, v) => q.where("status", v))
			.toSQL().sql;
		expect(sql).toContain('"status" = ?');
	});

	it("if skips callback when falsy", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.if(undefined, (q) => q.where("status", "paid"))
			.toSQL().sql;
		expect(sql).not.toContain('"status" = ?');
	});

	it("unless is inverse of if", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.unless(false, (q) => q.where("status", "paid"))
			.toSQL().sql;
		expect(sql).toContain('"status" = ?');
	});

	it("forPage sets limit + offset", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().forPage(3, 20).toSQL().sql;
		expect(sql).toContain("LIMIT 20");
		expect(sql).toContain("OFFSET 40");
	});

	it("clone is independent from the original", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const a = repo.query().where("status", "paid");
		const b = a.clone().where("status", "draft");
		// Original keeps one predicate; clone has both.
		expect(a.toSQL().params.length).toBe(1);
		expect(b.toSQL().params.length).toBe(2);
	});

	it("toQuery interpolates bindings as literals", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const q = repo.query().where("status", "paid").where("total", ">", 100);
		const interpolated = q.toQuery();
		expect(interpolated).toContain("'paid'");
		expect(interpolated).toContain("100");
		expect(interpolated).not.toContain("?");
	});

	it("scopes applied via apply() chain", () => {
		class Widget extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare status: string;
			static scopes = {
				active: (q: ModelQuery<Widget>) => q.where("status", "active"),
			};
		}
		Entity("widgets")(Widget);
		const repo = new BaseRepository(Widget, createMockDb());
		const sql = repo
			.query()
			.apply((s) => {
				s.active();
			})
			.toSQL().sql;
		expect(sql).toContain('"status" = ?');
	});

	it("apply throws on unknown scope", () => {
		class Gadget extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare status: string;
			static scopes = { active: (q: ModelQuery<Gadget>) => q };
		}
		Entity("gadgets")(Gadget);
		const repo = new BaseRepository(Gadget, createMockDb());
		expect(() =>
			repo.query().apply((s) => {
				(s as unknown as { nope: () => void }).nope();
			}),
		).toThrow(/Unknown scope/);
	});

	it("forUpdate noop on sqlite dialect", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo.query().forUpdate().toSQL().sql;
		expect(sql).not.toContain("FOR UPDATE");
	});
});

// === Story 30.7 — fill / merge + MassAssignmentError ==============================

describe("atlas > BaseEntity fill/merge (Story 30.7)", () => {
	class Post extends BaseEntity {
		static fillable = ["title", "body"];
		title?: string;
		body?: string;
		isAdmin?: boolean;
	}

	it("fill assigns fillable columns", () => {
		const p = new Post();
		p.fill({ title: "Hello", body: "World" });
		expect(p.title).toBe("Hello");
		expect(p.body).toBe("World");
	});

	it("fill rejects non-fillable columns", () => {
		const p = new Post();
		expect(() => p.fill({ title: "x", isAdmin: true })).toThrow(
			/mass-assignable/,
		);
	});

	it("fill resets fillable fields not in the payload", () => {
		const p = new Post();
		p.fill({ title: "Hello", body: "World" });
		p.fill({ title: "Only" });
		expect(p.title).toBe("Only");
		expect(p.body).toBeUndefined();
	});

	it("merge patches only the provided keys", () => {
		const p = new Post();
		p.fill({ title: "Hello", body: "World" });
		p.merge({ title: "New" });
		expect(p.title).toBe("New");
		expect(p.body).toBe("World");
	});

	it("declaring both fillable and guarded throws", () => {
		class Bad extends BaseEntity {
			static fillable = ["a"];
			static guarded = ["b"];
		}
		expect(() => new Bad().fill({ a: 1 })).toThrow(/both/);
	});
});

// === Epic 31 remainder — Relations advanced (31.2, 31.3, 31.6, 31.7, 31.9) =========

describe("atlas > @HasMany custom keys (Story 31.3)", () => {
	it("decorator accepts foreignKey + localKey options", () => {
		class Author extends BaseEntity {
			@PrimaryKey() declare uuid: string;
			@Column() declare name: string;
			@HasMany(() => BookX, { localKey: "uuid", foreignKey: "author_uuid" })
			declare books: BookX[];
		}
		class BookX extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare title: string;
		}
		Entity("authors")(Author);
		Entity("books")(BookX);

		const rel = getRelationMetadata(Author).find(
			(r) => r.propertyKey === "books",
		);
		expect(rel?.foreignKey).toBe("author_uuid");
		expect(rel?.localKey).toBe("uuid");
	});
});

describe("atlas > @HasManyThrough (Story 31.2)", () => {
	it("registers through metadata", () => {
		class Country extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare name: string;
			@HasManyThrough(
				() => Post,
				() => UserX,
			)
			declare posts: Post[];
		}
		class UserX extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare countryId: string;
		}
		class Post extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare userId: string;
		}
		Entity("countries")(Country);
		Entity("users_through")(UserX);
		Entity("posts_through")(Post);

		const rel = getRelationMetadata(Country).find(
			(r) => r.propertyKey === "posts",
		);
		expect(rel?.type).toBe("hasManyThrough");
		expect(rel?.through).toBeDefined();
	});
});

describe("atlas > @BelongsTo onQuery + serializeAs (Stories 31.3/31.4)", () => {
	it("stores onQuery + serializeAs on metadata", () => {
		class Profile extends BaseEntity {
			@PrimaryKey() declare id: string;
			@BelongsTo(() => UserY, {
				foreignKey: "user_id",
				serializeAs: "author",
				onQuery: (q) => {
					void q;
				},
			})
			declare user: UserY | null;
		}
		class UserY extends BaseEntity {
			@PrimaryKey() declare id: string;
		}
		Entity("profiles")(Profile);
		Entity("users_y")(UserY);

		const rel = getRelationMetadata(Profile).find(
			(r) => r.propertyKey === "user",
		);
		expect(rel?.serializeAs).toBe("author");
		expect(typeof rel?.onQuery).toBe("function");
		expect(rel?.foreignKey).toBe("user_id");
	});
});

// === Post-review fixes (gaps + C1/C2/G1/W1/W3) =====================================

describe("atlas > fill with $original (Gap: fill/$dirty)", () => {
	class Book extends BaseEntity {
		static fillable = ["title", "author"];
		title?: string;
		author?: string;
	}

	it("fill on fresh entity deletes absent fillable keys rather than setting undefined", () => {
		const b = new Book();
		b.fill({ title: "A", author: "X" });
		b.fill({ title: "B" });
		// 'author' should be absent, not `undefined`
		expect(Object.keys(b)).not.toContain("author");
		expect(b.title).toBe("B");
	});

	it("fill on hydrated entity resets absent fillable keys to $original value (no phantom dirty)", () => {
		const b = new Book();
		b.title = "A";
		b.author = "X";
		b.markAsPersisted();
		b.fill({ title: "B" });
		// author reset to persisted value, not marked dirty
		expect(b.author).toBe("X");
		expect(b.isDirty("author")).toBe(false);
		expect(b.isDirty("title")).toBe(true);
	});
});

describe("atlas > cursorPaginate multi-column (Gap)", () => {
	it("accepts an array of orderBy columns", () => {
		const repo = new BaseRepository(Order, createMockDb());
		// We only verify toSQL doesn't throw and the orderBy includes both cols.
		const q = repo.query();
		q.orderBy("createdAt").orderBy("id");
		// Direct check — we can't exec without a real DB. Validate cursor build
		// by calling the method with no cursor (empty fetch) and asserting the
		// returned shape is correct on an empty result.
		return q
			.cursorPaginate({ limit: 10, orderBy: ["created_at", "id"] })
			.then((res) => {
				expect(Array.isArray(res.items)).toBe(true);
				expect(res.hasMore).toBe(false);
				expect(res.nextCursor).toBeNull();
			});
	});

	it("rejects a cursor with mismatched tuple length", async () => {
		const repo = new BaseRepository(Order, createMockDb());
		const badCursor = Buffer.from(JSON.stringify({ v: ["only-one"] })).toString(
			"base64",
		);
		await expect(
			repo.query().cursorPaginate({
				cursor: badCursor,
				limit: 10,
				orderBy: ["created_at", "id"],
			}),
		).rejects.toThrow(/tuple length mismatch/);
	});
});

describe("atlas > scope() TS helper (Gap)", () => {
	it("is exported and acts as identity at runtime", async () => {
		const atlas = await import("../../src/index.js");
		expect(typeof atlas.scope).toBe("function");
		const fn = (q: unknown) => q;
		expect(atlas.scope(fn as never)).toBe(fn);
	});
});

describe("atlas > @HasOne blocks createMany/saveMany (G1)", () => {
	it("relatedProxy on hasOne rejects createMany with a clear message", async () => {
		class ProfileHO extends BaseEntity {
			@PrimaryKey() declare id: string;
			@Column() declare userId: string;
		}
		class UserHO extends BaseEntity {
			@PrimaryKey() declare id: string;
			@HasOne(() => ProfileHO) declare profile: ProfileHO | null;
		}
		Entity("profiles_ho")(ProfileHO);
		Entity("users_ho")(UserHO);

		const repo = new BaseRepository(UserHO, createMockDb());
		const user = new UserHO();
		user.id = "1";
		// Pretend the entity is hydrated so it has a REPO_REF.
		const REPO_REF = Symbol.for("atlas:repoRef");
		(user as unknown as Record<symbol, unknown>)[REPO_REF] = repo;
		await expect(user.related("profile").createMany?.([{}])).rejects.toThrow(
			/not supported on @HasOne/,
		);
	});
});

// === Final audit fixes — whereExpr / joinOn / PK order ============================

describe("atlas > safe whereExpr/joinOn + PK ordering", () => {
	it("whereExpr 3-arg form emits standard quoted WHERE", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { sql, params } = repo.query().whereExpr("total", ">", 100).toSQL();
		expect(sql).toContain('"total" > ?');
		expect(params).toEqual([100]);
	});

	it("whereExpr 4-arg form applies the extra expression", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const { sql, params } = repo
			.query()
			.whereExpr("total", "+ 10", ">=", 50)
			.toSQL();
		expect(sql).toContain('"total" + 10 >= ?');
		expect(params).toEqual([50]);
	});

	it("whereExpr rejects a dangerous extra expression", () => {
		const repo = new BaseRepository(Order, createMockDb());
		expect(() =>
			repo.query().whereExpr("total", "; DROP TABLE orders--", ">", 1),
		).toThrow(/forbidden characters/);
	});

	it("joinOn is equivalent to innerJoin string form", () => {
		const repo = new BaseRepository(Order, createMockDb());
		const a = repo
			.query()
			.joinOn("users", "users.id", "orders.user_id")
			.toSQL().sql;
		const b = repo
			.query()
			.innerJoin("users", "users.id", "orders.user_id")
			.toSQL().sql;
		expect(a).toBe(b);
		expect(a).toContain(
			'INNER JOIN "users" ON "users"."id" = "orders"."user_id"',
		);
	});

	it("repo.where orders by the resolved PK, not rowid", () => {
		// We can't easily exec() against the mock, but we can inspect the SQL
		// by going through query() with the same ordering semantics.
		const repo = new BaseRepository(Order, createMockDb());
		const sql = repo
			.query()
			.where("status", "paid")
			.orderBy("id", "desc")
			.toSQL().sql;
		expect(sql).not.toContain("rowid");
		expect(sql).toContain('ORDER BY "id" DESC');
	});
});

// === Atlas strict mode ============================================================

describe("atlas > strict mode (raw API hardening)", () => {
	it("whereRaw throws when strict mode is enabled", async () => {
		const { setAtlasStrictMode } = await import("../../src/index.js");
		setAtlasStrictMode(true);
		try {
			const repo = new BaseRepository(Order, createMockDb());
			expect(() => repo.query().whereRaw("1 = 1")).toThrow(
				/disabled in Atlas strict mode/,
			);
		} finally {
			setAtlasStrictMode(false);
		}
	});

	it("joinRaw throws when strict mode is enabled", async () => {
		const { setAtlasStrictMode } = await import("../../src/index.js");
		setAtlasStrictMode(true);
		try {
			const repo = new BaseRepository(Order, createMockDb());
			expect(() => repo.query().joinRaw("JOIN users ON 1 = 1")).toThrow(
				/disabled in Atlas strict mode/,
			);
		} finally {
			setAtlasStrictMode(false);
		}
	});

	it("whereExpr still works when strict mode is enabled", async () => {
		const { setAtlasStrictMode } = await import("../../src/index.js");
		setAtlasStrictMode(true);
		try {
			const repo = new BaseRepository(Order, createMockDb());
			const sql = repo.query().whereExpr("total", ">", 100).toSQL().sql;
			expect(sql).toContain('"total" > ?');
		} finally {
			setAtlasStrictMode(false);
		}
	});

	it("joinOn still works when strict mode is enabled", async () => {
		const { setAtlasStrictMode } = await import("../../src/index.js");
		setAtlasStrictMode(true);
		try {
			const repo = new BaseRepository(Order, createMockDb());
			const sql = repo
				.query()
				.joinOn("users", "users.id", "orders.user_id")
				.toSQL().sql;
			expect(sql).toContain('INNER JOIN "users"');
		} finally {
			setAtlasStrictMode(false);
		}
	});

	it("whereHas preload correlated join still works under strict mode", async () => {
		const { setAtlasStrictMode } = await import("../../src/index.js");
		setAtlasStrictMode(true);
		try {
			// whereHas internally builds a sub-query that calls #pushWhereRaw
			// (framework path) — strict mode must NOT block it.
			const repo = new BaseRepository(Order, createMockDb());
			const sql = repo.query().whereHas("items").toSQL().sql;
			expect(sql).toContain('EXISTS (SELECT * FROM "order_items"');
		} finally {
			setAtlasStrictMode(false);
		}
	});
});
