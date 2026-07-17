/**
 * `whereExists` on the builder `repo.query()` actually returns (it previously
 * existed only on the low-level `query/QueryBuilder`, so it was unreachable),
 * the `and*` aliases, and the or/and pivot filters.
 *
 * The pivot test is the load-bearing one: pivot filters compile as a
 * parenthesised group, so an `orWherePivot` cannot escape the
 * `pivot_fk IN (parents)` scoping and drag in another parent's rows.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import {
	Column,
	Entity,
	ManyToMany,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("roles")
class Role extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

@Entity("users")
class User extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare email: string;

	@ManyToMany(() => Role, {
		pivotTable: "role_user",
		foreignKey: "user_id",
		otherKey: "role_id",
		pivotColumns: ["active", "revoked_at"],
	})
	declare roles: Role[];
}

@Entity("posts")
class Post extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare userId: number;
	@Column() declare title: string;
}

describe("whereExists on repo.query()", () => {
	let conn: AsyncDatabaseConnection;
	let users: BaseRepository<User>;
	let posts: BaseRepository<Post>;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
			[],
		);
		await conn.execute(
			"CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)",
			[],
		);
		await conn.execute(
			"INSERT INTO users (id, email) VALUES (1, 'a@b.co')",
			[],
		);
		await conn.execute(
			"INSERT INTO users (id, email) VALUES (2, 'c@d.co')",
			[],
		);
		await conn.execute(
			"INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'hello')",
			[],
		);
		users = new BaseRepository(User, conn);
		posts = new BaseRepository(Post, conn);
	});

	it("keeps only the rows the correlated subquery matches", async () => {
		const rows = await users
			.query()
			.whereExists(posts.query().whereColumn("posts.user_id", "=", "users.id"))
			.exec();

		expect(rows.map((u) => u.id)).toEqual([1]);
	});

	it("inverts with whereNotExists", async () => {
		const rows = await users
			.query()
			.whereNotExists(
				posts.query().whereColumn("posts.user_id", "=", "users.id"),
			)
			.exec();

		expect(rows.map((u) => u.id)).toEqual([2]);
	});

	it("carries the subquery's own filters and bindings", async () => {
		const none = await users
			.query()
			.whereExists(
				posts
					.query()
					.whereColumn("posts.user_id", "=", "users.id")
					.where("title", "nope"),
			)
			.exec();
		expect(none).toEqual([]);

		const some = await users
			.query()
			.whereExists(
				posts
					.query()
					.whereColumn("posts.user_id", "=", "users.id")
					.where("title", "hello"),
			)
			.exec();
		expect(some.map((u) => u.id)).toEqual([1]);
	});

	it("composes with OR", async () => {
		const rows = await users
			.query()
			.where("email", "c@d.co")
			.orWhereExists(
				posts.query().whereColumn("posts.user_id", "=", "users.id"),
			)
			.exec();

		expect(rows.map((u) => u.id).sort()).toEqual([1, 2]);
	});
});

describe("and* aliases", () => {
	let conn: AsyncDatabaseConnection;
	let users: BaseRepository<User>;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
			[],
		);
		await conn.execute(
			"INSERT INTO users (id, email) VALUES (1, 'a@b.co')",
			[],
		);
		await conn.execute(
			"INSERT INTO users (id, email) VALUES (2, 'c@d.co')",
			[],
		);
		users = new BaseRepository(User, conn);
	});

	it("reads the same as the base method", async () => {
		const rows = await users
			.query()
			.where("id", 1)
			.andWhere("email", "a@b.co")
			.exec();
		expect(rows.map((u) => u.id)).toEqual([1]);
	});

	it("keeps the 2-arg overload from leaking a phantom operator", async () => {
		// andWhere(col, value) must not forward `undefined` as the value and turn
		// the value into an operator.
		const rows = await users.query().andWhere("id", 2).exec();
		expect(rows.map((u) => u.id)).toEqual([2]);
	});

	it("supports the 3-arg operator form", async () => {
		const rows = await users.query().andWhere("id", ">=", 2).exec();
		expect(rows.map((u) => u.id)).toEqual([2]);
	});

	it("supports the callback group form", async () => {
		const rows = await users
			.query()
			.andWhere((q) => {
				q.where("id", 1).orWhere("id", 2);
			})
			.exec();
		expect(rows.map((u) => u.id).sort()).toEqual([1, 2]);
	});

	it("negates columns with whereNotColumn", async () => {
		const rows = await users
			.query()
			.whereNotColumn("users.id", "=", "users.id")
			.exec();
		expect(rows).toEqual([]);
	});
});

describe("or/and pivot filters", () => {
	let conn: AsyncDatabaseConnection;
	let users: BaseRepository<User>;

	beforeEach(async () => {
		setAtlasDialect("sqlite");
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
			[],
		);
		await conn.execute(
			"CREATE TABLE roles (id INTEGER PRIMARY KEY, name TEXT)",
			[],
		);
		await conn.execute(
			"CREATE TABLE role_user (user_id INTEGER, role_id INTEGER, active INTEGER, revoked_at TEXT)",
			[],
		);
		await conn.execute(
			"INSERT INTO users (id, email) VALUES (1, 'a@b.co')",
			[],
		);
		await conn.execute(
			"INSERT INTO users (id, email) VALUES (2, 'c@d.co')",
			[],
		);
		await conn.execute("INSERT INTO roles (id, name) VALUES (1, 'admin')", []);
		await conn.execute("INSERT INTO roles (id, name) VALUES (2, 'editor')", []);
		// user 1 → admin (inactive), user 2 → editor (active)
		await conn.execute(
			"INSERT INTO role_user (user_id, role_id, active, revoked_at) VALUES (1, 1, 0, NULL)",
			[],
		);
		await conn.execute(
			"INSERT INTO role_user (user_id, role_id, active, revoked_at) VALUES (2, 2, 1, NULL)",
			[],
		);
		users = new BaseRepository(User, conn);
	});

	/**
	 * The regression this grouping exists for: flat, the OR would read as
	 * `WHERE user_id IN (1) OR active = 1` and pull user 2's editor row into
	 * user 1's roles.
	 */
	it("does not let an OR escape the parent IN scoping", async () => {
		const [user] = await users
			.query()
			.where("id", 1)
			.preload("roles", (q) => {
				q.wherePivot("active", 0).orWherePivot("active", 1);
			})
			.exec();

		expect(user?.roles.map((r) => r.name)).toEqual(["admin"]);
	});

	it("ORs the pivot filters within the group", async () => {
		const [user] = await users
			.query()
			.where("id", 2)
			.preload("roles", (q) => {
				// Neither matches on its own value except the second.
				q.wherePivot("active", 0).orWherePivot("active", 1);
			})
			.exec();

		expect(user?.roles.map((r) => r.name)).toEqual(["editor"]);
	});

	it("still ANDs by default", async () => {
		const [user] = await users
			.query()
			.where("id", 2)
			.preload("roles", (q) => {
				q.wherePivot("active", 0);
			})
			.exec();

		expect(user?.roles).toEqual([]);
	});

	it("filters on a null pivot column", async () => {
		const [user] = await users
			.query()
			.where("id", 1)
			.preload("roles", (q) => {
				q.whereNullPivot("revoked_at");
			})
			.exec();

		expect(user?.roles.map((r) => r.name)).toEqual(["admin"]);

		const [none] = await users
			.query()
			.where("id", 1)
			.preload("roles", (q) => {
				q.whereNotNullPivot("revoked_at");
			})
			.exec();
		expect(none?.roles).toEqual([]);
	});
});
