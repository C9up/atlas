import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import {
	defaultRelationForeignKey,
	Entity,
	PrimaryKey,
} from "../../src/decorators/entity.js";
import {
	CamelCaseNamingStrategy,
	defaultNamingStrategy,
	getNamingStrategy,
	type NamingStrategy,
} from "../../src/naming/NamingStrategy.js";

describe("atlas > CamelCaseNamingStrategy > tableName", () => {
	const s = new CamelCaseNamingStrategy();

	it("snake-cases and pluralizes a regular class name", () => {
		expect(s.tableName("UserProfile")).toBe("user_profiles");
	});

	it("does not double-pluralize a class name already ending in 's'", () => {
		expect(s.tableName("News")).toBe("news");
	});
});

describe("atlas > CamelCaseNamingStrategy > columnName / propertyName", () => {
	const s = new CamelCaseNamingStrategy();

	it("camelToSnake for column names", () => {
		expect(s.columnName("createdAt")).toBe("created_at");
		expect(s.columnName("emailAddress")).toBe("email_address");
	});

	it("snakeToCamel for property names", () => {
		expect(s.propertyName("created_at")).toBe("createdAt");
		expect(s.propertyName("user_id")).toBe("userId");
	});

	it("serializedName defaults to the property name (identity)", () => {
		expect(s.serializedName("emailAddress")).toBe("emailAddress");
	});
});

describe("atlas > CamelCaseNamingStrategy > relations", () => {
	const s = new CamelCaseNamingStrategy();

	it("relationLocalKey returns the parent's PK", () => {
		expect(s.relationLocalKey("hasMany", "id")).toBe("id");
		expect(s.relationLocalKey("belongsTo", "uuid")).toBe("uuid");
	});

	it("relationForeignKey answers with the ATTRIBUTE, as upstream does", () => {
		// Upstream returns `camelCase(`${Model}_${pk}`)` and runs it through
		// `columnName()` to reach a column. Returning the column here would send
		// it through that conversion twice, and a strategy ported from upstream
		// would silently produce the wrong name.
		expect(s.relationForeignKey("belongsTo", "User", "id")).toBe("userId");
		expect(s.relationForeignKey("hasMany", "OrderItem", "id")).toBe(
			"orderItemId",
		);
		// A multi-word PK is split on its word boundaries before rejoining.
		expect(s.relationForeignKey("hasMany", "User", "userId")).toBe(
			"userUserId",
		);
	});

	it("still derives the same COLUMN as before, through columnName", () => {
		// The contract changed; the schema must not. Every relation resolves its
		// foreign key by composing these two, so this is the pair that has to
		// keep answering what a migration actually created.
		const column = (cls: string, pk: string): string =>
			s.columnName(s.relationForeignKey("belongsTo", cls, pk));
		expect(column("User", "id")).toBe("user_id");
		expect(column("OrderItem", "id")).toBe("order_item_id");
		expect(column("User", "userId")).toBe("user_user_id");
	});

	it("relationPivotTable sorts class names alphabetically (UserSkill = SkillUser)", () => {
		expect(s.relationPivotTable("User", "Skill")).toBe(
			s.relationPivotTable("Skill", "User"),
		);
		expect(s.relationPivotTable("User", "Skill")).toBe("skill_user");
	});
});

describe("atlas > getNamingStrategy", () => {
	class Default {}

	// biome-ignore lint/complexity/noStaticOnlyClass: an entity fixture must be a class — the static naming-strategy override is the behaviour under test
	class Custom {
		static namingStrategy: NamingStrategy = new CamelCaseNamingStrategy();
	}

	class Inherits extends Custom {}

	it("returns the singleton default for a class without override", () => {
		expect(getNamingStrategy(Default)).toBe(defaultNamingStrategy);
	});

	it("returns the explicit static override on the class", () => {
		expect(getNamingStrategy(Custom)).toBe(Custom.namingStrategy);
	});

	it("walks the prototype chain to inherit the parent's override", () => {
		expect(getNamingStrategy(Inherits)).toBe(Custom.namingStrategy);
	});
});

describe("atlas > default relation foreign key follows the primary key", () => {
	@Entity("users")
	class User extends BaseEntity {
		@PrimaryKey() declare uuid: string;
	}

	@Entity("posts")
	class Post extends BaseEntity {
		@PrimaryKey() declare id: number;
	}

	@Entity("audits")
	class Audit extends BaseEntity {
		@PrimaryKey() declare recordId: string;
	}

	it("uses the model's real primary key, not a hardcoded id", () => {
		// The default used to be `${snake(class)}_id` in twenty-three places, so
		// a model keyed by `uuid` got a `user_id` column that does not exist.
		expect(defaultRelationForeignKey("hasMany", User)).toBe("user_uuid");
	});

	it("is unchanged for the usual id", () => {
		expect(defaultRelationForeignKey("hasMany", Post)).toBe("post_id");
	});

	it("snake-cases a multi-word primary key, as Lucid does", () => {
		expect(defaultRelationForeignKey("belongsTo", Audit)).toBe(
			"audit_record_id",
		);
	});

	it("honours a custom naming strategy", () => {
		class Prefixed extends CamelCaseNamingStrategy {
			override relationForeignKey(
				_kind: "belongsTo" | "hasMany" | "hasOne" | "manyToMany",
				parentClass: string,
				parentPk: string,
			): string {
				return `fk_${parentClass.toLowerCase()}_${parentPk}`;
			}
		}
		@Entity("things")
		class Thing extends BaseEntity {
			@PrimaryKey() declare id: number;
			static namingStrategy = new Prefixed();
		}
		expect(defaultRelationForeignKey("hasMany", Thing)).toBe("fk_thing_id");
	});
});

describe("atlas > a naming strategy ported from upstream", () => {
	/**
	 * The shape someone migrating writes: `relationForeignKey` returns the
	 * ATTRIBUTE, exactly as upstream's own strategies do. Before the contract
	 * changed, atlas took that camelCase value for a column name and looked for
	 * `userId` in the database — a column no migration ever created, and a
	 * failure that pointed at the relation rather than at the strategy.
	 */
	class PortedStrategy extends CamelCaseNamingStrategy {
		override relationForeignKey(
			relation: "belongsTo" | "hasMany" | "hasOne" | "manyToMany",
			parentClass: string,
			parentPk: string,
		): string {
			// Upstream distinguishes belongsTo; the value is an attribute either way.
			return relation === "belongsTo"
				? `${parentClass.toLowerCase()}Ref`
				: `${parentClass.toLowerCase()}${parentPk.replace(/^./, (c) => c.toUpperCase())}`;
		}
	}

	it("reaches a real column from the attribute it returns", () => {
		const s = new PortedStrategy();
		expect(s.columnName(s.relationForeignKey("belongsTo", "User", "id"))).toBe(
			"user_ref",
		);
		expect(s.columnName(s.relationForeignKey("hasMany", "User", "id"))).toBe(
			"user_id",
		);
	});
});
