import { describe, expect, it } from "vitest";
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

	it("relationForeignKey snake_cases the parent class + appends pk", () => {
		expect(s.relationForeignKey("belongsTo", "User", "id")).toBe("user_id");
		expect(s.relationForeignKey("hasMany", "OrderItem", "id")).toBe(
			"order_item_id",
		);
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
