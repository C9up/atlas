import "reflect-metadata";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";

@Entity("guard_widgets")
class Widget extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

describe("atlas > BaseRepository connection guard", () => {
	// Regression: a failed IoC injection used to surface as the cryptic
	// "Cannot read properties of undefined (reading 'dialect')". The ctor now
	// fails with a clear, actionable message. `Reflect.construct` lets the test
	// pass a non-connection without a type cast.
	it("throws a clear error when the connection is undefined", () => {
		expect(() =>
			Reflect.construct(BaseRepository, [Widget, undefined]),
		).toThrow(/requires a DatabaseConnection \(got undefined\)/);
	});

	it("throws a clear error when the connection is null", () => {
		expect(() => Reflect.construct(BaseRepository, [Widget, null])).toThrow(
			/requires a DatabaseConnection \(got null\)/,
		);
	});
});
