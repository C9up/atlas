/**
 * A subclass's `@Column` serialize options replace its parent's for the same
 * column, as a Lucid subclass's column definition does. The prototype chain
 * was merged from the subclass up, so the parent's entry won: a column the
 * subclass hid with `serializeAs: null` was serialized anyway.
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseEntity } from "../../src/BaseEntity.js";
import {
	Column,
	getDateColumnConfig,
	PrimaryKey,
} from "../../src/decorators/entity.js";

describe("atlas > a subclass's column serialize options win over its parent's", () => {
	class Account extends BaseEntity {
		@PrimaryKey() declare id: number;
		@Column({ serialize: (v: unknown) => v }) declare apiToken: string;
		@Column({ serializeAs: null }) declare internalCode: string;
		@Column({ serializeAs: "label" }) declare name: string;
	}

	class PublicAccount extends Account {
		@Column({ serializeAs: null }) declare apiToken: string;
		@Column({ serializeAs: "code" }) declare internalCode: string;
	}

	class ScopedAccount extends PublicAccount {
		@Column({ serializeAs: "title" }) declare name: string;
	}

	function fill<T extends Account>(account: T): T {
		account.id = 1;
		account.apiToken = "secret-token";
		account.internalCode = "X1";
		account.name = "acme";
		return account;
	}

	it("a subclass hides a column its parent transformed", () => {
		const out = JSON.parse(JSON.stringify(fill(new PublicAccount())));
		expect(out).not.toHaveProperty("apiToken");
		expect(JSON.stringify(out)).not.toContain("secret-token");
	});

	it("a subclass renames a column its parent hid", () => {
		const out = fill(new PublicAccount()).toJSON();
		expect(out.code).toBe("X1");
		expect(out).not.toHaveProperty("internalCode");
	});

	it("the nearest class wins in a three-level chain", () => {
		const out = fill(new ScopedAccount()).toJSON();
		expect(out).toEqual({ id: 1, code: "X1", title: "acme" });
		expect(fill(new Account()).toJSON()).toEqual({
			id: 1,
			apiToken: "secret-token",
			label: "acme",
		});
	});
});

describe("atlas > a subclass's date column declaration wins over its parent's", () => {
	class Event extends BaseEntity {
		@PrimaryKey() declare id: number;
		@Column.dateTime({ autoCreate: true }) declare happenedOn: unknown;
	}

	class DayEvent extends Event {
		@Column.date() declare happenedOn: unknown;
	}

	it("reads the subclass's config for a redeclared column", () => {
		expect(getDateColumnConfig(DayEvent).happenedOn).toEqual({
			dateOnly: true,
			autoCreate: undefined,
			autoUpdate: undefined,
		});
		expect(getDateColumnConfig(Event).happenedOn?.dateOnly).toBe(false);
	});
});
