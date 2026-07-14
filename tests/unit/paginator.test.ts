import { describe, expect, it } from "vitest";
import { Paginator } from "../../src/ModelQuery.js";

// 25 rows, 10 per page → 3 pages; sit on page 2 to exercise both directions.
function page2() {
	return new Paginator<{ id: number }>([{ id: 1 }], {
		total: 25,
		perPage: 10,
		currentPage: 2,
	})
		.baseUrl("/users")
		.queryString({ sort: "name" });
}

describe("atlas > Paginator URL helpers (Lucid parity)", () => {
	it("getUrl builds a page URL with baseUrl + queryString", () => {
		expect(page2().getUrl(3)).toBe("/users?sort=name&page=3");
	});

	it("getNextPageUrl / getPreviousPageUrl respect the boundaries", () => {
		const p = page2();
		expect(p.getNextPageUrl()).toBe("/users?sort=name&page=3");
		expect(p.getPreviousPageUrl()).toBe("/users?sort=name&page=1");
	});

	it("returns null past the edges", () => {
		const first = new Paginator<{ id: number }>([], {
			total: 25,
			perPage: 10,
			currentPage: 1,
		}).baseUrl("/u");
		expect(first.getPreviousPageUrl()).toBeNull();
		const last = new Paginator<{ id: number }>([], {
			total: 25,
			perPage: 10,
			currentPage: 3,
		}).baseUrl("/u");
		expect(last.getNextPageUrl()).toBeNull();
	});

	it("getUrlsForRange clamps to [1, lastPage] and flags the active page", () => {
		const range = page2().getUrlsForRange(0, 99);
		expect(range.map((r) => r.page)).toEqual([1, 2, 3]);
		expect(range.find((r) => r.isActive)?.page).toBe(2);
	});

	it("getUrl returns '' when no baseUrl was set", () => {
		const p = new Paginator<{ id: number }>([], {
			total: 5,
			perPage: 10,
			currentPage: 1,
		});
		expect(p.getUrl(1)).toBe("");
	});

	it("remaps meta key names via paginationMetaKeys (Lucid parity)", () => {
		const p = new Paginator<{ id: number }>(
			[{ id: 1 }],
			{ total: 25, perPage: 10, currentPage: 2 },
			{ total: "count", perPage: "per_page" },
		);
		const json = p.toJSON();
		expect(json.meta.count).toBe(25);
		expect(json.meta.per_page).toBe(10);
		expect("total" in json.meta).toBe(false);
		// Unmapped keys keep their default name.
		expect(json.meta.currentPage).toBe(2);
	});
});
