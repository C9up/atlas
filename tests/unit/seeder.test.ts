import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { DatabaseConnection } from "../../src/BaseRepository.js";
import { AtlasError } from "../../src/errors.js";
import {
	BaseSeeder,
	runSeederDirectory,
	runSeeders,
} from "../../src/schema/Seeder.js";

function noopDb(): DatabaseConnection {
	return {
		execute() {
			return Promise.resolve({ rowsAffected: 0 });
		},
		query() {
			return Promise.resolve([]);
		},
	};
}

describe("atlas > Seeder > runSeeders", () => {
	it("invokes run() on each seeder in declaration order", async () => {
		const calls: string[] = [];
		class SeederA extends BaseSeeder {
			async run() {
				calls.push("A");
			}
		}
		class SeederB extends BaseSeeder {
			async run() {
				calls.push("B");
			}
		}
		const db = noopDb();
		await runSeeders([new SeederA(db), new SeederB(db)]);
		expect(calls).toEqual(["A", "B"]);
	});

	it("propagates a thrown error and aborts subsequent seeders", async () => {
		const calls: string[] = [];
		class SeederA extends BaseSeeder {
			async run() {
				calls.push("A");
				throw new Error("boom");
			}
		}
		class SeederB extends BaseSeeder {
			async run() {
				calls.push("B");
			}
		}
		const db = noopDb();
		await expect(
			runSeeders([new SeederA(db), new SeederB(db)]),
		).rejects.toThrow("boom");
		expect(calls).toEqual(["A"]);
	});

	it("accepts an empty list without error", async () => {
		await expect(runSeeders([])).resolves.toBeUndefined();
	});
});

describe("atlas > Seeder > runSeederDirectory", () => {
	let tmpDir: string;

	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-seeder-"));
	});

	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	const seederTemplate = (label: string) => `
import { BaseSeeder } from '${path.resolve(__dirname, "../../src/schema/Seeder.ts")}'
export default class S extends BaseSeeder {
  async run() {
    globalThis.__atlasSeederCalls__ ??= []
    globalThis.__atlasSeederCalls__.push('${label}')
  }
}
`;

	it("throws E_SEEDER_DIR_NOT_FOUND when the directory does not exist", async () => {
		await expect(
			runSeederDirectory(path.join(tmpDir, "ghost"), noopDb()),
		).rejects.toMatchObject({
			code: expect.stringContaining("E_SEEDER_DIR_NOT_FOUND"),
		});
	});

	it("imports and runs every .ts/.js seeder in alphabetical order", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "AlphaSeeder.ts"),
			seederTemplate("alpha"),
		);
		await fsp.writeFile(
			path.join(tmpDir, "BetaSeeder.ts"),
			seederTemplate("beta"),
		);

		const sentinel: { __atlasSeederCalls__?: string[] } =
			globalThis as unknown as { __atlasSeederCalls__?: string[] };
		sentinel.__atlasSeederCalls__ = [];

		const executed = await runSeederDirectory(tmpDir, noopDb());

		expect(executed).toEqual(["AlphaSeeder", "BetaSeeder"]);
		expect(sentinel.__atlasSeederCalls__).toEqual(["alpha", "beta"]);
	});

	it("filters by `options.files` (basename without extension)", async () => {
		await fsp.writeFile(path.join(tmpDir, "Keep.ts"), seederTemplate("keep"));
		await fsp.writeFile(path.join(tmpDir, "Skip.ts"), seederTemplate("skip"));

		const sentinel: { __atlasSeederCalls__?: string[] } =
			globalThis as unknown as { __atlasSeederCalls__?: string[] };
		sentinel.__atlasSeederCalls__ = [];

		const executed = await runSeederDirectory(tmpDir, noopDb(), {
			files: ["Keep"],
		});

		expect(executed).toEqual(["Keep"]);
		expect(sentinel.__atlasSeederCalls__).toEqual(["keep"]);
	});

	it("ignores .d.ts files and non-ts/.js entries", async () => {
		await fsp.writeFile(path.join(tmpDir, "X.ts"), seederTemplate("x"));
		await fsp.writeFile(path.join(tmpDir, "ignored.txt"), "irrelevant");
		await fsp.writeFile(path.join(tmpDir, "types.d.ts"), "export type T = 1");

		const sentinel: { __atlasSeederCalls__?: string[] } =
			globalThis as unknown as { __atlasSeederCalls__?: string[] };
		sentinel.__atlasSeederCalls__ = [];

		const executed = await runSeederDirectory(tmpDir, noopDb());
		expect(executed).toEqual(["X"]);
	});

	it("throws E_SEEDER_INVALID when a file has no default export class", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "Bad.ts"),
			"export const x = 'not a class'",
		);
		await expect(runSeederDirectory(tmpDir, noopDb())).rejects.toBeInstanceOf(
			AtlasError,
		);
	});
});
