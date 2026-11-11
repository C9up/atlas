/**
 * A stored document shaped like the boundary's envelopes stays a document.
 *
 * Bytes and big integers cross the native boundary as `{"$bytes": …}` and
 * `{"$bigint": …}`. Both directions sniffed that shape in ANY value: writing
 * `{"$bigint": "42"}` stored the integer 42, and reading a JSON column that
 * held `{"$bytes": "AAAA"}` returned a Uint8Array. Only a real BigInt or
 * Uint8Array is an envelope now, and only a binary cell is revived as bytes.
 * Runs on SQLite always, and on Postgres/MySQL when ATLAS_TEST_PG_URL /
 * ATLAS_TEST_MYSQL_URL are set.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

interface Target {
	name: string;
	url: string;
	jsonType: string;
	blobType: string;
	placeholder: (n: number) => string;
}

const question = (): string => "?";
const dollar = (n: number): string => `$${n}`;

const TARGETS: Target[] = [
	{
		name: "sqlite",
		url: "sqlite::memory:",
		jsonType: "TEXT",
		blobType: "BLOB",
		placeholder: question,
	},
];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push({
		name: "postgres",
		url: process.env.ATLAS_TEST_PG_URL,
		jsonType: "JSONB",
		blobType: "BYTEA",
		placeholder: dollar,
	});
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push({
		name: "mysql",
		url: process.env.ATLAS_TEST_MYSQL_URL,
		jsonType: "JSON",
		blobType: "BLOB",
		placeholder: question,
	});
}

const DOCUMENTS: Array<Record<string, unknown>> = [
	{ $bigint: "42" },
	{ $bytes: "AAAA" },
	{ $json: { nested: true } },
	{ wrapper: { $bytes: "AAAA" } },
];

describe.each(TARGETS)("atlas > envelope-shaped data on $name", (target) => {
	let conn: AsyncDatabaseConnection;
	const ph = target.placeholder;

	beforeAll(async () => {
		conn = await createNapiConnection(target.url, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS envelope_docs");
		await conn.execute(
			`CREATE TABLE envelope_docs (id INTEGER PRIMARY KEY, doc ${target.jsonType}, bin ${target.blobType}, big BIGINT)`,
		);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS envelope_docs");
		await conn.close();
	});

	it.each(DOCUMENTS)(
		"stores and reads back %o as the document it is",
		async (doc) => {
			await conn.execute("DELETE FROM envelope_docs");
			// Bound as the object itself: the binder writes it as JSON text.
			await conn.execute(
				`INSERT INTO envelope_docs (id, doc) VALUES (1, ${ph(1)})`,
				[doc],
			);
			const [row] = await conn.query(
				"SELECT doc FROM envelope_docs WHERE id = 1",
			);
			const stored: unknown = row?.doc;
			expect(stored).not.toBeInstanceOf(Uint8Array);
			expect(typeof stored === "string" ? JSON.parse(stored) : stored).toEqual(
				doc,
			);
		},
	);

	it("still carries a real Uint8Array and BigInt across", async () => {
		await conn.execute("DELETE FROM envelope_docs");
		await conn.execute(
			`INSERT INTO envelope_docs (id, bin, big) VALUES (2, ${ph(1)}, ${ph(2)})`,
			[Uint8Array.from([0, 1, 2, 255]), 9007199254740993n],
		);
		const [row] = await conn.query(
			"SELECT bin, big FROM envelope_docs WHERE id = 2",
		);
		expect(row?.bin).toEqual(Uint8Array.from([0, 1, 2, 255]));
		expect(String(row?.big)).toBe("9007199254740993");
	});
});
