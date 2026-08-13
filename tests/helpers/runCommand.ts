import type { AtlasCommandClass } from "../../src/console/contract.js";

/**
 * Instantiate a command, assign its inputs, and run it.
 *
 * This is what the console kernel does once it has parsed argv. Tests skip the
 * parsing and hand the values directly, keyed by PROPERTY name (`schemaPath`),
 * not by flag name (`--schema-path`).
 */
export async function runCommand(
	Command: AtlasCommandClass,
	inputs: Record<string, unknown> = {},
): Promise<void> {
	const instance = new Command();
	Object.assign(instance, inputs);
	await instance.run();
}
