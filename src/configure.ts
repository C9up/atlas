import { stubsRoot } from "./stubs.js";

interface Codemods {
	addProvider(importPath: string): Promise<void>;
	registerCommand(importPath: string): Promise<void>;
	addEnvVars(vars: Record<string, string>): Promise<void>;
	writeFile(
		filePath: string,
		content: string,
		options?: { force?: boolean },
	): Promise<void>;
	makeUsingStub(
		stubsRoot: string,
		stubPath: string,
		state?: Record<string, string | number | boolean>,
		options?: { force?: boolean },
	): Promise<{ path: string; contents: string }>;
}

export async function configure(codemods: Codemods): Promise<void> {
	await codemods.addProvider("@c9up/atlas/provider");
	// `reamrc.commands` is how a package ships commands — the same channel Lucid
	// uses (`@adonisjs/lucid/commands`). Every migration / seeder / schema
	// command comes from here, configured by `config/database.ts`; none of them
	// needs a line in the `ream` binary.
	await codemods.registerCommand("@c9up/atlas/commands");
	await codemods.addEnvVars({
		DB_HOST: "localhost",
		DB_PORT: "5432",
		DB_DATABASE: "ream",
		DB_USER: "postgres",
		DB_PASSWORD: "change-me",
	});
	await codemods.makeUsingStub(stubsRoot, "config/database.stub");
}
