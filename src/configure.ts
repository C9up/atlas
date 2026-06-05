interface Codemods {
	addProvider(importPath: string): Promise<void>;
	addEnvVars(vars: Record<string, string>): Promise<void>;
	writeFile(
		filePath: string,
		content: string,
		options?: { force?: boolean },
	): Promise<void>;
}

export async function configure(codemods: Codemods): Promise<void> {
	await codemods.addProvider("@c9up/atlas/provider");
	await codemods.addEnvVars({
		DB_CONNECTION: "postgres",
		DB_HOST: "localhost",
		DB_PORT: "5432",
		DB_DATABASE: "ream",
		DB_USER: "postgres",
		DB_PASSWORD: "secret",
	});
	await codemods.writeFile(
		"config/database.ts",
		`import { defineConfig } from '@c9up/atlas'

export default defineConfig({
  connection: process.env.DB_CONNECTION ?? 'postgres',
  connections: {
    postgres: {
      host: process.env.DB_HOST ?? 'localhost',
      port: Number(process.env.DB_PORT ?? '5432'),
      database: process.env.DB_DATABASE ?? 'ream',
    },
  },
})
`,
	);
}
