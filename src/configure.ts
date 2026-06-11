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
		DB_HOST: "localhost",
		DB_PORT: "5432",
		DB_DATABASE: "ream",
		DB_USER: "postgres",
		DB_PASSWORD: "change-me",
	});
	await codemods.writeFile(
		"config/database.ts",
		`import { defineConfig } from '@c9up/atlas'

export default defineConfig({
  default: 'postgres',
  connections: {
    postgres: {
      url:
        process.env.DATABASE_URL ??
        \`postgres://\${process.env.DB_USER ?? 'postgres'}:\${process.env.DB_PASSWORD ?? ''}@\${process.env.DB_HOST ?? 'localhost'}:\${process.env.DB_PORT ?? '5432'}/\${process.env.DB_DATABASE ?? 'ream'}\`,
    },
  },
})
`,
	);
}
