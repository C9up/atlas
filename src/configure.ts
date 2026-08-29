interface Codemods {
	addProvider(importPath: string): Promise<void>;
	registerCommand(importPath: string): Promise<void>;
	addEnvVars(vars: Record<string, string>): Promise<void>;
	writeFile(
		filePath: string,
		content: string,
		options?: { force?: boolean },
	): Promise<void>;
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
	await codemods.writeFile(
		"config/database.ts",
		`import { defineConfig } from '@c9up/atlas'

export default defineConfig({
  connection: 'postgres',
  connections: {
    postgres: {
      url:
        process.env.DATABASE_URL ??
        \`postgres://\${process.env.DB_USER ?? 'postgres'}:\${process.env.DB_PASSWORD ?? ''}@\${process.env.DB_HOST ?? 'localhost'}:\${process.env.DB_PORT ?? '5432'}/\${process.env.DB_DATABASE ?? 'ream'}\`,
      migrations: {
        paths: ['database/migrations'],
      },
      seeders: {
        paths: ['database/seeders'],
      },
    },
  },
})
`,
	);
}
