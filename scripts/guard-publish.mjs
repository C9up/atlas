// Refuse a LOCAL `npm/pnpm publish`. Atlas's prebuilt NAPI binaries
// (index.<suffix>.node / db.<suffix>.node) ship in one npm package per
// platform, `@c9up/atlas-<suffix>` (npm/<suffix>/), which only the CI matrix
// builds and the publish job fills and publishes right before this package,
// whose optionalDependencies pin them. A local publish would put out a main
// package whose pinned platform packages may not exist at its version, so it
// would install and then fail to load. Publishing MUST go through the
// workflow_dispatch CI job (which sets CI=true).
//
// A footgun guard, not a control: `CI=1 pnpm publish` walks past it. What
// actually ties a version to the workflow is the provenance the publish job
// signs, and npm trusted publishing once it replaces the token.
if (!process.env.CI) {
	console.error(
		"@c9up/atlas: refusing a local publish.\n" +
			"Publish only via the CI workflow — it builds the NAPI binaries for\n" +
			"every platform and publishes their platform packages first. A local\n" +
			"`npm publish` would pin platform packages that may not exist.\n" +
			"Run the publish workflow instead.",
	);
	process.exit(1);
}
