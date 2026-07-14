// Refuse a LOCAL `npm/pnpm publish`. Atlas ships prebuilt multi-platform NAPI
// binaries (index.<suffix>.node / db.<suffix>.node) that only the CI matrix
// assembles + downloads before publishing. A local publish would package
// whatever single-platform / stale `.node` files happen to be on disk, so the
// published tarball would be broken on every other platform. Publishing MUST go
// through the workflow_dispatch CI job (which sets CI=true).
if (!process.env.CI) {
	console.error(
		"@c9up/atlas: refusing a local publish.\n" +
			"Publish only via the CI workflow — it builds and bundles the NAPI\n" +
			"binaries for every platform. A local `npm publish` would ship\n" +
			"incomplete/stale `.node` files. Run the publish workflow instead.",
	);
	process.exit(1);
}
