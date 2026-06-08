/**
 * Single source of truth for dialect detection. Both `AtlasProvider` and
 * `NapiDbAdapter` used to ship their own copy — if a new driver scheme was
 * added (e.g. `cockroachdb://`), only one side would be updated and the two
 * would silently drift. Extracted here so every call site resolves the same
 * way.
 */
import type { AtlasDialect } from "../query/native.js";

export function dialectFromUrl(url: string): AtlasDialect {
	if (url.startsWith("postgres://") || url.startsWith("postgresql://"))
		return "postgres";
	if (url.startsWith("mysql://") || url.startsWith("mariadb://"))
		return "mysql";
	return "sqlite";
}
