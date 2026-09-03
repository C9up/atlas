/**
 * Teach ream's `ContainerBindings` what `container.make('db')` returns.
 *
 * ream declares that interface open on purpose: it registers its own entries
 * and expects each package to contribute the one it owns — the comment on the
 * interface names `db` (atlas) as exactly this. Nothing filled it in, so
 * resolving by the string token answered `unknown` and every call site had to
 * assert a type it could not prove.
 *
 * Loaded from the package barrel and from the provider, so registering atlas
 * is enough — an application writes no `declare module` of its own.
 *
 * Type-only, and ream is an OPTIONAL peer: nothing here reaches a runtime
 * import, the provider still duck-types its host, and a `declare module` for a
 * specifier that does not resolve is simply inert. Atlas stays usable with no
 * framework at all.
 */

// Referenced so the augmentation below resolves the module it augments.
import type {} from "@c9up/ream/types";
import type { AsyncDatabaseConnection } from "./adapters/NapiDbAdapter.js";

declare module "@c9up/ream/types" {
	interface ContainerBindings {
		/** The default connection, bound by `AtlasProvider`. */
		"atlas.db": AsyncDatabaseConnection;
		/**
		 * The same binding under the name it had before the token carried its
		 * package. Kept bound so an existing `container.make(...)` resolves.
		 */
		db: AsyncDatabaseConnection;
		/** The default connection, under the longer name. */
		"atlas.db.connection": AsyncDatabaseConnection;
		/**
		 * The same binding under the name it had before the token carried its
		 * package. Kept bound so an existing `container.make(...)` resolves.
		 */
		"db.connection": AsyncDatabaseConnection;
		/**
		 * One named connection per entry in the config — `db:primary`,
		 * `db:replica`. A template-literal key rather than an enumeration,
		 * because the names come from an application's config file and this
		 * package cannot know them.
		 */
		[
			connection: `atlas.db:${string}` | `db:${string}`
		]: AsyncDatabaseConnection;
	}
}
