/**
 * Connection manager — the AdonisJS Lucid `db.manager` surface. Owns a map of
 * named {@link ConnectionNode}s (config + live connection + lifecycle state) and
 * emits `connect`/`disconnect` events. `AtlasProvider` registers the connections
 * it opens at boot; user code can also `add` + `connect` (or `patch`/`release`)
 * connections at runtime.
 */

import { EventEmitter } from "node:events";
import type { ConnectionConfig } from "./AtlasProvider.js";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "./adapters/NapiDbAdapter.js";

/** Lifecycle state of a {@link ConnectionNode} (Lucid parity). */
export type ConnectionState =
	| "registered"
	| "open"
	| "migrating"
	| "closing"
	| "closed";

/** A managed connection: its config, the live handle (once open), and its state. */
export interface ConnectionNode {
	readonly name: string;
	config: ConnectionConfig;
	connection?: AsyncDatabaseConnection;
	state: ConnectionState;
}

/** Opens a live connection from a config (Lucid's dialect client — here, napi). */
export type ConnectionFactory = (
	name: string,
	config: ConnectionConfig,
) => Promise<AsyncDatabaseConnection>;

/** Default factory — opens via the Rust/NAPI driver from a {@link ConnectionConfig}. */
export async function openFromConfig(
	name: string,
	config: ConnectionConfig,
): Promise<AsyncDatabaseConnection> {
	const url = config.url ?? config.connection;
	if (!url) {
		throw new Error(
			`[atlas] connection '${name}' has no URL — set 'url' (or Lucid's 'connection').`,
		);
	}
	return createNapiConnection(
		url,
		config.pool?.min ?? config.poolMin ?? 1,
		config.pool?.max ?? config.poolMax ?? 10,
		config.pragmas,
		{
			retries: config.connectRetries,
			backoffMs: config.connectBackoffMs,
			timeoutMs: config.connectTimeoutMs,
		},
		{ debug: config.debug ?? false, connectionName: name },
	);
}

/** The Lucid `db.manager` connection manager. */
export class ConnectionManager {
	readonly #nodes = new Map<string, ConnectionNode>();
	readonly #events = new EventEmitter();
	readonly #factory: ConnectionFactory;

	constructor(factory: ConnectionFactory = openFromConfig) {
		this.#factory = factory;
	}

	/** The node map, keyed by connection name (Lucid `manager.connections`). */
	get connections(): ReadonlyMap<string, ConnectionNode> {
		return this.#nodes;
	}

	/** Register a connection config WITHOUT opening it (Lucid `manager.add`). */
	add(name: string, config: ConnectionConfig): this {
		const existing = this.#nodes.get(name);
		if (existing && existing.state === "open") return this; // don't clobber a live node
		this.#nodes.set(name, { name, config, state: "registered" });
		return this;
	}

	/**
	 * Replace a connection's config (Lucid `manager.patch`). If it is currently
	 * open, the live pool is disconnected in the BACKGROUND (in-flight queries
	 * drain) and the node returns to `registered`, so the next `connect` opens a
	 * fresh pool with the new config.
	 */
	patch(name: string, config: ConnectionConfig): this {
		const node = this.#nodes.get(name);
		if (!node) {
			this.#nodes.set(name, { name, config, state: "registered" });
			return this;
		}
		const stale = node.connection;
		node.config = config;
		node.connection = undefined;
		node.state = "registered";
		if (stale) {
			// Background disconnect — don't block patch; swallow late errors.
			void stale.close().catch(() => {});
			this.#events.emit("disconnect", node);
		}
		return this;
	}

	/**
	 * Open a connection (Lucid `manager.connect`). Idempotent — returns the live
	 * handle if already open. The connection must have been {@link add}ed first,
	 * or pass its config here.
	 */
	async connect(
		name: string,
		config?: ConnectionConfig,
	): Promise<AsyncDatabaseConnection> {
		let node = this.#nodes.get(name);
		if (!node) {
			if (!config) {
				throw new Error(
					`[atlas] no connection '${name}' registered — call manager.add(name, config) first.`,
				);
			}
			node = { name, config, state: "registered" };
			this.#nodes.set(name, node);
		}
		if (node.state === "open" && node.connection) return node.connection;
		let connection: AsyncDatabaseConnection;
		try {
			connection = await this.#factory(name, config ?? node.config);
		} catch (err) {
			// Local `error` event as `(node, error)` — the AtlasProvider bridge
			// re-emits it on the app emitter as Lucid's `db:connection:error`
			// (`[error, node]`). Guard the emit: Node's EventEmitter throws on an
			// `error` event with no listeners, which would mask the real error.
			if (this.#events.listenerCount("error") > 0) {
				this.#events.emit("error", node, err);
			}
			throw err;
		}
		node.connection = connection;
		node.state = "open";
		this.#events.emit("connect", node);
		return connection;
	}

	/**
	 * @internal Register an ALREADY-OPEN connection (used by `AtlasProvider`, which
	 * opens connections itself with its retry/rollback logic). Node state = open.
	 */
	register(
		name: string,
		config: ConnectionConfig,
		connection: AsyncDatabaseConnection,
	): void {
		this.#nodes.set(name, { name, config, connection, state: "open" });
		this.#events.emit("connect", this.#nodes.get(name));
	}

	/** Whether a connection is registered (Lucid `manager.has`). */
	has(name: string): boolean {
		return this.#nodes.has(name);
	}

	/** The connection node, or `undefined` (Lucid `manager.get`). */
	get(name: string): ConnectionNode | undefined {
		return this.#nodes.get(name);
	}

	/** Whether the pool is active — `open` or `migrating` (Lucid `manager.isConnected`). */
	isConnected(name: string): boolean {
		const state = this.#nodes.get(name)?.state;
		return state === "open" || state === "migrating";
	}

	/** The live connection handle for `name`, or `undefined` if the pool isn't active. */
	connection(name: string): AsyncDatabaseConnection | undefined {
		const node = this.#nodes.get(name);
		return node && (node.state === "open" || node.state === "migrating")
			? node.connection
			: undefined;
	}

	/**
	 * Close the connection's pool (Lucid `manager.close`). Keeps the node (state →
	 * `closed`) so it can be reopened; pass `release: true` to also remove the node
	 * entirely (equivalent to {@link release}).
	 */
	async close(name: string, release = false): Promise<void> {
		const node = this.#nodes.get(name);
		if (
			node?.connection &&
			(node.state === "open" || node.state === "migrating")
		) {
			const conn = node.connection;
			node.state = "closing";
			node.connection = undefined;
			await conn.close();
			node.state = "closed";
			this.#events.emit("disconnect", node);
		}
		if (release) this.#nodes.delete(name);
	}

	/**
	 * Close every connection's pool (Lucid `manager.closeAll`). Pass `release: true`
	 * to also remove every node from the manager.
	 */
	async closeAll(release = false): Promise<void> {
		for (const name of [...this.#nodes.keys()]) {
			await this.close(name, release);
		}
	}

	/** Close and REMOVE a connection node entirely (Lucid `manager.release`). */
	async release(name: string): Promise<void> {
		await this.close(name, true);
	}

	/**
	 * @internal Remove a node WITHOUT closing it — the caller already closed the
	 * connection (e.g. `AtlasProvider.shutdown`). Ownership-guarded: only removes
	 * when the node still points at `connection`, so a re-registered node from a
	 * newer provider isn't dropped by an older one's teardown.
	 */
	deregister(name: string, connection?: AsyncDatabaseConnection): void {
		const node = this.#nodes.get(name);
		if (!node) return;
		if (connection && node.connection !== connection) return;
		this.#nodes.delete(name);
	}

	/**
	 * Move an open connection into the `migrating` state (Lucid parity) — the pool
	 * stays active (`isConnected`/`connection` still resolve). Call {@link endMigrating}
	 * (or it's restored by the migration runner) when done.
	 */
	markMigrating(name: string): void {
		const node = this.#nodes.get(name);
		if (node?.state === "open") node.state = "migrating";
	}

	/** Restore a `migrating` connection to `open`. */
	endMigrating(name: string): void {
		const node = this.#nodes.get(name);
		if (node?.state === "migrating") node.state = "open";
	}

	/**
	 * Subscribe to lifecycle events (Node EventEmitter). `connect`/`disconnect`
	 * call the listener with the {@link ConnectionNode}; `error` calls it with
	 * `(node, error)`.
	 */
	on(
		event: "connect" | "disconnect" | "error",
		listener: (node: ConnectionNode, error?: unknown) => void,
	): this {
		this.#events.on(event, listener);
		return this;
	}
	/** One-shot {@link on}. */
	once(
		event: "connect" | "disconnect" | "error",
		listener: (node: ConnectionNode, error?: unknown) => void,
	): this {
		this.#events.once(event, listener);
		return this;
	}
	/** Remove a lifecycle listener. */
	off(
		event: "connect" | "disconnect" | "error",
		listener: (node: ConnectionNode, error?: unknown) => void,
	): this {
		this.#events.off(event, listener);
		return this;
	}
}
