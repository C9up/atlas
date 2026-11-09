/**
 * PostgreSQL `LISTEN` / `NOTIFY` for a napi connection.
 *
 * Every subscription of a connection shares ONE dedicated connection, opened
 * outside the pool on the first `listen()` and closed when the last
 * subscription stops (or the connection closes), so a listening process
 * neither holds a pooled connection nor stays alive once it stops listening.
 * The Rust side re-establishes a dropped connection and subscribes again;
 * notifications sent in between are lost, which `onReconnect` reports.
 */

import { AtlasError } from "../errors.js";
import type { ReamListener } from "../native/generated.js";

/** A notification as the server delivered it. */
export interface Notification {
	channel: string;
	/** The `NOTIFY` payload; an empty string when none was sent. */
	payload: string;
	/** Backend process id of the session that sent it. */
	processId: number;
}

/** Called with each notification on the channel. */
export type NotificationHandler = (
	payload: string,
	notification: Notification,
) => void | Promise<void>;

/** Options of {@link ListenFn}. */
export interface ListenOptions {
	/**
	 * The listening connection dropped and is back, every channel subscribed
	 * again. Whatever was notified in between was lost: catch up here.
	 */
	onReconnect?: () => void;
	/**
	 * The listening connection is down and could not be re-established yet
	 * (atlas keeps trying), or the handler threw. Without it, the error is
	 * reported as a process warning.
	 */
	onError?: (error: Error) => void;
}

/** Stop a subscription; the last one on a channel sends `UNLISTEN`. */
export type Unlisten = () => Promise<void>;

/**
 * Subscribe `handler` to `channel`. Resolves once the server acknowledged the
 * `LISTEN`, so a `NOTIFY` sent afterwards is delivered. PostgreSQL only.
 */
export type ListenFn = (
	channel: string,
	handler: NotificationHandler,
	options?: ListenOptions,
) => Promise<Unlisten>;

interface Subscription {
	handler: NotificationHandler;
	options: ListenOptions;
}

type ListenEvent =
	| { kind: "notification"; notification: Notification }
	| { kind: "reconnected" }
	| { kind: "error"; message: string };

/** Read what the native listener reports; anything else is a broken contract. */
function parseEvent(json: string): ListenEvent {
	const raw: unknown = JSON.parse(json);
	if (typeof raw === "object" && raw !== null) {
		const kind = Reflect.get(raw, "kind");
		if (kind === "reconnected") return { kind };
		if (kind === "error")
			return { kind, message: String(Reflect.get(raw, "message")) };
		if (kind === "notification") {
			return {
				kind,
				notification: {
					channel: String(Reflect.get(raw, "channel")),
					payload: String(Reflect.get(raw, "payload")),
					processId: Number(Reflect.get(raw, "processId")),
				},
			};
		}
	}
	throw new AtlasError(
		"E_ATLAS_LISTEN_EVENT",
		`atlas: the native listener reported an event it does not define: ${json}`,
	);
}

function report(options: ListenOptions, error: Error): void {
	if (options.onError) options.onError(error);
	else process.emitWarning(error);
}

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}

/**
 * The `listen` of one connection, plus the `close` the connection calls when
 * it closes. `open` opens the native listener; `coded` turns a native error
 * into the {@link AtlasError} its message names.
 */
export function createListen(
	dialect: string,
	open: () => Promise<ReamListener>,
	coded: (error: unknown) => unknown,
): { listen: ListenFn; close: () => Promise<void> } {
	const channels = new Map<string, Set<Subscription>>();
	let native: ReamListener | undefined;
	// Subscribe / unsubscribe / close run one at a time: an UNLISTEN that
	// closes the connection must not race a LISTEN that still needs it.
	let queue: Promise<unknown> = Promise.resolve();
	function serial<T>(task: () => Promise<T>): Promise<T> {
		const run = queue.then(task, task);
		queue = run.then(
			() => undefined,
			() => undefined,
		);
		return run;
	}
	const call = <T>(promise: Promise<T>): Promise<T> =>
		promise.catch((error: unknown) => {
			throw coded(error);
		});

	function everySubscription(): Subscription[] {
		return [...channels.values()].flatMap((subs) => [...subs]);
	}

	async function dispatch(event: ListenEvent): Promise<void> {
		if (event.kind === "reconnected") {
			for (const sub of everySubscription()) sub.options.onReconnect?.();
			return;
		}
		if (event.kind === "error") {
			const error = new AtlasError("E_ATLAS_LISTEN_CONNECTION", event.message);
			for (const sub of everySubscription()) report(sub.options, error);
			return;
		}
		const { notification } = event;
		for (const sub of [...(channels.get(notification.channel) ?? [])]) {
			try {
				await sub.handler(notification.payload, notification);
			} catch (error) {
				report(sub.options, asError(error));
			}
		}
	}

	async function pump(listener: ReamListener): Promise<void> {
		for (;;) {
			const json = await listener.next();
			if (json === null) return;
			await dispatch(parseEvent(json));
		}
	}

	async function release(): Promise<void> {
		const listener = native;
		native = undefined;
		channels.clear();
		if (listener) await call(listener.close());
	}

	const listen: ListenFn = (channel, handler, options = {}) =>
		serial(async () => {
			if (dialect !== "postgres") {
				throw new AtlasError(
					"E_ATLAS_LISTEN_UNSUPPORTED",
					`LISTEN / NOTIFY is a PostgreSQL feature; this connection is ${dialect}.`,
				);
			}
			if (!native) {
				const opened = await call(open());
				native = opened;
				pump(opened).catch((error: unknown) => {
					for (const sub of everySubscription())
						report(sub.options, asError(error));
				});
			}
			let subs = channels.get(channel);
			if (!subs) {
				await call(native.listen(channel));
				subs = new Set();
				channels.set(channel, subs);
			}
			const sub: Subscription = { handler, options };
			subs.add(sub);

			let stopped = false;
			return () =>
				serial(async () => {
					if (stopped) return;
					stopped = true;
					const current = channels.get(channel);
					if (!current?.delete(sub) || current.size > 0) return;
					channels.delete(channel);
					if (channels.size === 0) {
						await release();
						return;
					}
					if (native) await call(native.unlisten(channel));
				});
		});

	return { listen, close: () => serial(release) };
}
