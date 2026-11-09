//! PostgreSQL `LISTEN` / `NOTIFY` on a connection of its own.
//!
//! A listening connection waits on the server indefinitely, so it cannot come
//! from the pool: it would hold one of the application's connections for the
//! life of the process, and a pool of one would deadlock. [`Listener`] opens its
//! own, with the pool's connect options (TLS included), and keeps it in a
//! background task. The task re-establishes the connection when it drops,
//! subscribes again to every channel, and reports the reconnection, because
//! notifications sent while it was down are gone.

use std::time::Duration;

use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::sync::{mpsc, oneshot, Mutex};

/// What the background task reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenEvent {
    /// A `NOTIFY` on a channel this listener subscribed to.
    Notification {
        channel: String,
        payload: String,
        process_id: u32,
    },
    /// The connection dropped and is back, every channel subscribed again.
    /// Anything notified in between was lost.
    Reconnected,
    /// The connection is down and could not be re-established yet; the task
    /// keeps trying.
    Error(String),
}

enum Command {
    Listen(String, oneshot::Sender<Result<(), String>>),
    Unlisten(String, oneshot::Sender<Result<(), String>>),
    Close(oneshot::Sender<()>),
}

/// Delay before trying a dropped connection again, doubling up to the cap.
const RETRY_FIRST: Duration = Duration::from_millis(250);
const RETRY_CAP: Duration = Duration::from_secs(30);

/// A dedicated `LISTEN` connection. Dropping it stops the task.
pub struct Listener {
    commands: mpsc::UnboundedSender<Command>,
    events: Mutex<mpsc::UnboundedReceiver<ListenEvent>>,
}

impl Listener {
    /// Open a connection with `pool`'s connect options, outside the pool.
    pub async fn open(pool: &PgPool) -> Result<Self, String> {
        let own = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            // A listening connection is idle by design: neither limit applies.
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with((*pool.connect_options()).clone())
            .await
            .map_err(|e| format!("E_ATLAS_LISTEN_CONNECT: {}", e))?;
        let listener = PgListener::connect_with(&own)
            .await
            .map_err(|e| format!("E_ATLAS_LISTEN_CONNECT: {}", e))?;
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = mpsc::unbounded_channel();
        tokio::spawn(run(listener, own, command_rx, event_tx));
        Ok(Self {
            commands,
            events: Mutex::new(events),
        })
    }

    /// `LISTEN channel`. Resolves once the server has acknowledged it, so a
    /// `NOTIFY` sent afterwards is delivered.
    pub async fn listen(&self, channel: &str) -> Result<(), String> {
        validate_channel(channel)?;
        self.ask(|reply| Command::Listen(channel.to_owned(), reply))
            .await?
    }

    /// `UNLISTEN channel`.
    pub async fn unlisten(&self, channel: &str) -> Result<(), String> {
        validate_channel(channel)?;
        self.ask(|reply| Command::Unlisten(channel.to_owned(), reply))
            .await?
    }

    /// The next event; `None` once the listener is closed.
    pub async fn next(&self) -> Option<ListenEvent> {
        self.events.lock().await.recv().await
    }

    /// Stop the task and close the connection. Idempotent.
    pub async fn close(&self) {
        let (reply, done) = oneshot::channel();
        if self.commands.send(Command::Close(reply)).is_ok() {
            let _ = done.await;
        }
    }

    async fn ask<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T, String> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(command(reply))
            .map_err(|_| "E_ATLAS_LISTEN_CLOSED: the listener is closed".to_string())?;
        answer
            .await
            .map_err(|_| "E_ATLAS_LISTEN_CLOSED: the listener is closed".to_string())
    }
}

/// A channel is an identifier: the server truncates it past 63 bytes, which
/// would make two distinct names one channel, and a NUL would cut it short.
pub fn validate_channel(channel: &str) -> Result<(), String> {
    if channel.is_empty() || channel.len() > 63 || channel.contains('\0') {
        return Err(format!(
            "E_ATLAS_LISTEN_CHANNEL: a channel name is 1 to 63 bytes without NUL, got {:?}",
            channel
        ));
    }
    Ok(())
}

async fn run(
    mut listener: PgListener,
    pool: PgPool,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<ListenEvent>,
) {
    // Set while the connection is down and could not be re-established: when
    // to try again, and how long to wait after that if it fails once more.
    let mut retry: Option<(tokio::time::Instant, Duration)> = None;
    loop {
        if let Some((at, delay)) = retry {
            tokio::select! {
                command = commands.recv() => {
                    if !handle(&mut listener, command).await { break; }
                }
                _ = tokio::time::sleep_until(at) => {
                    // Any statement through the listener reconnects it and
                    // subscribes to every channel again.
                    match sqlx::query("SELECT 1").execute(&mut listener).await {
                        Ok(_) => {
                            retry = None;
                            let _ = events.send(ListenEvent::Reconnected);
                        }
                        Err(e) => {
                            let _ = events.send(ListenEvent::Error(e.to_string()));
                            let next = (delay * 2).min(RETRY_CAP);
                            retry = Some((tokio::time::Instant::now() + delay, next));
                        }
                    }
                }
            }
            continue;
        }
        tokio::select! {
            command = commands.recv() => {
                if !handle(&mut listener, command).await { break; }
            }
            // Cancel-safe: a message is consumed only once fully read.
            received = listener.try_recv() => match received {
                Ok(Some(n)) => {
                    let _ = events.send(ListenEvent::Notification {
                        channel: n.channel().to_owned(),
                        payload: n.payload().to_owned(),
                        process_id: n.process_id(),
                    });
                }
                // The connection dropped and was re-established in place.
                Ok(None) => {
                    let _ = events.send(ListenEvent::Reconnected);
                }
                Err(e) => {
                    let _ = events.send(ListenEvent::Error(e.to_string()));
                    retry = Some((tokio::time::Instant::now() + RETRY_FIRST, RETRY_FIRST * 2));
                }
            }
        }
    }
    pool.close().await;
}

/// Apply one command; `false` when the task must stop.
async fn handle(listener: &mut PgListener, command: Option<Command>) -> bool {
    match command {
        Some(Command::Listen(channel, reply)) => {
            let _ = reply.send(listener.listen(&channel).await.map_err(|e| e.to_string()));
            true
        }
        Some(Command::Unlisten(channel, reply)) => {
            let _ = reply.send(listener.unlisten(&channel).await.map_err(|e| e.to_string()));
            true
        }
        Some(Command::Close(reply)) => {
            let _ = listener.unlisten_all().await;
            let _ = reply.send(());
            false
        }
        // Every handle is gone.
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::validate_channel;

    #[test]
    fn a_channel_name_is_an_identifier_the_server_keeps_whole() {
        assert!(validate_channel("jobs").is_ok());
        assert!(validate_channel("Jobs.Done-1").is_ok());
        assert!(validate_channel(&"x".repeat(63)).is_ok());
        assert!(validate_channel("").is_err());
        assert!(validate_channel(&"x".repeat(64)).is_err());
        assert!(validate_channel("jobs\0done").is_err());
    }
}
