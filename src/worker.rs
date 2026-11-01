//! Command-driven background worker. Trait + runner; implementors define
//! commands and state. Modelled on `sqlx-sqlite/src/connection/worker.rs`.
//!
//! The in-tree consumer is the TCP connection actor in
//! `crate::tcp::connection_actor`.

use std::future::Future;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::MissedTickBehavior;

use tracing::{Instrument, Span, debug, trace};

/// A long-running command-driven background task. Futures must be
/// `Send`; the runner is spawned on the multi-threaded tokio runtime.
pub(crate) trait CommandWorker: Send + 'static {
    /// Commands this worker accepts. Embed reply channels in the
    /// command variants (`oneshot::Sender<R>` for single replies,
    /// `mpsc::Sender<R>` for streams).
    type Command: Send + 'static;

    /// Process one command. Sequential, `&mut self`. Errors surface
    /// through the reply channel; per-command failures don't exit
    /// the worker.
    fn handle(&mut self, cmd: Self::Command) -> impl Future<Output = ()> + Send + '_;

    /// Fires every [`idle_interval`][Self::idle_interval] when no
    /// command is pending. Default: no-op.
    fn on_idle(&mut self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    /// Period for [`on_idle`][Self::on_idle]. `None` disables
    /// periodic ticks.
    fn idle_interval(&self) -> Option<Duration> {
        None
    }

    /// Final-cleanup hook before the task exits. Not called on
    /// panic.
    fn on_shutdown(&mut self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    /// Tracing span / diagnostic label. Always implement -- there's
    /// no sensible generic default.
    fn name() -> &'static str;

    /// What to do when an idle-tick fires later than scheduled (after
    /// a long [`handle`][Self::handle] call or a paused runtime).
    /// Default is [`Skip`][MissedTickBehavior::Skip]: drop the
    /// backlog, schedule the next tick from now. The right choice
    /// for flush timers -- after a 30 s pause we want ONE flush,
    /// not 6 back-to-back. Override for workers that need every
    /// missed tick to fire.
    fn missed_tick_behavior() -> MissedTickBehavior {
        MissedTickBehavior::Skip
    }
}

/// Owns a spawned worker task. Drop triggers implicit graceful
/// shutdown (drain + [`CommandWorker::on_shutdown`] + exit).
/// [`WorkerControl::shutdown`] does the same explicitly and waits for
/// the [`JoinHandle`].
#[must_use = "dropping triggers shutdown without awaiting the task; \
              call `.shutdown().await` or `let _ = control;`"]
pub(crate) struct WorkerControl<C> {
    tx: mpsc::Sender<(C, Span)>,
    /// Explicit shutdown signal. Needed because outstanding
    /// [`WorkerHandle`] clones hold their own `tx` clones and would
    /// keep the channel alive otherwise.
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// `Option` so [`shutdown`][WorkerControl::shutdown] can `take`
    /// the handle to consume-await it while still letting `Drop` run.
    join: Option<JoinHandle<()>>,
    name: &'static str,
}

/// Cheap-clone send-side handle. Does NOT keep the worker alive;
/// [`WorkerControl`] does.
pub(crate) struct WorkerHandle<C> {
    tx: mpsc::Sender<(C, Span)>,
    name: &'static str,
}

// Manual Clone: derive would add `C: Clone` bound, but `mpsc::Sender<T>`
// is `Clone` for any `T`.
impl<C> Clone for WorkerHandle<C> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            name: self.name,
        }
    }
}

impl<C> std::fmt::Debug for WorkerControl<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The shutdown sender and join handle are not printable and say
        // nothing a reader wants; `alive` already reports the state.
        f.debug_struct("WorkerControl")
            .field("name", &self.name)
            .field("alive", &!self.tx.is_closed())
            .finish_non_exhaustive()
    }
}

impl<C> std::fmt::Debug for WorkerHandle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("name", &self.name)
            .field("alive", &!self.tx.is_closed())
            .finish()
    }
}

impl<C: Send + 'static> WorkerControl<C> {
    /// Cheap-clone send-side handle.
    pub(crate) fn handle(&self) -> WorkerHandle<C> {
        WorkerHandle {
            tx: self.tx.clone(),
            name: self.name,
        }
    }

    /// Signal shutdown, await the task, report status. Distinct from
    /// `Drop` in that it waits and surfaces panics.
    ///
    /// # Errors
    ///
    /// [`JoinError`] if the worker panicked.
    pub(crate) async fn shutdown(mut self) -> Result<(), JoinError> {
        debug!(target: "clickhouse::worker", worker = self.name, "explicit shutdown requested");
        // Signal first so the runner exits even with outstanding handles.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Take and await the JoinHandle. `self.tx` will drop with `self`
        // at the end of this call (closes the channel if no other senders).
        match self.join.take() {
            Some(j) => j.await,
            None => Ok(()),
        }
    }
}

impl<C> Drop for WorkerControl<C> {
    /// Implicit shutdown. Signals exit; does not wait. Use
    /// [`shutdown`][WorkerControl::shutdown] to await completion.
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl<C: Send + 'static> WorkerHandle<C> {
    /// Send one command. Current [`Span`] propagated.
    ///
    /// # Errors
    ///
    /// [`SendError`] if the worker exited.
    pub(crate) async fn send(&self, cmd: C) -> Result<(), SendError<C>> {
        self.tx
            .send((cmd, Span::current()))
            .await
            .map_err(|e| SendError(e.0.0))
    }
}

/// Worker exited; command returned for caller-side recovery or DLQ.
/// `#[non_exhaustive]` reserves room for future fields (e.g. exit
/// reason); access the command via `err.0`.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) struct SendError<C>(pub C);

impl<C> std::fmt::Display for SendError<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker has exited; cannot send command")
    }
}

impl<C: std::fmt::Debug> std::error::Error for SendError<C> {}

/// Spawn a [`CommandWorker`]. `capacity` is the mpsc channel size
/// (16-64 typical; ~8K for high-fan-in inserters). Returns a
/// [`WorkerControl`] owning the task.
///
/// # Panics
///
/// Runner doesn't catch panics. Panics in `handle` / `on_idle` /
/// `on_shutdown` surface as [`JoinError`] via
/// [`WorkerControl::shutdown`]. Worker is then considered broken;
/// discard the inserter (matches `sqlx-sqlite` policy).
pub(crate) fn spawn<W: CommandWorker>(worker: W, capacity: usize) -> WorkerControl<W::Command> {
    let name = W::name();
    let (tx, rx) = mpsc::channel(capacity);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let span = tracing::info_span!(target: "clickhouse::worker", "worker", worker = name);
    let join = tokio::spawn(run::<W>(worker, rx, shutdown_rx).instrument(span));
    WorkerControl {
        tx,
        shutdown_tx: Some(shutdown_tx),
        join: Some(join),
        name,
    }
}

async fn run<W: CommandWorker>(
    mut worker: W,
    mut rx: mpsc::Receiver<(W::Command, Span)>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let name = W::name();
    debug!(target: "clickhouse::worker", worker = name, "started");

    let interval = worker.idle_interval();
    let missed_tick = W::missed_tick_behavior();
    let mut idle = interval.map(|period| {
        let mut iv = tokio::time::interval(period);
        iv.set_missed_tick_behavior(missed_tick);
        iv
    });

    loop {
        // Two arms rather than an `if let`: each is a distinct `select!`, and
        // the idle branch only exists when a tick interval was configured.
        #[allow(clippy::single_match_else)]
        let next = match idle.as_mut() {
            Some(iv) => tokio::select! {
                biased;
                _ = &mut shutdown_rx => None,
                cmd = rx.recv() => cmd.map(NextEvent::Command),
                _ = iv.tick() => Some(NextEvent::Idle),
            },
            None => tokio::select! {
                biased;
                _ = &mut shutdown_rx => None,
                cmd = rx.recv() => cmd.map(NextEvent::Command),
            },
        };

        match next {
            Some(NextEvent::Command((cmd, caller_span))) => {
                // `Instrument`, not `Span::enter`: an `Entered` guard held
                // across an `.await` bleeds the caller's span onto
                // whatever the runtime schedules next on this thread.
                trace!(target: "clickhouse::worker", worker = name, "handling command");
                // Shutdown interrupts a command in flight: one waiting on a
                // silent peer with no deadline would otherwise outlive the
                // pool that recycled it, socket and all.
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        debug!(target: "clickhouse::worker", worker = name, "shutdown interrupted a command");
                        break;
                    }
                    () = worker.handle(cmd).instrument(caller_span) => {}
                }
            }
            Some(NextEvent::Idle) => {
                trace!(target: "clickhouse::worker", worker = name, "idle tick");
                worker.on_idle().await;
            }
            None => break, // shutdown signal OR all senders dropped -- graceful exit
        }
    }

    debug!(target: "clickhouse::worker", worker = name, "draining; running on_shutdown");
    worker.on_shutdown().await;
    debug!(target: "clickhouse::worker", worker = name, "exited");
}

enum NextEvent<C> {
    Command((C, Span)),
    Idle,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::{Notify, oneshot};

    /// Toy worker: a counter with increment/get/wait-for-shutdown.
    struct Counter {
        n: u64,
        idle_ticks: Arc<AtomicU64>,
        on_shutdown_fired: Arc<Notify>,
    }

    #[derive(Debug)]
    enum CounterCmd {
        Inc,
        Get(oneshot::Sender<u64>),
    }

    impl CommandWorker for Counter {
        type Command = CounterCmd;
        fn name() -> &'static str {
            "counter-test"
        }

        fn handle(&mut self, cmd: CounterCmd) -> impl Future<Output = ()> + Send + '_ {
            match cmd {
                CounterCmd::Inc => self.n += 1,
                CounterCmd::Get(reply) => {
                    let _ = reply.send(self.n);
                }
            }
            std::future::ready(())
        }

        fn idle_interval(&self) -> Option<Duration> {
            // Fast tick for tests; real workers use seconds.
            Some(Duration::from_millis(20))
        }

        fn on_idle(&mut self) -> impl Future<Output = ()> + Send + '_ {
            let counter = self.idle_ticks.clone();
            async move {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        fn on_shutdown(&mut self) -> impl Future<Output = ()> + Send + '_ {
            let notify = self.on_shutdown_fired.clone();
            async move {
                notify.notify_one();
            }
        }
    }

    #[tokio::test]
    async fn counter_handles_command_and_replies() {
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );
        let handle = control.handle();

        handle.send(CounterCmd::Inc).await.unwrap();
        handle.send(CounterCmd::Inc).await.unwrap();

        let (tx, rx) = oneshot::channel();
        handle.send(CounterCmd::Get(tx)).await.unwrap();
        assert_eq!(rx.await.unwrap(), 2);

        control.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn handle_clones_send_independently() {
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );

        let h1 = control.handle();
        let h2 = control.handle();
        h1.send(CounterCmd::Inc).await.unwrap();
        h2.send(CounterCmd::Inc).await.unwrap();

        let (tx, rx) = oneshot::channel();
        h1.send(CounterCmd::Get(tx)).await.unwrap();
        assert_eq!(rx.await.unwrap(), 2);

        control.shutdown().await.unwrap();
    }

    // Paused time: on a loaded host a real 120 ms sleep can see two 20 ms ticks.
    #[tokio::test(start_paused = true)]
    async fn idle_ticks_fire_when_quiet() {
        let idle_ticks = Arc::new(AtomicU64::new(0));
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: idle_ticks.clone(),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );

        // Wait long enough for several ticks.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let ticks = idle_ticks.load(Ordering::Relaxed);
        assert!(
            ticks >= 3,
            "expected several idle ticks within 120 ms, got {ticks}"
        );

        control.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn on_shutdown_fires_when_handles_dropped() {
        let notify = Arc::new(Notify::new());
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: notify.clone(),
            },
            8,
        );

        // Drop the control -- the worker should run on_shutdown then exit.
        drop(control);

        // The notify should fire shortly.
        tokio::time::timeout(Duration::from_millis(200), notify.notified())
            .await
            .expect("on_shutdown did not fire within 200 ms");
    }

    #[tokio::test]
    async fn explicit_shutdown_returns_after_drain() {
        let notify = Arc::new(Notify::new());
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: notify.clone(),
            },
            8,
        );

        control.handle().send(CounterCmd::Inc).await.unwrap();
        control.shutdown().await.unwrap();

        // Already-fired before shutdown returned.
        let already_fired =
            tokio::time::timeout(Duration::from_millis(10), notify.notified()).await;
        assert!(
            already_fired.is_ok(),
            "on_shutdown should fire by the time shutdown returns"
        );
    }

    #[tokio::test]
    async fn send_after_shutdown_returns_send_error_with_command() {
        let control = spawn(
            Counter {
                n: 0,
                idle_ticks: Arc::new(AtomicU64::new(0)),
                on_shutdown_fired: Arc::new(Notify::new()),
            },
            8,
        );
        let handle = control.handle();
        control.shutdown().await.unwrap();

        let result = handle.send(CounterCmd::Inc).await;
        assert!(result.is_err(), "send after shutdown should fail");
        // The original command is preserved for caller-side recovery.
        let SendError(_cmd) = result.unwrap_err();
    }

    #[test]
    fn worker_control_and_handle_are_send_sync() {
        // Compile-time only: if `WorkerControl<C>` / `WorkerHandle<C>`
        // stop being `Send + Sync` for a normal `Send + 'static`
        // command type, this test stops compiling. See `rust.md`
        // "Send + Sync Discipline".
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WorkerControl<()>>();
        assert_send_sync::<WorkerHandle<()>>();
        assert_send_sync::<SendError<()>>();
    }

    /// A command that never finishes, as a socket read with no deadline does,
    /// still gives way to shutdown, and `on_shutdown` still runs: otherwise
    /// the pool's recycle times out and leaves the worker and its socket
    /// running.
    #[tokio::test]
    async fn shutdown_interrupts_a_command_that_never_finishes() {
        struct Stuck(Arc<Notify>, Arc<Notify>);
        impl CommandWorker for Stuck {
            type Command = ();
            fn name() -> &'static str {
                "stuck"
            }
            async fn handle(&mut self, (): ()) {
                self.0.notify_one();
                std::future::pending::<()>().await;
            }
            fn on_shutdown(&mut self) -> impl Future<Output = ()> + Send + '_ {
                let fired = Arc::clone(&self.1);
                async move { fired.notify_one() }
            }
        }

        let (started, shut) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let control = spawn(Stuck(Arc::clone(&started), Arc::clone(&shut)), 1);
        control.handle().send(()).await.unwrap();
        started.notified().await;

        tokio::time::timeout(Duration::from_secs(2), control.shutdown())
            .await
            .expect("shutdown must not wait on the stuck command")
            .unwrap();
        tokio::time::timeout(Duration::from_millis(10), shut.notified())
            .await
            .expect("on_shutdown runs after the interrupted command");
    }

    // Paused time: the sleep below completes only once the worker has run.
    #[tokio::test(start_paused = true)]
    async fn worker_panic_surfaces_as_join_error() {
        // Runner does not catch panics; they exit the worker task and
        // surface to the caller via `JoinHandle` -> `JoinError`.
        struct Panicky;
        impl CommandWorker for Panicky {
            type Command = ();
            fn name() -> &'static str {
                "panicky"
            }
            async fn handle(&mut self, (): ()) {
                panic!("intentional test panic");
            }
        }

        let control = spawn(Panicky, 1);
        // Send the poison command. The send itself succeeds because
        // the channel has capacity; the worker panics while processing.
        control.handle().send(()).await.unwrap();

        // Give the worker time to start processing the command and
        // panic. Without this, `biased;` in the runner's select means
        // an immediate shutdown signal can win the race and the task
        // exits cleanly before the panic ever fires.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Explicit shutdown surfaces the JoinError. The runner does
        // not catch panics, so the JoinHandle resolves to Err.
        let result = control.shutdown().await;
        assert!(
            result.is_err(),
            "shutdown after worker panic should return Err(JoinError); \
             got {result:?}"
        );
        let join_err = result.unwrap_err();
        assert!(
            join_err.is_panic(),
            "JoinError should indicate a panic; got {join_err:?}"
        );
    }
}
