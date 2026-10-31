//! Per-connection worker task and its message protocol.
//!
//! Each pooled connection is owned by a [`ConnectionTask`] running in a
//! dedicated tokio task; callers interact with it through a
//! [`ConnectionHandle`] by sending [`ConnectionOperation`] messages over
//! an mpsc channel and awaiting a oneshot response. This serializes
//! access to the underlying driver connection — drivers are not
//! required to be `Sync` on `exec` — and gives the connection its own
//! lifecycle independent of the pool object that holds the handle.
//!
//! With [`Builder::inline_connections`](crate::db::Builder::inline_connections),
//! there is no task: the caller runs the operation itself, on its own task,
//! holding the connection's lock. Drivers whose statements depend on the
//! caller's task-local context (its transaction, its span) need this.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use toasty_core::driver::operation::RawSql;
use toasty_core::driver::{Connection, Rows};
#[cfg(feature = "migration")]
use toasty_core::schema::db::{AppliedMigration, Migration};
use toasty_core::stmt::Value;
use tokio::sync::{mpsc, oneshot};
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;
use tracing::Instrument;

use super::pool::SweepWaker;
use crate::engine::Engine;

/// Operations sent to the connection task.
///
/// Each variant carries the caller's `tracing::Span`, captured at send time.
/// The worker task runs on its own tokio task, so span context does not
/// propagate to it implicitly; the task enters the carried span while
/// handling the operation so engine and driver events land inside the
/// caller's span tree.
pub(crate) enum ConnectionOperation {
    /// Execute a statement (compile + run on the connection).
    ExecStatement {
        stmt: Box<toasty_core::stmt::Statement>,
        in_transaction: bool,
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<toasty_core::driver::ExecResponse>>,
    },
    ExecOperation {
        operation: Box<toasty_core::driver::operation::Operation>,
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<toasty_core::driver::ExecResponse>>,
    },
    /// Execute user-authored SQL through the engine's SQL capability guard.
    ExecRawSql {
        raw: Box<RawSql>,
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<toasty_core::driver::ExecResponse>>,
    },
    /// Push schema to the database.
    PushSchema {
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<()>>,
    },
    /// Read migration IDs recorded by the driver.
    #[cfg(feature = "migration")]
    AppliedMigrations {
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<Vec<AppliedMigration>>>,
    },
    /// Apply and record one migration.
    #[cfg(feature = "migration")]
    ApplyMigration {
        id: u64,
        name: String,
        migration: Box<Migration>,
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<()>>,
    },
    /// Active liveness probe issued by the pool's background sweep
    /// (and, eventually, per-acquire pre-ping). Routes through the
    /// connection task so the ping is serialized against any in-flight
    /// `exec` on the same connection.
    Ping {
        span: tracing::Span,
        tx: oneshot::Sender<crate::Result<()>>,
    },
}

/// Handle to a connection: a dedicated connection task, or a connection its
/// callers run operations on themselves (inline).
pub(crate) struct ConnectionHandle {
    mode: Mode,
}

enum Mode {
    /// When dropped, `in_tx` closes the channel, causing the background task
    /// to finish processing remaining messages and exit gracefully. Not on
    /// `wasm32`, which has no Tokio runtime to spawn tasks on.
    #[cfg(not(target_arch = "wasm32"))]
    Task {
        in_tx: mpsc::UnboundedSender<ConnectionOperation>,
        join_handle: JoinHandle<()>,
    },
    Inline {
        task: tokio::sync::Mutex<ConnectionTask>,
        /// Set once the connection went bad.
        closed: Arc<AtomicBool>,
        /// Operations sent without waiting (a dropped transaction's
        /// rollback): they run before the next one.
        detached: Mutex<Vec<ConnectionOperation>>,
    },
}

impl ConnectionHandle {
    /// Spawn a worker task that owns `connection` and serializes
    /// operations against it. `sweep_waker` is woken whenever the task
    /// observes that the connection went bad so the pool's health-check
    /// sweep can escalate immediately rather than waiting for its next
    /// periodic tick.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn spawn(
        connection: Box<dyn Connection>,
        engine: Engine,
        sweep_waker: Arc<SweepWaker>,
    ) -> Self {
        let (in_tx, in_rx) = mpsc::unbounded_channel::<ConnectionOperation>();
        let task = ConnectionTask {
            connection,
            engine,
            in_rx: Some(in_rx),
            closed: Arc::default(),
            sweep_waker,
        };
        let join_handle = tokio::spawn(task.run());
        Self {
            mode: Mode::Task { in_tx, join_handle },
        }
    }

    /// A connection whose operations run on the task sending them.
    pub(crate) fn inline(
        connection: Box<dyn Connection>,
        engine: Engine,
        sweep_waker: Arc<SweepWaker>,
    ) -> Self {
        let closed = Arc::new(AtomicBool::new(false));
        let task = ConnectionTask {
            connection,
            engine,
            in_rx: None,
            closed: closed.clone(),
            sweep_waker,
        };
        Self {
            mode: Mode::Inline {
                task: tokio::sync::Mutex::new(task),
                closed,
                detached: Mutex::default(),
            },
        }
    }

    /// Runs `op`: sends it to the connection task, or runs it on this task.
    /// Its response goes to the operation's `tx`. Fails if the connection is
    /// gone.
    pub(crate) async fn send(&self, op: ConnectionOperation) -> Result<(), ConnectionGone> {
        match &self.mode {
            #[cfg(not(target_arch = "wasm32"))]
            Mode::Task { in_tx, .. } => in_tx.send(op).map_err(|_| ConnectionGone),
            Mode::Inline {
                task,
                closed,
                detached,
            } => {
                let mut task = task.lock().await;
                let detached =
                    std::mem::take(&mut *detached.lock().unwrap_or_else(|e| e.into_inner()));
                for op in detached.into_iter().chain([op]) {
                    if closed.load(Ordering::Acquire) {
                        return Err(ConnectionGone);
                    }
                    task.handle(op).await;
                }
                Ok(())
            }
        }
    }

    /// Sends `op` without waiting for it to run: to the connection task, or,
    /// inline, to run before the next operation.
    pub(crate) fn send_detached(&self, op: ConnectionOperation) {
        match &self.mode {
            #[cfg(not(target_arch = "wasm32"))]
            Mode::Task { in_tx, .. } => {
                let _ = in_tx.send(op);
            }
            Mode::Inline { detached, .. } => {
                detached.lock().unwrap_or_else(|e| e.into_inner()).push(op);
            }
        }
    }

    /// Returns true once the connection can't run operations anymore: its
    /// task exited, or it went bad. Used by the pool's `recycle` to detect
    /// dead slots.
    pub(crate) fn is_closed(&self) -> bool {
        match &self.mode {
            #[cfg(not(target_arch = "wasm32"))]
            Mode::Task { in_tx, join_handle } => in_tx.is_closed() || join_handle.is_finished(),
            Mode::Inline { closed, .. } => closed.load(Ordering::Acquire),
        }
    }
}

/// The connection's task (or inline connection) is gone.
#[derive(Debug)]
pub(crate) struct ConnectionGone;

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("inline", &matches!(self.mode, Mode::Inline { .. }))
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Owns one database connection and serializes operations against it.
/// Spawned by [`ConnectionHandle::spawn`]; runs until `in_rx` closes
/// (sender side dropped) or the connection reports `is_valid() == false`.
/// Inline, it has no `in_rx`: its callers run [`handle`](Self::handle).
struct ConnectionTask {
    connection: Box<dyn Connection>,
    engine: Engine,
    in_rx: Option<mpsc::UnboundedReceiver<ConnectionOperation>>,
    /// Set once the connection went bad (for inline connections).
    closed: Arc<AtomicBool>,
    sweep_waker: Arc<SweepWaker>,
}

impl ConnectionTask {
    #[cfg(not(target_arch = "wasm32"))]
    async fn run(mut self) {
        while let Some(op) = self
            .in_rx
            .as_mut()
            .expect("a spawned task's channel")
            .recv()
            .await
        {
            if !self.handle(op).await {
                return;
            }
        }
    }

    /// Dispatch one operation, entering the caller's span for its duration.
    /// Returns `false` if the connection went bad during it and the task
    /// should exit.
    async fn handle(&mut self, op: ConnectionOperation) -> bool {
        match op {
            ConnectionOperation::ExecStatement {
                stmt,
                in_transaction,
                span,
                tx,
            } => {
                let result = self
                    .exec_statement(*stmt, in_transaction)
                    .instrument(span)
                    .await;
                self.respond(tx, result)
            }
            ConnectionOperation::ExecOperation {
                operation,
                span,
                tx,
            } => {
                let result = self
                    .connection
                    .exec(&self.engine.schema, *operation)
                    .instrument(span)
                    .await;
                self.respond(tx, result)
            }
            ConnectionOperation::ExecRawSql { raw, span, tx } => {
                let result = self
                    .engine
                    .exec_raw_sql(&mut *self.connection, *raw)
                    .instrument(span)
                    .await;
                self.respond(tx, result)
            }
            ConnectionOperation::PushSchema { span, tx } => {
                let result = self
                    .connection
                    .push_schema(&self.engine.schema)
                    .instrument(span)
                    .await;
                self.respond(tx, result)
            }
            #[cfg(feature = "migration")]
            ConnectionOperation::AppliedMigrations { span, tx } => {
                let result = self.connection.applied_migrations().instrument(span).await;
                self.respond(tx, result)
            }
            #[cfg(feature = "migration")]
            ConnectionOperation::ApplyMigration {
                id,
                name,
                migration,
                span,
                tx,
            } => {
                let result = self
                    .connection
                    .apply_migration(id, &name, &migration)
                    .instrument(span)
                    .await;
                self.respond(tx, result)
            }
            ConnectionOperation::Ping { span, tx } => {
                let result = self.connection.ping().instrument(span).await;
                self.respond(tx, result)
            }
        }
    }

    async fn exec_statement(
        &mut self,
        stmt: toasty_core::stmt::Statement,
        in_transaction: bool,
    ) -> crate::Result<toasty_core::driver::ExecResponse> {
        let single = stmt.is_single();
        let mut response = self
            .engine
            .exec(&mut *self.connection, stmt, in_transaction)
            .await?;
        response.values.buffer().await?;

        if single {
            let Rows::Value(Value::List(mut items)) = response.values else {
                unreachable!()
            };
            assert!(
                items.len() <= 1,
                "expected at most 1 row for single statement, got {}",
                items.len()
            );
            response.values = Rows::Value(items.pop().unwrap_or(Value::Null));
        }

        Ok(response)
    }

    /// Send `result` to the caller, but if the connection reported
    /// invalid during the op, close `in_rx` (or mark the inline connection
    /// closed) *first* so the mpsc transitions to closed synchronously. A consumer woken by the response that then
    /// re-enters the pool sees `in_tx.is_closed() == true` in
    /// `Manager::recycle` and the slot is evicted — no race with this
    /// task's exit. Returns whether the task should keep running.
    ///
    /// Also wakes the pool's background sweep so it can escalate to a
    /// full idle-pool ping pass without waiting for the next periodic
    /// tick. A real connection-lost error usually means more than one
    /// connection in the pool is affected (database restart, network
    /// event), and catching the rest eagerly turns a multi-second
    /// recovery window into one round-trip per idle slot.
    fn respond<T>(&mut self, tx: oneshot::Sender<T>, result: T) -> bool {
        if self.connection.is_valid() {
            let _ = tx.send(result);
            true
        } else {
            tracing::debug!("connection reported invalid; closing channel and exiting");
            if let Some(in_rx) = &mut self.in_rx {
                in_rx.close();
            }
            self.closed.store(true, Ordering::Release);
            self.sweep_waker.wake();
            let _ = tx.send(result);
            false
        }
    }
}
