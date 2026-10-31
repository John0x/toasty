//! The pool on `wasm32`: deadpool's needs `Send` drivers and connections,
//! which `wasm32`'s need not be (a Cloudflare Worker's sockets, JS promises).
//!
//! It keeps idle connection handles for reuse and at most `max_size` checked
//! out at once. There is no timer to bound waits with: the pool's timeouts
//! don't apply.

use std::{cell::RefCell, rc::Rc, sync::Arc};

use deadpool::managed::Metrics;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{ConnectionHandle, Manager};

pub(crate) struct LocalPool {
    inner: Rc<Inner>,
}

struct Inner {
    manager: Manager,
    idle: RefCell<Vec<(ConnectionHandle, Metrics)>>,
    /// A permit per connection that may be checked out.
    slots: Arc<Semaphore>,
    max_size: usize,
}

/// A connection handle checked out of a [`LocalPool`]: back to its idle
/// connections when dropped, unless it went bad.
pub(crate) struct Object {
    handle: Option<(ConnectionHandle, Metrics)>,
    pool: Rc<Inner>,
    _slot: OwnedSemaphorePermit,
}

impl LocalPool {
    pub(super) fn new(manager: Manager, max_size: usize) -> Self {
        Self {
            inner: Rc::new(Inner {
                manager,
                idle: RefCell::default(),
                slots: Arc::new(Semaphore::new(max_size)),
                max_size,
            }),
        }
    }

    pub(super) fn manager(&self) -> &Manager {
        &self.inner.manager
    }

    /// Checks out an idle connection that passes the manager's check, or
    /// opens one, once fewer than `max_size` are checked out.
    pub(super) async fn get(&self) -> crate::Result<Object> {
        let slot = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(toasty_core::Error::connection_pool)?;
        while let Some((mut handle, mut metrics)) = self.inner.pop_idle() {
            if self
                .inner
                .manager
                .check(&mut handle, &metrics)
                .await
                .is_ok()
            {
                metrics.recycle_count += 1;
                return Ok(self.object(handle, metrics, slot));
            }
        }
        let handle = self.inner.manager.connect().await?;
        Ok(self.object(handle, Metrics::default(), slot))
    }

    fn object(
        &self,
        handle: ConnectionHandle,
        metrics: Metrics,
        slot: OwnedSemaphorePermit,
    ) -> Object {
        Object {
            handle: Some((handle, metrics)),
            pool: self.inner.clone(),
            _slot: slot,
        }
    }

    pub(super) fn status(&self) -> deadpool::Status {
        let available = self.inner.idle.borrow().len();
        let checked_out = self.inner.max_size - self.inner.slots.available_permits();
        deadpool::Status {
            max_size: self.inner.max_size,
            size: available + checked_out,
            available,
            waiting: 0,
        }
    }
}

impl Inner {
    fn pop_idle(&self) -> Option<(ConnectionHandle, Metrics)> {
        self.idle.borrow_mut().pop()
    }
}

impl std::ops::Deref for Object {
    type Target = ConnectionHandle;

    fn deref(&self) -> &ConnectionHandle {
        &self.handle.as_ref().expect("a checked out connection").0
    }
}

impl Drop for Object {
    fn drop(&mut self) {
        if let Some((handle, metrics)) = self.handle.take()
            && !handle.is_closed()
        {
            self.pool.idle.borrow_mut().push((handle, metrics));
        }
    }
}

impl std::fmt::Debug for LocalPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalPool")
            .field("manager", &self.inner.manager)
            .field("status", &self.status())
            .finish()
    }
}
