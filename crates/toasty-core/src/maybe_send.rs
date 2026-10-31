//! `Send`/`Sync` bounds that only apply on native targets.
//!
//! `wasm32` has a single thread, and the values of its runtimes (JS promises, a
//! Cloudflare Worker's sockets) are not `Send`. Toasty's driver traits, its
//! errors' sources and its value streams bound on [`MaybeSend`]/[`MaybeSync`]
//! instead of `Send`/`Sync`: the same bounds as before on native targets, none on
//! `wasm32`, so drivers on `!Send` clients can run there.
//!
//! The futures of the driver traits follow: implement them with
//! `#[async_trait]` on native targets and `#[async_trait(?Send)]` on `wasm32`:
//!
//! ```ignore
//! #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
//! #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
//! impl Driver for MyDriver {
//!     // ...
//! }
//! ```

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    /// `Send` on native targets, no bound on `wasm32`.
    pub trait MaybeSend: Send {}
    impl<T: Send + ?Sized> MaybeSend for T {}

    /// `Sync` on native targets, no bound on `wasm32`.
    pub trait MaybeSync: Sync {}
    impl<T: Sync + ?Sized> MaybeSync for T {}

    /// A boxed error, `Send + Sync` on native targets.
    pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
}

#[cfg(target_arch = "wasm32")]
mod imp {
    /// `Send` on native targets, no bound on `wasm32`.
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}

    /// `Sync` on native targets, no bound on `wasm32`.
    pub trait MaybeSync {}
    impl<T: ?Sized> MaybeSync for T {}

    /// A boxed error, `Send + Sync` on native targets.
    pub(crate) type BoxError = Box<dyn std::error::Error>;
}

pub(crate) use imp::BoxError;
pub use imp::{MaybeSend, MaybeSync};
