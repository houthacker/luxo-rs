#![warn(missing_docs)]

//! `Luxor`, a database engine that sheds light on any query.

use tracing::{event, Level};
use tracing_subscriber;

/// This module contains all in-memory algorithm implementations used by `luxor`.
#[doc(hidden)]
pub(crate) mod algo;

/// This module contains code that is shared between different modules of `luxor`.
#[doc(hidden)]
pub mod common;

/// This module contains all I/O related functionality.
#[doc(hidden)]
pub(crate) mod fs;

#[tracing::instrument]
fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting the global subscriber here should be the first attempt to do so, and therefore be successful.");

    // At this time, only linux is supported. The reason for this is that
    // Open File Description Locks are non-POSIX. These lock types are used by `luxor` to ensure
    // more flexibility: these locks are associated with an open file description instead of with
    // a process.
    #[cfg(not(target_os = "linux"))]
    panic!("At this time, linux is the only OS supported by luxor.");

    event!(Level::INFO, "luxo_rs started.")
}
