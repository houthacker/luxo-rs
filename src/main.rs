#![warn(missing_docs)]

//! `Luxor`, a database engine that sheds light on any query.

use tracing::{event, Level};
use tracing_subscriber;

#[doc(hidden)]
/// This module contains all in-memory algorithm implementations used by `luxor`.
pub(crate) mod algo;

/// This module contains all I/O related functionality.
pub(crate) mod io;

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
    if !cfg!(target_os = "linux") {
        panic!("At this time, linux is the only OS supported by luxor.")
    }

    event!(Level::INFO, "luxo_rs started.")
}
