#![warn(missing_docs)]

//! `luxo_rs`, a database engine that sheds light on any query.

use tracing::{event, Level};
use tracing_subscriber;

#[doc(hidden)]
/// This module contains all in-memory algorithm implementations used by `luxo_rs`.
pub(crate) mod algo;

#[tracing::instrument]
fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting the global subscriber here should be the first attempt to do so, and therefore be successful.");

    event!(Level::INFO, "luxo_rs started.")
}
