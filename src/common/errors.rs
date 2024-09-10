use std::error::Error;
use std::fmt::{Debug, Formatter};

/// Common error types.
#[derive(thiserror::Error, Eq, PartialEq)]
pub enum LuxorError {
    #[error("Incorrect alignment: {value:#?} is not aligned to {alignment:#?}.")]
    AlignmentError { value: String, alignment: String },
}

/// Writes the error and its stack trace - if any - to the given [Formatter].
#[doc(hidden)]
pub(crate) fn error_chain_fmt(error: &impl Error, fmt: &mut Formatter<'_>) -> std::fmt::Result {
    writeln!(fmt, "{}\n", error)?;
    let mut current = error.source();
    while let Some(cause) = current {
        writeln!(fmt, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }

    Ok(())
}

impl Debug for LuxorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}
