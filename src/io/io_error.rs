use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;

#[derive(thiserror::Error)]
pub enum IOError {
    #[error("Cannot read file metadata")]
    AccessDeniedError(#[source] std::io::Error),

    #[error("File not found")]
    FileNotFoundError(#[source] std::io::Error),

    #[error("Generic I/O error")]
    Generic(
        #[source]
        #[from]
        std::io::Error,
    ),
}

fn error_chain_fmt(error: &impl Error, fmt: &mut Formatter<'_>) -> std::fmt::Result {
    writeln!(fmt, "{}\n", error)?;
    let mut current = error.source();
    while let Some(cause) = current {
        writeln!(fmt, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }

    Ok(())
}

impl Debug for IOError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}
