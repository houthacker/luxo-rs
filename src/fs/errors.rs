use crate::common::errors::error_chain_fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::ErrorKind;

/// Generic error type for I/O related failures.
#[derive(thiserror::Error)]
pub enum IOError {
    #[error("Access denied")]
    AccessDeniedError(#[source] std::io::Error),

    #[error("File not found")]
    FileNotFoundError(#[source] std::io::Error),

    #[error("Thread interrupted")]
    Interrupted(#[source] std::io::Error),

    #[error("Invalid input")]
    InvalidInput(#[source] std::io::Error),

    #[error("Generic I/O error")]
    Generic(#[source] std::io::Error),
}

impl From<std::io::Error> for IOError {
    fn from(value: std::io::Error) -> Self {
        match value.kind() {
            ErrorKind::PermissionDenied => Self::AccessDeniedError(value),
            ErrorKind::NotFound => Self::FileNotFoundError(value),
            ErrorKind::Interrupted => Self::Interrupted(value),
            ErrorKind::InvalidInput => Self::InvalidInput(value),
            _ => Self::Generic(value),
        }
    }
}

impl Debug for IOError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}
