use crate::fs::errors::IOError;
use nix::errno::Errno;

impl From<Errno> for IOError {
    fn from(value: Errno) -> Self {
        IOError::from(std::io::Error::from(value))
    }
}
