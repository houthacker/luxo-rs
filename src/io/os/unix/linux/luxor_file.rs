use crate::io::errors::IOError;
use crate::io::luxor_file::{
    FileKeyResolver, FileRwLock, FileRwLockGuard, FileSerial, LockableFile, LuxorFile,
};
use nix::errno::Errno;
use nix::fcntl::{fcntl, FcntlArg};
use nix::libc::{c_short, flock, F_RDLCK, F_UNLCK, F_WRLCK, SEEK_SET};
use nix::unistd::Whence;
use std::fs;
use std::os::fd::{AsRawFd, RawFd};
use std::os::linux::fs::MetadataExt;
use std::path::Path;
use tracing::{event, span, Level};

impl FileKeyResolver for FileSerial {
    fn resolve_file_key<P: AsRef<Path>>(path: P) -> Result<u64, IOError> {
        let meta = fs::metadata(path.as_ref())?;
        Ok(meta.st_ino())
    }
}

/// The [LockableFile] implements file-based locks for [LuxorFile] instances.
/// This lock type is linux-specific.
impl LockableFile for LuxorFile {
    type Lock = OpenFileDescriptorLock;

    /// Returns an Open File Descriptor lock that allows for shared and exclusive access to
    /// data within this file.
    fn file_lock(&self, offset: i64, length: i64) -> OpenFileDescriptorLock {
        let fd = self.file.as_raw_fd();

        OpenFileDescriptorLock {
            fd,
            whence: Whence::SeekSet,
            start: offset,
            length,
        }
    }
}

/// The [OpenFileDescriptorLock] is a Linux-specific advisory lock that are associated with the
/// open file description on which they are acquired. Locks placed the same file descriptor are
/// always compatible (i.e. do not conflict). When overlapping locks are requested through different
/// file descriptors, they are not compatible (i.e. do conflict). This enables users to synchronize
/// access to a file by having different threads use their own file descriptor to acquire such locks.
///
/// *Note*: In contrast with traditional lock records, the Linux kernel performs no deadlock
/// detection on Open File Descriptor locks.
///
/// See also: [Open File Description Locks](https://www.gnu.org/software/libc/manual/html_node/Open-File-Description-Locks.html)
#[doc(hidden)]
#[non_exhaustive]
pub struct OpenFileDescriptorLock {
    fd: RawFd,
    whence: Whence,
    start: i64,
    length: i64,
}

impl OpenFileDescriptorLock {
    /// Tries to obtain the requested lock type.
    ///
    /// ### Parameters
    /// * `descriptor` - A reference to a `flock` struct containing the description of the requested lock.
    /// * `blocking` - `true` if the request must block, `false` if non-blocking is required.
    ///
    /// *Note*: If this is a blocking request and the current thread is interrupted while waiting
    /// to acquire the lock, this method will retry until either the lock is obtained, or an
    /// error occurs.
    #[doc(hidden)]
    fn lock(&self, descriptor: &flock, blocking: bool) -> anyhow::Result<OFDLockGuard, IOError> {
        loop {
            match fcntl(
                self.fd,
                if blocking {
                    FcntlArg::F_OFD_SETLKW(descriptor)
                } else {
                    FcntlArg::F_OFD_SETLK(descriptor)
                },
            ) {
                Ok(_) => {
                    break Ok(OFDLockGuard {
                        fd: self.fd,
                        whence: self.whence,
                        start: self.start,
                        length: self.length,
                    })
                }
                Err(errno) => match errno {
                    // If we're interrupted while waiting to acquire a lock, retry. This only
                    // happens with `FcntlArg::F_OFD_SETLKW`.
                    Errno::EINTR => continue,

                    // For all other errors, users must decide themselves whether a retry
                    // is feasible.
                    _ => break Err(IOError::from(errno)),
                },
            }
        }
    }
}

impl FileRwLock for OpenFileDescriptorLock {
    type Guard = OFDLockGuard;

    /// Obtains a shared lock on the resource as described by this [OpenFileDescriptorLock], and blocks until
    /// either the lock is obtained or an error occurs.
    ///
    /// *Note*: If the current thread is interrupted while waiting to acquire the lock,
    /// this method will retry until either the lock is obtained, or an error occurs.
    fn read(&self) -> anyhow::Result<OFDLockGuard, IOError> {
        // Prepare the flock struct
        let flock = flock {
            l_type: F_RDLCK as c_short,
            l_whence: SEEK_SET as c_short,
            l_start: self.start,
            l_len: self.length,
            l_pid: 0, // required for OFD locks
        };

        self.lock(&flock, true)
    }

    /// Obtains an exclusive lock on the resource as described by this [OpenFileDescriptorLock], and blocks until
    /// either the lock is obtained or an error occurs.
    ///
    /// *Note*: If the current thread is interrupted while waiting to acquire the lock,
    /// this method will retry until either the lock is obtained, or an error occurs.
    fn write(&self) -> anyhow::Result<OFDLockGuard, IOError> {
        // Prepare the flock struct
        let flock = flock {
            l_type: F_WRLCK as c_short,
            l_whence: SEEK_SET as c_short,
            l_start: self.start,
            l_len: self.length,
            l_pid: 0, // required for OFD locks
        };

        self.lock(&flock, true)
    }
}

/// The [OFDLockGuard] is an Open File Descriptor RAII guard that will release the owning
/// thread's (read- or write) lock when dropped.
///
/// *Note*: All errors are suppressed when dropping an [OFDLockGuard], instead an error message is
/// logged when an error occurs.
#[non_exhaustive]
pub struct OFDLockGuard {
    fd: RawFd,
    whence: Whence,
    start: i64,
    length: i64,
}

impl FileRwLockGuard for OFDLockGuard {}

/// The purpose of an [OFDLockGuard] is solely to offer RAII-style synchronization management.
/// When an [OFDLockGuard] is dropped, the lock is released.
///
/// *Note*: If an error occurs while releasing the lock, the lock becomes lost.
/// A lost shared lock prevents all future exclusive locks from being obtained, whereas a
/// lost exclusive lock prevents all future locks (of any type) from being obtained.<br>
/// However, this error cannot propagate and therefore is swallowed. Instead, an error message
/// is emitted to the log.
impl Drop for OFDLockGuard {
    fn drop(&mut self) {
        let span = span!(Level::ERROR, "drop");
        let _guard = span.enter();

        let mut flock = flock {
            l_type: F_UNLCK as c_short,
            l_whence: self.whence as c_short,
            l_start: self.start,
            l_len: self.length,
            l_pid: 0,
        };

        match fcntl(self.fd, FcntlArg::F_OFD_SETLK(&mut flock)) {
            Ok(_) => (),
            Err(errno) => {
                event!(
                    Level::ERROR,
                    "Could not release lock on fd <{}>: errno {} ({}). \
                    This will lead to an unresponsive and ultimately shutdown application since \
                    future locks (possibly) cannot be obtained. The application will \
                    attempt some retries that will fail, and shutdown after.",
                    self.fd,
                    errno,
                    errno.desc()
                );
            }
        }
    }
}
