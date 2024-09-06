use crate::io::errors::IOError;
use anyhow::Result;
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};
use std::{fs, path};
use tracing::{event, span, Level};

/// A [LuxorFile] is used to operate on B+Tree- and WAL files.
///
/// <h3>Concurrency</h3>
///
/// <h4>Synchronization</h4>
///
/// Instances of [LuxorFile] delegate their inter-thread synchronization to a unique [FileSerial].
/// For example, on Unix systems, this serial is the `inode` number of the underlying file. This
/// ensures that even if files are opened from different paths, synchronization will work
/// throughout the running process because all such files share the same file serial.<br>
/// Although the [LuxorFile] is designed for concurrent accesses, it is *strongly* recommended that
/// every platform thread uses its own [LuxorFile] instance. This will provide each thread with
/// its own file descriptor, ensuring that the file-based locking mechanisms such as [LockableFile]
/// will work as intended.
///
/// <h4>Locking</h4>
/// A [LuxorFile] supports three disjoint locking features. An explanation of these features can be
/// found at their respective methods:
/// * In-process read/write mutual exclusive locking using [LuxorFile::memory_rw_lock].
/// * In-process exclusive locking using [LuxorFile::memory_exclusive_lock].
/// * Intra-process file locks using [LockableFile::file_lock].
pub struct LuxorFile {
    /// The full, absolute path to this file.
    path: PathBuf,

    /// The unique file serial used for inter-thread synchronization.
    serial: Arc<FileSerial>,

    /// The file itself.
    pub(crate) file: File,
}

impl LuxorFile {
    /// Using `options`, opens a new [LuxorFile] that will reside at `path`.
    ///
    /// ### Parameters
    /// * `path` - The path to the file, which may be relative.
    /// * `options` - The options to use when opening the file.
    ///
    /// ### Return
    /// On success, returns an owned [LuxorFile].<br>
    /// On failure, returns an [IOError].
    pub fn open<P: AsRef<Path>>(path: P, options: &OpenOptions) -> Result<Self> {
        let span = span!(Level::TRACE, "open");
        let _guard = span.enter();

        let abs_path = path::absolute(path.as_ref())?;

        // Open the file first, since this might also mandate the manner of creating it.
        let data = options.open::<&Path>(abs_path.as_ref())?;

        // Then, if that succeeds, retrieve the file serial. This will (desirably) fail if the user
        // forgot  to create the file, for example.
        let serial = FileSerial::find::<&Path>(abs_path.as_ref())?;
        Ok(Self {
            path: abs_path,
            serial,
            file: data,
        })
    }

    /// Returns the absolute path of this file.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Returns the size of this file, in bytes.
    ///
    /// *Note*: If the file is heavily altered, the returned value may be outdated at the time the
    /// user retrieves it.
    pub fn size(&self) -> Result<u64, IOError> {
        Ok(fs::metadata::<&Path>(self.path.as_ref())?.len())
    }

    /// Attempts to flush all OS-caches of this file to disk.
    /// This operation blocks until all caches have been written to the physical device.
    pub fn sync(&self) -> Result<(), IOError> {
        Ok(self.file.sync_data()?)
    }

    /// Returns a read/write mutex that allows for shared and exclusive access to data within this
    /// file. Use this lock when both readers and writers are contending for access to the protected
    /// resource.<br>
    ///
    /// *Note*: This lock is disjoint (i.e. does not conflict with) locks obtained through
    /// [LuxorFile::memory_exclusive_lock] or [LuxorFile::file_lock].
    pub fn memory_rw_lock(&self) -> &RwLock<Locker> {
        &self.serial.rw
    }

    /// Returns a mutex that allows for exclusive access to data within this file.
    /// Use this lock when only writers are contending for the protected resource.<br>
    ///
    /// *Note*: This lock is disjoint (i.e. does not conflict with) locks obtained through
    /// [LuxorFile::memory_rw_lock] or [LuxorFile::file_lock].
    pub fn memory_exclusive_lock(&self) -> &Mutex<Locker> {
        &self.serial.mutex
    }
}

/// The [ReadableFile] allows for various (OS-) independent implementations to read data from
/// Random Access Files.
pub trait ReadableFile {
    /// Reads `buf.len()` bytes from this file into `buf`, starting at `offset` in this file.<br>
    /// Returns the number of bytes read.<br>
    /// Note that similar to [File::read], it is not an error to return with a short read.
    ///
    /// ### Parameters
    /// * `buf` - The buffer to read into.
    /// * `offset` - The file offset in bytes to start reading at.
    ///
    /// ### Errors
    /// * If an error occurs while reading data, this method returns an error variant. If an error
    /// is returned, users must guarantee that no bytes are read.
    /// * An [IOError::Interrupted] is non-fatal, and the read operation should be retried if
    /// there is nothing else to do.
    #[allow(rustdoc::broken_intra_doc_links)] // `File::read` _does_ exist, but it is a trait implementation.
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize, IOError>;
}

/// The [WritableFile] allows for various (OS-) independent implementations to write data to
/// Random Access Files.
pub trait WritableFile {
    /// Writes `buf.len()` bytes to this file, starting at `offset` in this file.<br>
    /// Returns the number of bytes written. If this number is `0`, this indicates one
    /// of two scenarios:
    /// * The given `buf` was 0 bytes in length.
    /// * "End of file" has been reached.
    /// When writing beyond the end of the file, the file is appropriately extended and the
    /// intermediate bytes are initialized with the value 0.<br>
    /// Note that similar to [File::write], it is not an error to return with a short write.
    ///
    /// ### Parameters
    /// * `buf` - The buffer to write to the file.
    /// * `offset` - The file offset in bytes to start writing at.
    ///
    /// ### Errors
    /// * An [IOError] may be generated indicating that the operation could not be completed.
    /// In that case, no bytes from `buf` were written to the file.<br>
    /// * An [IOError::Interrupted] is non-fatal, and the write operation should be retried if
    /// there is nothing else to do.
    #[allow(rustdoc::broken_intra_doc_links)] // `File::write` _does_ exist, but it is a trait implementation.
    fn write(&self, buf: &[u8], offset: u64) -> Result<usize, IOError>;
}

/// A [LockableFile] is a file of which accesses can be synchronized between different threads
/// by locking file regions one wants to operate on.
pub trait LockableFile {
    type Lock: FileRwLock;

    /// Returns a [FileRwLock] with which to lock the file region specified by `offset` and `length`.
    fn file_lock(&self, offset: i64, length: i64) -> Self::Lock;
}

/// An [FileRwLock] is an advisory lock on the related file. Whether this lock is associated with a
/// process or for example with the file description is implementation-dependent.
pub trait FileRwLock {
    type Guard: FileRwLockGuard;

    /// Obtains a shared lock on the resource as described by this [FileRwLock], and blocks until
    /// either the lock is obtained or an error occurs.
    ///
    /// *Note*: If the current thread is interrupted while waiting to acquire the lock,
    /// this method will retry until either the lock is obtained, or an error occurs.
    fn read(&self) -> Result<Self::Guard, IOError>;

    /// Obtains an exclusive lock on the resource as described by this [FileRwLock], and blocks until
    /// either the lock is obtained or an error occurs.
    ///
    /// *Note*: If the current thread is interrupted while waiting to acquire the lock,
    /// this method will retry until either the lock is obtained, or an error occurs.
    fn write(&self) -> Result<Self::Guard, IOError>;
}

/// A [FileRwLockGuard] is an RAII guard that will release the owning thread's (read- or write)
/// lock when dropped.
///
/// *Note*: Implementations must ensure that all errors are suppressed when they are dropped
/// and instead log an error message when an error occurs.
#[allow(drop_bounds)] // This ensures that all FileRwLockGuard implementations also implement Drop.
pub trait FileRwLockGuard: Drop {}

pub trait FileKeyResolver {
    fn resolve_file_key<P: AsRef<Path>>(path: P) -> Result<u64, IOError>;
}

/// Tagging struct to enable disjoint locks on a [FileSerial].
#[doc(hidden)]
#[non_exhaustive]
#[derive(Debug)]
pub struct Locker;

/// A [FileSerial] is a unique identification of a given file or directory within its filesystem,
/// and should be used to synchronize access between threads to the underlying resource.
///
/// ### Locking
/// The [FileSerial] contains two disjoint locking mechanisms:
/// * A [RwLock] to coordinate exclusive access with shared access to the associated file.
/// * A [Mutex] to coordinate exclusive access to the associated file.
#[non_exhaustive]
#[derive(Debug)]
pub struct FileSerial {
    /// The unique identifying object of this [FileSerial].
    key: u64,

    /// The lock object for read/write locks.
    rw: RwLock<Locker>,

    /// The lock object for exclusive locks.
    mutex: Mutex<Locker>,
}

/// The list of all [FileSerial] instances in use by the current process.
/// The list is sorted by [FileSerial::key].
#[doc(hidden)]
static SERIALS: LazyLock<Arc<RwLock<Vec<Weak<FileSerial>>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Vec::with_capacity(32))));

/// Searches the current list of [FileSerial] instances for one having the given `key`.
#[doc(hidden)]
fn find_existing_serial(key: u64) -> Option<Arc<FileSerial>> {
    let list = SERIALS.read().unwrap();

    let search_result = list
        .as_slice()
        .binary_search_by(|elem| match elem.upgrade() {
            Some(serial) => serial.key.partial_cmp(&key).unwrap(),
            None => Ordering::Less,
        });

    match search_result {
        Ok(index) => Some(Weak::upgrade(&list[index])?),
        _ => None,
    }
}

impl FileSerial {
    /// Obtains a unique identification of the given path. Multiple paths leading to the same
    /// location will yield the same [FileSerial].
    ///
    /// # Parameters
    /// * `path` - The path to identify.
    ///
    /// # Errors
    /// This method will return an error in at least to following cases:
    /// * The user lacks permissions to retrieve the metadata of `path`.
    /// * The `path` does not exist.
    pub fn find<P: AsRef<Path>>(path: P) -> Result<Arc<FileSerial>, IOError> {
        let span = span!(Level::TRACE, "find");
        let _guard = span.enter();

        //let meta = fs::metadata(path.as_ref())?;
        //let key = meta.st_ino();
        let key = Self::resolve_file_key(path.as_ref())?;

        match find_existing_serial(key) {
            Some(e) => Ok(e),
            None => {
                event!(
                    Level::TRACE,
                    "FileSerial {} of path {} not yet referenced; creating new FileSerial.",
                    key,
                    path.as_ref().display()
                );

                // No FileSerial found, so create a new one and store it.
                let mut list = SERIALS.write().unwrap();

                let subject = Arc::new(FileSerial {
                    key,
                    rw: RwLock::new(Locker),
                    mutex: Mutex::new(Locker),
                });

                // Add the new FileSerial to the list and sort it afterward.
                list.push(Arc::downgrade(&subject));
                list.sort_by(|lhs, rhs| match (lhs.upgrade(), rhs.upgrade()) {
                    (None, None) => Ordering::Equal,
                    (None, _) => Ordering::Greater,
                    (_, None) => Ordering::Less,
                    (Some(l), Some(r)) => l.key.partial_cmp(&r.key).unwrap(),
                });

                Ok(subject)
            }
        }
    }
}

/// When the last reference to a [FileSerial] is removed, that serial must be removed from the
/// global list to prevent entries without a (valid) value.
impl Drop for FileSerial {
    #[tracing::instrument]
    fn drop(&mut self) {
        // Get a write lock on the list
        let mut list = SERIALS.write().unwrap();

        // Find `self` by checking the strong count. At this point, `self` is already removed
        // from the weak reference.
        match list.iter().rposition(|element| element.strong_count() == 0) {
            Some(index) => {
                list.remove(index);
            }
            _ => {
                event!(Level::TRACE, "Dropping a FileSerial that doesn't exist in the global list. This might indicate that a FileSerial was created outside of the luxor_file module.");
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::*;

    #[test]
    fn find_serial_of_non_existing_path() {
        let result = FileSerial::find(Path::new("/tmp/luxor_do_not_exist"));
        assert!(
            matches!(result.err(), Some(IOError::FileNotFoundError(_))),
            "Finding the FileSerial of a non-existing path should always fail."
        );
    }

    #[test]
    fn find_serial_of_existing_path() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let result = FileSerial::find::<&Path>(path.as_ref());
        assert_eq!(
            Arc::strong_count(result.as_ref().unwrap()),
            1,
            "A FileSerial should have exactly 1 strong reference if only a single thread uses it."
        );

        // Use `result` again to prevent early dropping.
        assert!(matches!(result, Ok(_)));
    }

    #[test]
    fn find_serial_twice() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let first = FileSerial::find::<&Path>(path.as_ref());
        let second = FileSerial::find::<&Path>(path.as_ref());

        assert_eq!(
            Arc::strong_count(first.as_ref().unwrap()),
            2,
            "A FileSerial that is retrieved <x> times must have exactly <x> strong references."
        );

        // Use `first` and `second` again to prevent early dropping.
        assert!(matches!(first, Ok(_)));
        assert!(matches!(second, Ok(_)));
    }

    #[test]
    fn find_serial_from_multiple() {
        let path_one = NamedTempFile::new().unwrap().into_temp_path();
        let path_two = NamedTempFile::new().unwrap().into_temp_path();
        let path_three = NamedTempFile::new().unwrap().into_temp_path();

        // Ensure all three paths are referenced in the global FileSerial list
        let first = FileSerial::find::<&Path>(path_one.as_ref());
        let _ = FileSerial::find::<&Path>(path_two.as_ref());
        let _ = FileSerial::find::<&Path>(path_three.as_ref());

        // Find the first one, ensuring correct ordering.
        let serial = FileSerial::find::<&Path>(path_one.as_ref());
        assert!(matches!(serial, Ok(_)));
        assert_eq!(serial.unwrap().key, first.unwrap().key);
    }

    #[test]
    fn open_luxor_file() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(false);
        let result = LuxorFile::open(&path, &options);

        assert!(matches!(result, Ok(_)));
        let luxor_file = result.unwrap();
        let file_path = luxor_file.path();
        assert_eq!(file_path, <TempPath as AsRef<Path>>::as_ref(&path));
    }
}
