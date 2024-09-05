use std::cmp::Ordering;
use std::fs::File;
use std::os::linux::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};
use std::{fs, io};
use tracing::{event, Level};

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
/// its own file descriptor, ensuring that the underlying file locking mechanisms will work
/// as intended.
///
/// <h4>Locking</h4>
/// A [LuxorFile] supports three disjoint locking features. An explanation of these features can be
/// found at their respective methods:
/// * In-process read/write mutual exclusive locking using [LuxorFile::serial::rw].
/// * In-process exclusive locking using [LuxorFile::serial::mutex].
/// * Intra-process file locks using TODO.
pub struct LuxorFile {
    /// The full, absolute path to this file.
    path: PathBuf,

    /// The unique file serial used for inter-thread synchronization.
    serial: Arc<FileSerial>,

    /// The file itself.
    data: File,
}

/// Empty struct to enable disjoint locks on a [FileSerial].
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
    pub key: u64,

    /// The lock object for read/write locks.
    pub rw: RwLock<Locker>,

    /// The lock object for exclusive locks.
    pub mutex: Mutex<Locker>,
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
    /// * `path` The path to identify.
    ///
    /// # Errors
    /// This method will return an error in at least to following cases:
    /// * The user lacks permissions to retrieve the metadata of `path`.
    /// * The `path` does not exist.
    pub fn find<P: AsRef<Path>>(path: P) -> Result<Arc<FileSerial>, io::Error> {
        let meta = fs::metadata(path.as_ref())?;
        let key = meta.st_ino();

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
        let result = FileSerial::find(Path::new("/tmp/luxor_do_not_exist")).map_err(|e| e.kind());
        assert_eq!(
            result.err(),
            Some(io::ErrorKind::NotFound),
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
}
