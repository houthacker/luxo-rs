use crate::common::errors::LuxorError;
use crate::fs::errors::IOError;
use crate::fs::LuxorFile;
use cfg_if::cfg_if;
use memmap2::{MmapMut, MmapOptions};
use nix::unistd;
use nix::unistd::SysconfVar;
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use tracing::{event, span, Level};

const FALLBACK_MEM_PAGE_SIZE: usize = 4096;

/// Retrieves the memory page size at compile time and panics if it cannot be retrieved.
#[doc(hidden)]
#[inline]
unsafe fn sys_page_size() -> usize {
    let span = span!(Level::TRACE, "sys_page_size");
    let _guard = span.enter();

    cfg_if! {
        if #[cfg(target_family = "unix")] {
            match unistd::sysconf(SysconfVar::PAGE_SIZE) {
                Ok(Some(value)) => value as usize,
                Ok(None) => {
                    event!(Level::TRACE, "sysconf(_SC_PAGE_SIZE) variable not supported, using fallback page size.");
                    FALLBACK_MEM_PAGE_SIZE
                },
                Err(errno) => {
                    event!(Level::TRACE, "Error while reading sysconf(_SC_PAGE_SIZE): {}. Using fallback page size.", errno.desc());
                    FALLBACK_MEM_PAGE_SIZE
                }
            }
        } else {
            unimplemented!("Retrieving the page size is not yet supported on this platform.")
        }
    }
}

/// Trait to enable support for file-backed memory mapping.
pub trait MemoryMappable {
    /// Maps the given region of the associated file into memory. The returned [MappedMemorySegment] is accessible
    /// by other threads and processes.
    ///
    /// To map a file region into memory, at least the following preconditions must be met:
    /// * The file must have been opened for writing.
    /// * The region must exist in the file prior to accessing the mapping.
    ///
    /// *Note*:
    /// * Both [AlignedFileRegion::offset] and [AlignedFileRegion::length] will at runtime get aligned to the OS page size
    /// by this method.
    ///
    /// ### Parameters
    /// * `region` - The file region to map into memory.
    ///
    /// ### Errors
    /// Memory mapping can fail due to various reasons, in which case an [IOError] is returned.
    ///
    /// ### Safety
    /// This method is `unsafe` because while creating or using the mapped memory segment, other processes might
    /// access and alter the associated file.<br>
    /// Additionally, if mapped memory is accessed outside the file bounds, the application
    /// **will be killed** by the Operating System because this behaviour is analogous to accessing undefined memory.<br>
    ///     * On Unix systems, this means the kernel will send a signal 7 (`SIGBUS: access to undefined memory`) to the
    ///       calling process, which will kill the application.
    ///     * On Windows systems, a General Protection Fault will be triggered, which is something an application
    ///       generally does not recover from.
    unsafe fn map_shared(&self, region: AlignedFileRegion) -> Result<MappedMemorySegment, IOError>;
}

/// Helper struct to create memory-mapped file regions. The `offset` and `length` values are always aligned
/// with the system page size.
#[derive(Debug)]
pub struct AlignedFileRegion {
    /// The absolute offset in bytes at which the region begins.
    pub offset: u64,

    /// The length in bytes of the region.
    pub length: usize,
}

impl AlignedFileRegion {
    /// Creates a new [AlignedFileRegion] using the given `offset` and `length`.
    ///
    /// ### Parameters
    /// * `offset` - The file offset, in bytes, where the region begins.
    /// * `length` - The length of the region, in bytes.
    ///
    /// ### Errors
    /// * Returns a [LuxorError::AlignmentError] if `offset` is not aligned to the OS page size.
    /// * Returns a [LuxorError::AlignmentError] if `length` is not aligned to the OS page size.
    pub fn new(offset: u64, length: usize) -> Result<AlignedFileRegion, LuxorError> {
        let page_size = unsafe { sys_page_size() };
        if offset % page_size as u64 != 0 {
            Err(LuxorError::AlignmentError {
                value: offset.to_string(),
                alignment: page_size.to_string(),
            })
        } else if length % page_size != 0 {
            Err(LuxorError::AlignmentError {
                value: length.to_string(),
                alignment: page_size.to_string(),
            })
        } else {
            Ok(AlignedFileRegion { offset, length })
        }
    }

    /// Creates a new [AlignedFileRegion] using the given `min_offset` and `min_length`. The given values are rounded
    /// up to the next multiple of the OS page size, if not already so.
    ///
    /// ### Parameters
    /// * `min_offset` - The minimum required byte offset. The aligned offset is guaranteed to be either this value
    /// or the next multiple of the system page size.
    /// * `min_length` - The minimum required region length. The aligned length is guaranteed to be either this value
    /// or the next multiple of the system page size.
    ///
    /// ### Examples
    /// ```
    /// /// How to retrieve the current system page size on Unix. For Windows systems, use for example
    /// /// the `page_size` crate.
    /// fn main() {
    ///     let page_size = unsafe { nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE).unwrap() };
    /// }
    /// ```
    pub fn new_aligned(min_offset: u64, min_length: usize) -> AlignedFileRegion {
        let page_size = unsafe { sys_page_size() };
        let aligned_offset = min_offset.next_multiple_of(page_size as u64);
        let aligned_length = min_length.next_multiple_of(page_size);

        AlignedFileRegion {
            offset: aligned_offset,
            length: aligned_length,
        }
    }
}

/// A [MappedMemorySegment] is an opaque struct that provides access to a contiguous region of mutable memory obtained
/// through [MemoryMappable::map_shared].
///
/// A [MappedMemorySegment] implements [`AsMut<[u8]>`] and [Deref] to enable reasoning over it like a regular,
/// mutable slice of bytes.
#[non_exhaustive]
pub struct MappedMemorySegment {
    data: MmapMut,
}

impl AsMut<[u8]> for MappedMemorySegment {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.data.as_mut()
    }
}

impl Deref for MappedMemorySegment {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data.deref()
    }
}

impl DerefMut for MappedMemorySegment {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.deref_mut()
    }
}

impl Debug for MappedMemorySegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedMemorySegment")
            .field("ptr", &self.data.as_ptr())
            .field("len", &self.data.len())
            .finish()
    }
}

/// Implement memory mapping for [LuxorFile] instances, since these are used to implement the Write-Ahead Log (which
/// uses a memory-mapped index).
impl MemoryMappable for LuxorFile {
    unsafe fn map_shared(&self, region: AlignedFileRegion) -> Result<MappedMemorySegment, IOError> {
        if self.len()? < region.offset + (region.length - 1) as u64 {}
        Ok(MappedMemorySegment {
            data: MmapOptions::new()
                .offset(region.offset)
                .len(region.length)
                .map_mut(&self.file)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ReadableFile;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    /// Proves that an [AlignedFileRegion] cannot be created if the offset is not aligned.
    #[test]
    fn create_unaligned_offset_file_region() {
        assert_eq!(
            AlignedFileRegion::new(1, 4096).unwrap_err(),
            LuxorError::AlignmentError {
                value: 1.to_string(),
                alignment: 4096.to_string(),
            },
            "Attempting to create an AlignedFileRegion with unaligned offset must fail.",
        );
    }

    /// Proves that an [AlignedFileRegion] cannot be created if the length is not aligned.
    #[test]
    fn create_unaligned_length_file_region() {
        assert_eq!(
            AlignedFileRegion::new(0, 4095).unwrap_err(),
            LuxorError::AlignmentError {
                value: 4095.to_string(),
                alignment: 4096.to_string(),
            },
            "Attempting to create an AlignedFileRegion with unaligned length must fail.",
        );
    }

    #[test]
    fn mmap_extend_file_after_creation() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let file = LuxorFile::open(
            &path,
            &OpenOptions::new().read(true).write(true).create(false),
        )
        .unwrap();

        // First, get the mapping.
        let len = 4096usize;
        let mut segment =
            unsafe { file.map_shared(AlignedFileRegion::new_aligned(0, len)) }.unwrap();

        // Then truncate the file to the correct length.
        file.set_len(len as u64).unwrap();

        // All extended bytes should be set to 0.
        assert_eq!(
            segment.as_mut()[len - 1],
            0u8,
            "Using a memory mapped file after extending it to the required size should succeed."
        );
    }

    /// Proves that a [MappedMemorySegment] is usable after the associated file (handle) has been closed.
    #[test]
    fn use_mapping_after_file_is_dropped() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let file = LuxorFile::open(
            &path,
            &OpenOptions::new().read(true).write(true).create(false),
        )
        .unwrap();

        // Set file length so we can access some shared memory.
        file.set_len(unsafe { sys_page_size() } as u64).unwrap();

        // Get the segment to play with.
        let mut segment =
            unsafe { file.map_shared(AlignedFileRegion::new_aligned(0, 4096)) }.unwrap();

        // Ensure the file is dropped.
        drop(file);

        // Mutate the data, and flush the segment.
        let mut data = segment.as_mut();
        data.fill(1u8);

        // Use the `data` private field to flush, since we don't want to expose this method
        // to users (mmap is used to implement the Write-Ahead Log, which must not flush to disk).
        segment.data.flush().unwrap();

        // Re-open the file and verify its contents.
        let verification =
            LuxorFile::open(&path, &OpenOptions::new().read(true).create(false)).unwrap();
        let mut buf = [0u8; 4096];
        verification.read(&mut buf, 0).unwrap();

        // The buffer must contain the ones we wrote to it earlier.
        assert_eq!(&buf, &[1u8; 4096]);
    }
}
