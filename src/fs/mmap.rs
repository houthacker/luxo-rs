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

/// Memory mapping for [LuxorFile] instances. Used to implement the Write-Ahead Log (which
/// uses a memory-mapped index).
impl MemoryMappable for LuxorFile {
    /// Maps the given `region` of this file into shared, mutable memory.
    ///
    /// *Note*: The mapped region need not exist in the file prior to mapping, but it must be backed by actual file
    /// contents *before* the first time the returned segment is read from or written to.
    ///
    /// ### Parameters
    /// * `region` - The file region to map into memory.
    ///
    /// ### Safety
    /// Mapping file contents is inherently unsafe, since other processes can also write to the associated file, or even
    /// delete it. There are some manners of protecting against this, but these are not available on all platforms
    /// alike.
    ///
    /// If a mapping is accessed and the associated file region does not exist (anymore), that access will cause a
    /// panic.
    unsafe fn map_shared(&self, region: AlignedFileRegion) -> Result<MappedMemorySegment, IOError> {
        Ok(MappedMemorySegment {
            data: MmapOptions::new()
                .offset(region.offset)
                .len(region.length)
                .map_mut(&self.file)?,
        })
    }
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

impl MappedMemorySegment {
    /// Returns the length of this segment in bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Re-interprets the memory region `self[offset..(offset + mem::sizeof::<T>())]` as a [`&T`].
    ///
    /// ### Parameters
    /// * `offset` - The offset in bytes within this segment.
    ///
    /// ### Safety
    /// To ensure some safety, the following preconditions must hold:
    /// * `offset + mem::size_of::<T>()` must not overflow [usize::MAX].
    /// * `offset + mem::size_of::<T>()` must not exceed [Self::len].
    /// * [T] must be `#[repr(C, packed)]`: aligned to a byte and have [`C` Representation](https://doc.rust-lang.org/reference/type-layout.html#the-c-representation)
    /// If any of these preconditions do *not* hold, this method will panic.
    ///
    /// Operating on memory-mapped data can never be fully safe, since conflicting processes might alter the associated
    /// file region as well, which cannot be controlled by but a single process.
    ///
    /// ### Example
    /// ```
    /// use crate::fs::LuxorFile;
    /// use crate::fs::mmap::MappedMemorySegment;
    ///
    /// #[repr(C)]
    /// struct Child {
    ///     child_field: bool,
    /// }
    ///
    /// #[repr(C)]
    /// struct MyStruct {
    ///     child: Child,
    ///     other_field: u64,
    /// }
    ///
    /// /// *Note*: error checking removed for brevity.
    /// pub fn main() {
    ///     // Create an empty temporary file.
    ///     let path = tempfile::file::NamedTempFile::new().unwrap().into_temp_path();
    ///
    ///     let file = LuxorFile::open(
    ///         &path,
    ///         &OpenOptions::new().read(true).write(true).create(false),
    ///     )
    ///     .unwrap();
    ///
    ///     // Set file length so we can access some shared memory.
    ///     let page_size = unsafe { sys_page_size() } as u64;
    ///     file.set_len(page_size).unwrap();
    ///
    ///     // Get the segment to play with.
    ///     let mut segment =
    ///         unsafe { file.map_shared(AlignedFileRegion::new_aligned(0, page_size)) }.unwrap();
    ///
    ///     // Get a MyStruct instance from the segment
    ///     let my_struct = unsafe { segment.transmute_unchecked::<MyStruct>(0) };
    ///
    ///     // Use it like any other regular struct.
    ///     println!("my_struct.other_field = {}", my_struct.other_field);
    /// }
    /// ```
    pub unsafe fn transmute_unchecked<T: Sized>(&self, offset: usize) -> &T {
        let (_head, body, _tail) = self.data[offset..].align_to();

        &body[0]
    }

    /// Re-interprets the memory region `self[offset..(offset + mem::sizeof::<T>())]` as a [`&mut T`].
    ///
    /// ### Parameters
    /// * `offset` - The offset in bytes within this segment.
    ///
    /// ### Safety
    /// To ensure some safety, the following preconditions must hold:
    /// * `offset + sizeof(T)` must not overflow [usize::MAX].
    /// * `offset + sizeof(T)` must not exceed [Self::len].
    /// * [T] must be `#[repr(C, packed)]`: aligned to a byte and have [`C` Representation](https://doc.rust-lang.org/reference/type-layout.html#the-c-representation)
    /// If any of these preconditions do *not* hold, this method will panic.
    ///
    /// Operating on memory-mapped data can never be fully safe, since conflicting processes might alter the associated
    /// file region as well, which cannot be controlled by but a single process.
    ///
    /// ### Example
    /// ```
    /// use crate::fs::LuxorFile;
    /// use crate::fs::mmap::MappedMemorySegment;
    ///
    /// #[repr(C)]
    /// struct Child {
    ///     child_field: bool,
    /// }
    ///
    /// #[repr(C)]
    /// struct MyStruct {
    ///     child: Child,
    ///     other_field: u64,
    /// }
    ///
    /// /// *Note*: error checking removed for brevity.
    /// pub fn main() {
    ///     // Create an empty temporary file.
    ///     let path = tempfile::file::NamedTempFile::new().unwrap().into_temp_path();
    ///
    ///     let file = LuxorFile::open(
    ///         &path,
    ///         &OpenOptions::new().read(true).write(true).create(false),
    ///     )
    ///     .unwrap();
    ///
    ///     // Set file length so we can access some shared memory.
    ///     let page_size = unsafe { sys_page_size() } as u64;
    ///     file.set_len(page_size).unwrap();
    ///
    ///     // Get the segment to play with.
    ///     let mut segment =
    ///         unsafe { file.map_shared(AlignedFileRegion::new_aligned(0, page_size)) }.unwrap();
    ///
    ///     // Get a MyStruct instance from the segment
    ///     let my_struct = unsafe { segment.transmute_as_mut_unchecked::<MyStruct>(0) };
    ///
    ///     // Use it like any other regular struct.
    ///     my_struct.other_field = 1337;
    /// }
    /// ```
    pub unsafe fn transmute_as_mut_unchecked<T: Sized>(&mut self, offset: usize) -> &mut T {
        let (_head, body, _tail) = self.data[offset..].align_to_mut();

        &mut body[0]
    }
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

    /// Proves that a non-existing file region can be mapped and that the region can be accessed once the file has
    /// backing contents.
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
        let data = segment.as_mut();
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

    /// Proves that a region of mapped memory can be transmuted to an immutable struct instance.
    #[test]
    fn transmute_unchecked_from_segment() {
        #[derive(Eq, PartialEq, Debug)]
        #[repr(C)]
        struct MyStruct {
            field_one: u64,
            field_two: bool,
        }

        // The path to the file we're going to test with.
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let file = LuxorFile::open(
            &path,
            &OpenOptions::new().read(true).write(true).create(false),
        )
        .unwrap();

        // Set file length so we can access some shared memory.
        file.set_len(unsafe { sys_page_size() } as u64).unwrap();

        // Get the segment to play with.
        let segment = unsafe { file.map_shared(AlignedFileRegion::new_aligned(0, 4096)) }.unwrap();

        // Transmute a Parent instance from the segment
        let my_struct = unsafe { segment.transmute_unchecked::<MyStruct>(0) };
        assert_eq!(
            my_struct,
            &MyStruct {
                field_one: 0,
                field_two: false,
            }
        );
    }

    /// Proves that updates to a struct that is transmuted from a mapped memory segment are reflected in
    /// the underlying file.
    #[test]
    fn transmute_as_mut_cycle() {
        // Create two structs with a parent-child relation

        #[derive(Eq, PartialEq, Debug)]
        #[repr(C)]
        struct Child {
            salt: u32,
            checksum: u64,
        }

        #[derive(Eq, PartialEq, Debug)]
        #[repr(C)]
        struct Parent {
            child: Child,
            is_stale: bool,
        }

        impl Parent {
            fn is_stale(&self) -> bool {
                self.is_stale
            }
        }

        // The path to the file we're going to test with.
        let path = NamedTempFile::new().unwrap().into_temp_path();

        // In a designated scope (so they'll be dropped after), create a file and a mapped memory segment.
        // Transmute a region of the segment to a Parent instance and update some values.
        {
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

            // Transmute a Parent instance from the segment
            let parent = unsafe { segment.transmute_as_mut_unchecked::<Parent>(0) };
            assert_eq!(
                parent,
                &Parent {
                    child: Child {
                        salt: 0,
                        checksum: 0
                    },
                    is_stale: false
                }
            );

            // Update the parent, flush the segment.
            parent.is_stale = true;
            parent.child.salt = 42;
            parent.child.checksum = 1337;

            // Flush the data to disk so a new LuxorFile of the same path will read this data.
            segment.data.flush().unwrap();
        }

        // Now that the original file and memory segment have been dropped, create a new file and compare the Parent
        // we read from a memory segment associated with the file.
        let file = LuxorFile::open(
            &path,
            &OpenOptions::new().read(true).write(true).create(false),
        )
        .unwrap();

        let mut segment =
            unsafe { file.map_shared(AlignedFileRegion::new_aligned(0, 4096)) }.unwrap();

        let parent = unsafe { segment.transmute_as_mut_unchecked::<Parent>(0) };

        // This Parent must equal the one we wrote to the previous memory segment.
        assert_eq!(
            parent,
            &Parent {
                child: Child {
                    salt: 42,
                    checksum: 1337
                },
                is_stale: true
            }
        );
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 0 but the index is 0")]
    fn transmute_out_of_bounds_access() {
        #[derive(Eq, PartialEq, Debug)]
        #[repr(C)]
        struct Test {
            field: u64,
            another_field: bool,
        }

        // The path to the file we're going to test with.
        let path = NamedTempFile::new().unwrap().into_temp_path();

        // Create an empty file.
        let file = LuxorFile::open(
            &path,
            &OpenOptions::new().read(true).write(true).create(false),
        )
        .unwrap();

        // Get the segment to play with.
        let mut segment =
            unsafe { file.map_shared(AlignedFileRegion::new(0, 0).unwrap()) }.unwrap();

        // panic!
        let _ = unsafe { segment.transmute_as_mut_unchecked::<Test>(0) };
    }
}
