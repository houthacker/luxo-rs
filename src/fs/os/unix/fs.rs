use crate::fs::errors::IOError;
use crate::fs::{LuxorFile, ReadableFile, WritableFile};
use std::os::unix::fs::FileExt;

impl ReadableFile for LuxorFile {
    /// Reads `buf.len()` bytes from this file into `buf`, starting at `offset` in this file.<br>
    /// Returns the number of bytes read.<br>
    /// Note that similar to [std::io::Read::read], it is not an error to return with a short read.
    ///
    /// ### Parameters
    /// * `buf` - The buffer to read into.
    /// * `offset` - The file offset in bytes to start reading at.
    ///
    /// ### Errors
    /// * If an error occurs while reading data, this method returns an error variant. If an error
    /// is returned, it is guaranteed that no bytes were read.
    /// * An [IOError::Interrupted] is non-fatal, and the read operation should be retried if
    /// there is nothing else to do.
    fn read(&self, buf: &mut [u8], offset: u64) -> anyhow::Result<usize, IOError> {
        Ok(self.file.read_at(buf, offset)?)
    }
}

impl WritableFile for LuxorFile {
    /// Writes `buf.len()` bytes to this file, starting at `offset` in this file.<br>
    /// Returns the number of bytes written. If this number is `0`, this indicates one
    /// of two scenarios:
    /// * The given `buf` was 0 bytes in length.
    /// * "End of file" has been reached.
    /// When writing beyond the end of the file, the file is appropriately extended and the
    /// intermediate bytes are initialized with the value 0.<br>
    /// Note that similar to [std::io::Write::write], it is not an error to return with a short write.
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
    fn write(&self, buf: &[u8], offset: u64) -> anyhow::Result<usize, IOError> {
        Ok(self.file.write_at(buf, offset)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::LuxorFile;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    #[test]
    fn luxor_file_read_write_cycle() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        let result = LuxorFile::open(
            &path,
            &OpenOptions::new().read(true).write(true).create(true),
        );

        // Write the data.
        assert!(matches!(result, Ok(_)));
        let luxor_file = result.unwrap();
        let data = [1u8, 3, 3, 7];
        assert!(matches!(luxor_file.write(&data, 0), Ok(4usize)));

        // Ensure the data has reached the disk
        assert!(matches!(luxor_file.sync(), Ok(_)));

        // And read it back.
        let mut retrieved = [0u8; 4];
        assert!(matches!(luxor_file.read(&mut retrieved, 0), Ok(4usize)));
        assert_eq!(
            data, retrieved,
            "The data read from the file must exactly equal the data written to it."
        );
    }
}
