/// Unix-specific file system-related trait implementations.
mod fs;

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {

        /// Linux-specific file system-related trait implementations.
        pub mod linux;
    }
}
