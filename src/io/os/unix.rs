/// Unix-specific I/O related trait implementations.
mod luxor_file;

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {

        /// Linux-specific I/O-related trait implementations.
        pub mod linux;
    }
}
