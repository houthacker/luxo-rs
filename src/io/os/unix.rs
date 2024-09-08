/// Unix-specific I/O related trait implementations.
mod fs;

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {

        /// Linux-specific I/O-related trait implementations.
        pub mod linux;
    }
}
