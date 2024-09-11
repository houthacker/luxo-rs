use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(target_family = "unix")] {
        pub mod unix;

        // Export the sys_page_size function so that is can be
        // called like os::sys_page_size(). That way, users know that it
        // is an os-specific implementation but do not have to care which one.
        pub use unix::unistd::sys_page_size;
    }
}

#[cfg(target_os = "linux")]
pub mod linux;
