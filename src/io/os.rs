use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(target_family = "unix")] {
        pub mod unix;
    }
}
