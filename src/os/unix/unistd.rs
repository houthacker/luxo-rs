use nix::unistd;
use nix::unistd::SysconfVar;
use std::sync::LazyLock;
use tracing::{event, span, Level};

const FALLBACK_MEM_PAGE_SIZE: usize = 4096;

static SYS_PAGE_SIZE: LazyLock<usize> = LazyLock::new(|| init_sys_page_size());

fn init_sys_page_size() -> usize {
    let span = span!(Level::TRACE, "sys_page_size");
    let _guard = span.enter();

    match unistd::sysconf(SysconfVar::PAGE_SIZE) {
        Ok(Some(value)) => value as usize,
        Ok(None) => {
            event!(
                Level::TRACE,
                "sysconf(_SC_PAGE_SIZE) variable not supported, using fallback page size."
            );
            FALLBACK_MEM_PAGE_SIZE
        }
        Err(errno) => {
            event!(
                Level::TRACE,
                "Error while reading sysconf(_SC_PAGE_SIZE): {}. Using fallback page size.",
                errno.desc()
            );
            FALLBACK_MEM_PAGE_SIZE
        }
    }
}

/// Retrieves the memory page size in bytes. If the page size cannot be read from the OS, a fallback value
/// of `4096` is returned.
pub fn sys_page_size() -> usize {
    *SYS_PAGE_SIZE
}
