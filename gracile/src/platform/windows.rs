use signal_hook::{consts::TERM_SIGNALS, low_level};
use std::{
    io::{self, ErrorKind},
    ptr,
};
use winapi::{
    ctypes::c_long,
    shared::{minwindef::BOOL, ntdef::HANDLE},
    um::{
        synchapi::{ReleaseSemaphore, WaitForSingleObject},
        winbase::{CreateSemaphoreA, INFINITE, WAIT_FAILED, WAIT_OBJECT_0},
    },
};

const MAX_SEM_COUNT: c_long = 255;
static mut SEMAPHORE: HANDLE = 0 as HANDLE;

unsafe fn os_handler() -> BOOL {
    ReleaseSemaphore(SEMAPHORE, 1, ptr::null_mut())
}

/// # Safety
/// shut up
#[inline]
pub unsafe fn init_os_handler() -> Result<(), io::Error> {
    SEMAPHORE = CreateSemaphoreA(ptr::null_mut(), 0, MAX_SEM_COUNT, ptr::null());
    if SEMAPHORE.is_null() {
        return Err(io::Error::last_os_error());
    }

    for sig in TERM_SIGNALS {
        low_level::register(*sig, || {
            os_handler();
        })?;
    }

    Ok(())
}

/// # Safety
/// shut up
#[inline]
pub unsafe fn block_for_sig() -> Result<(), io::Error> {
    match WaitForSingleObject(SEMAPHORE, INFINITE) {
        WAIT_OBJECT_0 => Ok(()),
        WAIT_FAILED => Err(io::Error::last_os_error()),
        ret => Err(io::Error::new(
            ErrorKind::Other,
            format!(
                "WaitForSingleObject(), unexpected return value \"{:x}\"",
                ret
            ),
        )),
    }
}
