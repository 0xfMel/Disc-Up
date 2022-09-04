use signal_hook::{consts::TERM_SIGNALS, iterator::Signals};
use std::io;

static mut SIGNALS: Option<Signals> = None;

/// # Safety
/// shut up
#[inline]
pub unsafe fn init_os_handler() -> Result<(), io::Error> {
    SIGNALS = Some(Signals::new(TERM_SIGNALS)?);
    Ok(())
}

/// # Safety
/// shut up
#[inline]
pub unsafe fn block_for_sig() -> Result<(), io::Error> {
    if let Some(ref mut signals) = SIGNALS {
        loop {
            if signals.wait().count() > 0 {
                break;
            }
        }
    }

    Ok(())
}
