use std::{
    fmt::{self, Display, Formatter},
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use flume::{Receiver, Sender};
use signal_hook::{consts::TERM_SIGNALS, flag};

mod platform;

pub enum InitError {
    IO(io::Error),
    Duplicate,
}

impl Display for InitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            InitError::IO(e) => e.fmt(f),
            InitError::Duplicate => write!(f, "Duplicate signal handlers added"),
        }
    }
}

pub struct Terminate {
    inner: AtomicBool,
}

impl Terminate {
    const fn new() -> Self {
        Self {
            inner: AtomicBool::new(false),
        }
    }

    pub fn set(&self) {
        self.inner.store(true, Ordering::SeqCst);
    }

    pub fn get(&self) -> bool {
        self.inner.load(Ordering::SeqCst)
    }
}

pub static TERMINATE: Terminate = Terminate::new();

#[derive(Clone)]
pub struct ErrHandle {
    tx: Sender<String>,
}

impl ErrHandle {
    fn new(tx: Sender<String>) -> Self {
        Self { tx }
    }

    pub fn term_err(&self, err: String) {
        let _ = self.tx.send(err);
    }
}

pub struct TermHandle {
    rx: Option<Receiver<()>>,
    pub err_rx: Receiver<String>,
    pub err_handle: ErrHandle,
}

impl TermHandle {
    fn new(rx: Receiver<()>) -> Self {
        Self::new_inner(Some(rx))
    }

    fn new_inner(rx: Option<Receiver<()>>) -> Self {
        let (tx, err_rx) = flume::bounded(0);
        Self {
            rx,
            err_rx,
            err_handle: ErrHandle::new(tx),
        }
    }

    pub fn rx(&mut self) -> &Receiver<()> {
        self.rx.get_or_insert_with(|| flume::bounded(0).1)
    }
}

impl Default for TermHandle {
    fn default() -> Self {
        Self::new_inner(None)
    }
}

impl Drop for TermHandle {
    fn drop(&mut self) {
        if TERMINATE.get() {
            drop(self.rx.take());
        }
    }
}

/// # Safety
/// Should only be called once
pub unsafe fn init_handle() -> Result<TermHandle, InitError> {
    let (tx, rx) = flume::bounded(0);

    for sig in TERM_SIGNALS {
        let stop_now = Arc::new(AtomicBool::new(false));
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&stop_now))
            .map_err(InitError::IO)?;
        flag::register(*sig, stop_now).map_err(InitError::IO)?;
    }

    platform::init_os_handler().map_err(InitError::IO)?;

    thread::spawn(move || match platform::block_for_sig() {
        Ok(_) => {
            TERMINATE.set();
            while tx.send(()).is_ok() {}
        }
        Err(e) => eprintln!("Error blocking for signal: {}", e),
    });

    Ok(TermHandle::new(rx))
}
