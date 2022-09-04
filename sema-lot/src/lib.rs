use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

use parking_lot::{Condvar, Mutex};

pub struct Semaphore {
    count: AtomicIsize,
    locked: AtomicBool,
    lock: Mutex<()>,
    cvar: Condvar,
}

pub struct SemaphoreGuard<'a> {
    sem: &'a Semaphore,
}

impl<'a> Drop for SemaphoreGuard<'a> {
    fn drop(&mut self) {
        self.sem.release();
    }
}

impl Semaphore {
    pub fn new(initial: isize) -> Self {
        Self {
            count: AtomicIsize::new(initial),
            locked: AtomicBool::new(false),
            lock: Mutex::new(()),
            cvar: Condvar::new(),
        }
    }

    pub fn acquire(&self) {
        let mut lock = None;
        loop {
            let mut count = self.count.load(Ordering::SeqCst);
            loop {
                if count > 0 {
                    match self.count.compare_exchange_weak(
                        count,
                        count - 1,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return,
                        Err(c) => count = c,
                    }
                } else if let Some(ref mut lock) = lock {
                    self.cvar.wait(lock);
                    break;
                } else {
                    self.locked.store(true, Ordering::SeqCst);
                    lock = Some(self.lock.lock());
                }
            }
        }
    }

    pub fn try_acquire(&self) -> bool {
        let mut count = self.count.load(Ordering::SeqCst);
        loop {
            if count > 0 {
                match self.count.compare_exchange_weak(
                    count,
                    count - 1,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break true,
                    Err(c) => count = c,
                }
            } else {
                break false;
            }
        }
    }

    pub fn release(&self) {
        let mut lock = None;
        if self.locked.load(Ordering::SeqCst) {
            lock = Some(self.lock.lock());
            self.locked.store(false, Ordering::SeqCst);
        }
        self.count.fetch_add(1, Ordering::SeqCst);
        drop(lock);
        self.cvar.notify_one();
    }

    pub fn access(&self) -> SemaphoreGuard {
        self.acquire();
        SemaphoreGuard { sem: self }
    }

    pub fn try_access(&self) -> Option<SemaphoreGuard> {
        if self.try_acquire() {
            Some(SemaphoreGuard { sem: self })
        } else {
            None
        }
    }

    pub fn count(&self) -> isize {
        self.count.load(Ordering::SeqCst)
    }
}
