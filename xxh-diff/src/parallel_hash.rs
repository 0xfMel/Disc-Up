use std::{
    fs::File,
    hash::Hasher,
    io::Read,
    iter,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use atomic_float::AtomicF32;
use flume::{Receiver, Selector, Sender, TryRecvError};
use gracile::{ErrHandle, TERMINATE};
use hashbrown::HashMap;
use sema_lot::Semaphore;
use twox_hash::XxHash64;

use crate::data_fmt::HashResult;

enum HashThreadMsg {
    Hash(HashResult),
    Halted(usize),
}

pub struct ParallelHash {
    pub path_rx: Receiver<PathBuf>,
    pub err_handle: ErrHandle,
    pub fd_sem: Arc<Semaphore>,
}

struct ThreadVars {
    parallel_hash: ParallelHash,
    path_rx_done: AtomicBool,
    thread_halt: AtomicU32,
}

pub fn hash_paths(
    parallel_hash: ParallelHash,
    send_hash: Sender<HashResult>,
    term_rx: Receiver<()>,
) {
    fn start_thread(
        thread_id: usize,
        thread_vars: &Arc<ThreadVars>,
        tx: &Sender<HashThreadMsg>,
        thread_speed: Arc<AtomicF32>,
    ) -> JoinHandle<()> {
        thread::spawn({
            let thread_vars = Arc::clone(thread_vars);
            let tx = tx.clone();
            move || {
                let ThreadVars {
                    parallel_hash,
                    path_rx_done,
                    thread_halt,
                } = &*thread_vars;

                let ParallelHash {
                    path_rx,
                    err_handle,
                    fd_sem,
                } = parallel_hash;

                let mut buf = [0u8; 64 * 1024];

                'thread_loop: loop {
                    if thread_id != 0 {
                        let mut to_halt = thread_halt.load(Ordering::Acquire);

                        loop {
                            if to_halt == 0 {
                                break;
                            }

                            match thread_halt.compare_exchange_weak(
                                to_halt,
                                to_halt - 1,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break 'thread_loop,
                                Err(new_to_halt) => to_halt = new_to_halt,
                            }
                        }
                    }

                    let file_path = match path_rx.try_recv() {
                        Ok(f) => f,
                        Err(TryRecvError::Disconnected) => {
                            path_rx_done.store(true, Ordering::Release);
                            break;
                        }
                        Err(TryRecvError::Empty) => {
                            if thread_id != 0 {
                                break;
                            }
                            let old_speed = thread_speed.swap(-2.0, Ordering::Release);
                            let path = match path_rx.recv() {
                                Ok(f) => f,
                                Err(_) => {
                                    path_rx_done.store(true, Ordering::Release);
                                    break;
                                }
                            };
                            thread_speed.store(old_speed, Ordering::Release);
                            path
                        }
                    };

                    let (hash, before, file_size) = {
                        let _guard = match fd_sem.try_access() {
                            Some(g) => g,
                            None => {
                                let old_speed = thread_speed.swap(-2.0, Ordering::Release);
                                let guard = fd_sem.access();
                                thread_speed.store(old_speed, Ordering::Release);
                                guard
                            }
                        };

                        let before = Instant::now();
                        let mut hash = XxHash64::default();
                        let mut file_size = 0;

                        let mut file = match File::open(&file_path) {
                            Ok(f) => f,
                            Err(e) => {
                                err_handle.term_err(format!(
                                    "Error opening file for hashing {}: {}",
                                    file_path.display(),
                                    e
                                ));
                                break;
                            }
                        };

                        loop {
                            match file.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => {
                                    hash.write(&buf[..n]);
                                    file_size += n;
                                }
                                Err(e) => {
                                    err_handle.term_err(format!(
                                        "Error reading from file for hashing {}: {}",
                                        file_path.display(),
                                        e
                                    ));
                                    break 'thread_loop;
                                }
                            }
                        }

                        (hash, before, file_size)
                    };

                    let hashed = hash.finish();
                    let speed =
                        file_size as f32 / Instant::now().duration_since(before).as_secs_f32();

                    thread_speed.store(speed, Ordering::Release);

                    if tx
                        .send(HashThreadMsg::Hash(HashResult(file_path, hashed)))
                        .is_err()
                    {
                        break;
                    }
                }

                let _ = tx.send(HashThreadMsg::Halted(thread_id));
            }
        })
    }

    let thread_vars = Arc::new(ThreadVars {
        parallel_hash,
        path_rx_done: AtomicBool::new(false),
        thread_halt: AtomicU32::new(0),
    });

    let ThreadVars {
        parallel_hash,
        path_rx_done,
        thread_halt,
        ..
    } = &*thread_vars;
    let ParallelHash { fd_sem, .. } = &parallel_hash;

    let (tx, rx) = flume::unbounded();

    let mut time = Instant::now();
    let mut thread_speeds = HashMap::new();

    let thread_speed = Arc::new(AtomicF32::new(-1.0));
    thread_speeds.insert(0, Arc::clone(&thread_speed));
    start_thread(0, &thread_vars, &tx, thread_speed);

    let mut next_thread_id = 1;
    let mut thread_count = 1;

    let mut last_num_per_sec: f64 = -1.0;
    let mut last_speed: f32 = -1.0;
    let mut thread_change: i64 = 0;
    'main_loop: loop {
        if TERMINATE.get() {
            break;
        }

        let msg = match rx.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                match thread_change {
                    0 => {}
                    tc if tc < 0 => {
                        thread_halt.fetch_add((-tc) as u32, Ordering::Release);
                        thread_change = 0;
                    }
                    mut tc => {
                        let to_halt = thread_halt.load(Ordering::Acquire);
                        if to_halt > 0 {
                            if thread_halt
                                .compare_exchange_weak(
                                    to_halt,
                                    to_halt.saturating_sub(tc as u32),
                                    Ordering::Release,
                                    Ordering::Relaxed,
                                )
                                .is_err()
                            {
                                continue;
                            }

                            tc -= to_halt as i64;
                        }

                        if tc > 0 {
                            for i in 0..tc as usize {
                                let thread_id = next_thread_id + i;
                                let thread_speed = Arc::new(AtomicF32::new(-1.0));
                                thread_speeds.insert(thread_id, thread_speed.clone());
                                start_thread(thread_id, &thread_vars, &tx, thread_speed);
                            }

                            next_thread_id += tc as usize;
                            thread_count += tc as u32;
                            thread_change -= tc;
                        } else {
                            thread_change = 0;
                        }
                    }
                }

                match Selector::new()
                    .recv(&rx, |msg| match msg {
                        Ok(msg) => Some(msg),
                        Err(_) => None,
                    })
                    .recv(&term_rx, |_| None)
                    .wait()
                {
                    Some(msg) => msg,
                    None => break,
                }
            }
        };

        let mut processed_num = 0;
        for msg in iter::once(msg).chain(rx.try_iter()) {
            match msg {
                HashThreadMsg::Halted(thread_id) => {
                    thread_speeds.remove(&thread_id);
                    thread_count -= 1;
                    if thread_count == 0 {
                        break 'main_loop;
                    }
                }
                HashThreadMsg::Hash(res) => {
                    if send_hash.send(res).is_err() {
                        break 'main_loop;
                    }
                    processed_num += 1;
                }
            }
        }

        if path_rx_done.load(Ordering::Acquire) {
            thread_speeds.clear();
            continue;
        }

        let num_per_sec = processed_num as f64 / Instant::now().duration_since(time).as_secs_f64();

        let mut no_speed = 0;
        let mut total_speed = 0.0;
        let mut num_thread_speeds = 0;
        for (_, speed) in thread_speeds.iter() {
            let speed = speed.load(Ordering::Acquire);
            if speed == -2.0 {
                continue;
            }

            num_thread_speeds += 1;
            if speed < 0.0 {
                no_speed += 1;
            } else {
                total_speed += speed;
            }
        }

        if no_speed > 0 {
            let perc_no_speed = no_speed as f32 / num_thread_speeds as f32;
            total_speed /= 1.0 - perc_no_speed;
        }

        if last_num_per_sec >= 0.0 && last_speed >= 0.0 && total_speed <= last_speed {
            if num_per_sec < last_num_per_sec {
                if thread_count > 1 {
                    thread_change -= 1;
                }
            } else if fd_sem.count() > 0 {
                thread_change += 1;
            }
        }

        last_num_per_sec = num_per_sec;
        last_speed = total_speed;
        time = Instant::now();
    }
}
