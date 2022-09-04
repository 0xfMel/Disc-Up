use std::{
    cell::Cell,
    fs,
    io::{self, ErrorKind, Write},
    iter,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use clap::Parser;
use crossbeam_utils::sync::Unparker;
use data_fmt::{DataErr, HashResult, ReadXxhDiffDataInner, XxhDiffData};
use flume::{RecvError, Selector};
use gracile::{TermHandle, TERMINATE};
use hashbrown::HashMap;
use parallel_hash::ParallelHash;
use parking_lot::Mutex;
use raw_path_bytes::RawPathBytes;
use sema_lot::Semaphore;

mod data_fmt;
mod parallel_hash;
mod paths;
mod raw_path_bytes;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(long, short)]
    data: Option<String>,

    #[clap(long, short)]
    output_data: Option<String>,

    #[clap(long, short = 'f', default_value = "500")]
    max_files_open: u32,

    #[clap(multiple = true)]
    rest: Vec<String>,
}

#[cfg(unix)]
fn get_fs_dirs(dirs: Vec<PathBuf>) -> Result<Vec<Vec<PathBuf>>, String> {
    use proc_mounts::MountIter;

    let mounts = MountIter::new()
        .map_err(|e| format!("Error parsing proc_mounts: {}", e))?
        .map(|m| m.map(|m| (m.dest, m.source)))
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(|e| format!("Error parsing proc/mounts line: {}", e))?;
    let mut fs_dirs: HashMap<&PathBuf, Vec<_>> = HashMap::new();

    'outer: for dir in dirs {
        let mut trunc_dir = dir.clone();
        loop {
            if let Some(source) = mounts.get(&trunc_dir) {
                fs_dirs.entry(source).or_default().push(dir);
                continue 'outer;
            }

            if !trunc_dir.pop() {
                break;
            }
        }

        return Err(format!("Couldn't find device of path {}", dir.display()));
    }

    Ok(fs_dirs.into_iter().map(|(_, v)| v).collect())
}

#[cfg(windows)]
fn get_fs_dirs(dirs: Vec<PathBuf>) -> Result<Vec<Vec<PathBuf>>, String> {
    use std::{
        path::{Component, PrefixComponent},
        rc::Rc,
    };

    let fs_dirs: Vec<_> = {
        let dirs: Vec<_> = dirs.into_iter().map(Rc::new).collect();
        let mut fs_dirs: HashMap<PrefixComponent, Vec<Rc<PathBuf>>> = HashMap::new();
        for dir in dirs.iter() {
            match dir.components().next() {
                Some(Component::Prefix(p)) => fs_dirs.entry(p).or_default().push(Rc::clone(dir)),
                c => {
                    return Err(format!(
                        "Unexpected path component for {}: {:?}",
                        dir.display(),
                        c
                    ))
                }
            }
        }

        fs_dirs.into_iter().map(|(_, v)| v).collect()
    };

    Ok(fs_dirs
        .into_iter()
        .map(|d| d.into_iter().map(|d| Rc::try_unwrap(d).unwrap()).collect())
        .collect())
}

pub struct MainThreadPool {
    handles: Vec<JoinHandle<()>>,
}

impl MainThreadPool {
    fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, f: F)
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        self.handles.push(thread::spawn(f));
    }
}

impl Drop for MainThreadPool {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn main() -> Result<(), String> {
    let mut term_handle = match unsafe { gracile::init_handle() } {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error adding signal handlers: {}", e);
            TermHandle::default()
        }
    };

    let args = Args::parse();

    let dirs = args
        .rest
        .iter()
        .map(|d| {
            fs::canonicalize(d).map_err(|e| match e.kind() {
                ErrorKind::NotFound => format!("Path {} does not exist", d),
                _ => format!("Error trying to canonicalize path {}: {}", d, e),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let data_out_file = match args
        .output_data
        .as_ref()
        .map(|o| XxhDiffData::new(&PathBuf::from(o), false))
    {
        Some(Ok(d)) => Some(d),
        None => None,
        Some(Err(e)) => return Err(format!("Error opening data out file: {}", e)),
    };

    let read_done = Arc::new(AtomicBool::new(
        data_out_file.as_ref().map_or(true, |o| !o.is_read()),
    ));
    let data_out_file = Arc::new(data_out_file.map(Cell::new).map(Mutex::new));
    let existing_hashes = Arc::default();

    let mut data_file = match args
        .data
        .map(|d| XxhDiffData::new(&PathBuf::from(d), true).map(|d| (d, HashMap::new())))
    {
        Some(Ok(d)) => Some(d),
        None => None,
        Some(Err(e)) => match e.kind() {
            ErrorKind::NotFound => return Err("Data file not found".to_string()),
            _ => return Err(format!("Error opening data file: {}", e)),
        },
    };

    let (tx, rx) = flume::unbounded();
    let mut unparkers = Vec::new();
    let mut thread_pool = MainThreadPool::new();
    let fd_sem = Arc::new(Semaphore::new(args.max_files_open as isize));
    let term_rx = term_handle.rx().clone();

    for dirs in get_fs_dirs(dirs)? {
        let (path_rx, unparker) =
            paths::start_paths_thread(dirs, &existing_hashes, &read_done, &mut thread_pool);
        unparkers.push(unparker);

        thread_pool.spawn({
            let send_hash = tx.clone();
            let term_rx = term_rx.clone();
            let err_handle = term_handle.err_handle.clone();
            let fd_sem = Arc::clone(&fd_sem);
            move || {
                let parallel_hash = ParallelHash {
                    path_rx,
                    err_handle,
                    fd_sem,
                };

                parallel_hash::hash_paths(parallel_hash, send_hash, term_rx);
            }
        });
    }

    drop(tx);

    let mut new_results = if let Some(data_out_file_inner) = &*data_out_file {
        if data_out_file_inner.lock().get_mut().is_read() {
            thread_pool.spawn({
                let data_out_file = Arc::clone(&data_out_file);
                let read_done = Arc::clone(&read_done);
                let existing_hashes = Arc::clone(&existing_hashes);
                let err_handle = term_handle.err_handle.clone();
                move || {
                    if let Some(data_out_file) = &*data_out_file {
                        let existing_hashes = existing_hashes.pin();
                        loop {
                            if TERMINATE.get() {
                                break;
                            }

                            let mut data_out_file = data_out_file.lock();
                            match data_out_file.get_mut().read() {
                                Ok(HashResult(path, hash)) => {
                                    existing_hashes.insert(path, hash);
                                    unparkers.iter().for_each(Unparker::unpark);
                                }
                                Err(DataErr::Empty) => break,
                                Err(e) => {
                                    err_handle.term_err(format!(
                                        "Error reading from existing data out file: {}",
                                        e
                                    ));
                                    break;
                                }
                            }
                        }
                    }

                    read_done.store(true, Ordering::Release);
                    unparkers.iter().for_each(Unparker::unpark);
                }
            });

            Some(Vec::new())
        } else {
            None
        }
    } else {
        None
    };

    loop {
        enum SelectorMsg {
            Hash(Result<HashResult, RecvError>),
            Err(Result<String, RecvError>),
            Term,
        }

        match Selector::new()
            .recv(&rx, SelectorMsg::Hash)
            .recv(&term_handle.err_rx, SelectorMsg::Err)
            .recv(&term_rx, |_| SelectorMsg::Term)
            .wait()
        {
            SelectorMsg::Hash(msg) => match msg {
                Ok(hash) => {
                    let mut hashes: Vec<HashResult> =
                        iter::once(hash).chain(rx.try_iter()).collect();
                    let write_hashes: Vec<_> = hashes.iter().collect();

                    for HashResult(hash_path, hash) in write_hashes.iter() {
                        let hash_matches =
                            if let Some((ref mut data_file, ref mut data_hashes)) = data_file {
                                if let Some(data_hash) = data_hashes.get(hash_path) {
                                    data_hash == hash
                                } else {
                                    let mut data_hash_res = data_file.read();
                                    loop {
                                        match data_hash_res {
                                            Ok(HashResult(data_path, data_hash)) => {
                                                let matches = data_path == *hash_path;
                                                data_hashes.insert(data_path, data_hash);
                                                if matches {
                                                    break data_hash == *hash;
                                                }
                                                data_hash_res = data_file.read();
                                            }
                                            Err(DataErr::Empty) => break false,
                                            Err(e) => {
                                                return Err(format!(
                                                    "Error reading from data file: {}",
                                                    e
                                                ))
                                            }
                                        }
                                    }
                                }
                            } else {
                                false
                            };

                        if !hash_matches {
                            if let Err(e) = io::stdout()
                                .write_all(&match hash_path.try_as_bytes() {
                                    Ok(p) => p,
                                    Err(p) => {
                                        return Err(format!(
                                            "Couldn't convert path buf {} to bytes",
                                            p.display()
                                        ))
                                    }
                                })
                                .and_then(|_| io::stdout().write_all(&[0xA]))
                            {
                                return Err(format!("Error writing path to stdout: {}", e));
                            }
                        }
                    }

                    if let Err(e) = io::stdout().flush() {
                        return Err(format!("Error flushing stdout: {}", e));
                    }

                    if let Some(data_out_file) = &*data_out_file {
                        if let Err(e) = data_out_file.lock().get_mut().write(&write_hashes) {
                            return Err(format!(
                                "Error writing hash results to data output file: {}",
                                e
                            ));
                        }
                    }

                    if let Some(results) = new_results.as_mut() {
                        results.append(&mut hashes);
                    }
                }
                Err(_) => break,
            },
            SelectorMsg::Err(msg) => {
                if let Ok(e) = msg {
                    TERMINATE.set();
                    return Err(e);
                }
            }
            SelectorMsg::Term => break,
        }

        if let Some(hashes) = new_results.as_ref() {
            if read_done.load(Ordering::Acquire) {
                if let Some(data_out_file) = &*data_out_file {
                    let mut data_out_file = data_out_file.lock();
                    if let XxhDiffData::Read(_, ReadXxhDiffDataInner { status, .. }) =
                        &*data_out_file.get_mut()
                    {
                        if status.is_err() {
                            let existing_hashes: Vec<_> = existing_hashes
                                .pin()
                                .iter()
                                .map(|(k, v)| HashResult(k.clone(), *v))
                                .collect();
                            let write_hashes: Vec<_> =
                                existing_hashes.iter().chain(hashes).collect();

                            if let Some(ref output_data) = args.output_data {
                                match XxhDiffData::reset(&PathBuf::from(output_data)) {
                                    Ok(new_data) => drop(data_out_file.replace(new_data)),
                                    Err(e) => return Err(format!("Failed to open data output file when attempting to reset: {}", e)),
                                }

                                if let Err(e) = data_out_file.get_mut().write(&write_hashes) {
                                    return Err(format!(
                                        "Failed to write to new data output file: {}",
                                        e
                                    ));
                                }
                            }
                        }

                        new_results = None;
                    }
                }
            }
        }
    }

    Ok(())
}
