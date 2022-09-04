use std::{
    io::ErrorKind,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crossbeam_utils::sync::{Parker, Unparker};
use flume::Receiver;
use flurry::HashMap;
use gracile::TERMINATE;

use crate::MainThreadPool;

pub fn start_paths_thread(
    paths: Vec<PathBuf>,
    existing_hashes: &Arc<HashMap<PathBuf, u64>>,
    read_done: &Arc<AtomicBool>,
    thread_pool: &mut MainThreadPool,
) -> (Receiver<PathBuf>, Unparker) {
    let (tx, rx) = flume::unbounded();

    let parker = Parker::new();
    let unparker = parker.unparker().clone();

    thread_pool.spawn({
        let existing_hashes = Arc::clone(existing_hashes);
        let read_done = Arc::clone(read_done);
        move || {
            let existing_hashes = existing_hashes.pin();

            let maybe_send = |path| {
                loop {
                    if existing_hashes.contains_key(&path) {
                        return false;
                    }
                    if read_done.load(Ordering::Acquire) {
                        break;
                    }
                    parker.park();
                }
                let _ = tx.send(path);
                true
            };

            let mut paths: Vec<_> = paths
                .into_iter()
                .filter_map(|p| match p.symlink_metadata() {
                    Ok(m) if m.is_file() => {
                        maybe_send(p);
                        None
                    }
                    Ok(_) => Some(p),
                    Err(e) => {
                        eprintln!("Error getting metadata for path {}: {}", p.display(), e);
                        None
                    }
                })
                .collect();

            while let Some(path) = paths.pop() {
                if TERMINATE.get() {
                    break;
                }

                let dir = match path.read_dir() {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("Error reading dir {}: {}", path.display(), e);
                        continue;
                    }
                };

                for file in dir {
                    if TERMINATE.get() {
                        break;
                    }

                    let file = match file {
                        Ok(f) => f,
                        Err(e) => {
                            eprintln!("Error getting dir entry of {}: {}", path.display(), e);
                            if e.kind() == ErrorKind::InvalidInput {
                                break;
                            }
                            continue;
                        }
                    };

                    let file_type = match file.file_type() {
                        Ok(ft) => ft,
                        Err(e) => {
                            eprintln!(
                                "Error getting file type of {}: {}",
                                file.path().display(),
                                e
                            );
                            continue;
                        }
                    };

                    if file_type.is_file() {
                        maybe_send(file.path());
                    } else if file_type.is_dir() {
                        paths.push(file.path());
                    }
                }
            }
        }
    });

    (rx, unparker)
}
