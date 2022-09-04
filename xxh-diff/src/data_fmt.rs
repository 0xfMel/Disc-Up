use std::{
    fmt::Display,
    fmt::{self, Formatter},
    fs::File,
    io::{self, ErrorKind, Read, Seek, SeekFrom, Write},
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use crate::raw_path_bytes::RawPathBytes;

#[derive(Debug)]
pub struct HashResult(pub PathBuf, pub u64);

pub enum ReadStatus {
    Open,
    Stopped,
    Error,
}

impl ReadStatus {
    fn is_stop(&self) -> bool {
        !matches!(self, ReadStatus::Open)
    }

    pub fn is_err(&self) -> bool {
        matches!(self, ReadStatus::Error)
    }
}

pub struct ReadXxhDiffDataInner {
    pub status: ReadStatus,
    initial_len: u64,
    cursor_pos: Option<u64>,
}

impl ReadXxhDiffDataInner {
    fn new(file: &mut File) -> io::Result<Self> {
        let initial_len = file.seek(SeekFrom::End(0))?;
        let status = match initial_len {
            0 => ReadStatus::Stopped,
            _ => ReadStatus::Open,
        };
        file.rewind()?;

        Ok(Self {
            status,
            initial_len,
            cursor_pos: None,
        })
    }
}

pub enum XxhDiffData {
    Read(File, ReadXxhDiffDataInner),
    Write(File),
}

const U64_BYTES: u32 = u64::BITS / 8;
const USIZE_BYTES: u32 = usize::BITS / 8;
const HEAD_SIZE: u32 = U64_BYTES + USIZE_BYTES;

#[derive(Debug)]
pub enum DataErr {
    Empty,
    IOErr(io::Error),
    ParseErr(String),
}

impl Display for DataErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "No more data"),
            Self::IOErr(e) => e.fmt(f),
            Self::ParseErr(e) => write!(f, "{}", e),
        }
    }
}

impl XxhDiffData {
    pub fn new(path: &Path, read_required: bool) -> io::Result<Self> {
        let mut opts = File::options();
        let opts = opts
            .append(true)
            .create_new(!read_required)
            .read(read_required);
        match opts.open(path) {
            Ok(file) => XxhDiffData::from_file(file, read_required),
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => {
                    let file = opts.read(true).create_new(false).open(path)?;
                    XxhDiffData::from_file(file, true)
                }
                _ => Err(e),
            },
        }
    }

    fn from_file(mut file: File, read: bool) -> io::Result<Self> {
        match read {
            true => {
                let inner = ReadXxhDiffDataInner::new(&mut file)?;
                Ok(Self::Read(file, inner))
            }
            false => Ok(Self::Write(file)),
        }
    }

    pub fn reset(path: &Path) -> io::Result<Self> {
        Ok(XxhDiffData::Write(
            File::options()
                .write(true)
                .truncate(true)
                .create(true)
                .open(path)?,
        ))
    }

    pub fn is_read(&self) -> bool {
        matches!(self, Self::Read(..))
    }

    pub fn read(&mut self) -> Result<HashResult, DataErr> {
        match self {
            Self::Write(_) => Err(DataErr::Empty),
            Self::Read(
                file,
                ReadXxhDiffDataInner {
                    status,
                    initial_len,
                    cursor_pos,
                },
            ) => {
                if status.is_stop() {
                    return Err(DataErr::Empty);
                }

                if let Some(cursor_pos) = cursor_pos.take() {
                    if let Err(e) = file.seek(SeekFrom::Start(cursor_pos)) {
                        *status = ReadStatus::Error;
                        return Err(DataErr::IOErr(e));
                    }
                }

                let mut hlen: MaybeUninit<[u8; 1]> = MaybeUninit::uninit();
                let hlen = unsafe { hlen.assume_init_mut() };
                if let Err(e) = file.read_exact(hlen) {
                    *status = ReadStatus::Error;
                    return Err(DataErr::IOErr(e));
                }

                let mut head: Vec<u8> = vec![0; hlen[0] as usize];
                if let Err(e) = file.read_exact(&mut head) {
                    *status = ReadStatus::Error;
                    return Err(DataErr::IOErr(e));
                }

                if head.len() != HEAD_SIZE as usize {
                    *status = ReadStatus::Error;
                    return Err(DataErr::ParseErr(format!(
                        "Wrong number of bytes in head: {:?}",
                        head
                    )));
                }

                let (hash_head, head_path_len) = head.split_at(U64_BYTES as usize);
                let hash = u64::from_le_bytes(hash_head.try_into().unwrap());
                let path_len = usize::from_le_bytes(head_path_len.try_into().unwrap());

                let mut path_buf: Vec<u8> = vec![0; path_len];
                if let Err(e) = file.read_exact(&mut path_buf) {
                    *status = ReadStatus::Error;
                    return Err(DataErr::IOErr(e));
                }

                let path_buf = match PathBuf::try_from_bytes(path_buf) {
                    Ok(p) => p,
                    Err(p) => {
                        *status = ReadStatus::Error;
                        return Err(DataErr::ParseErr(format!(
                            "Couldn't parse path bytes {:?} to path buf",
                            p
                        )));
                    }
                };

                let pos = match file.stream_position() {
                    Ok(p) => p,
                    Err(e) => {
                        *status = ReadStatus::Error;
                        return Err(DataErr::IOErr(e));
                    }
                };

                if pos >= *initial_len {
                    *status = ReadStatus::Stopped;

                    if pos > *initial_len {
                        return Err(DataErr::Empty);
                    }
                }

                Ok(HashResult(path_buf, hash))
            }
        }
    }

    pub fn write(&mut self, results: &[&HashResult]) -> Result<(), DataErr> {
        if results.is_empty() {
            return Ok(());
        }

        let (file, cursor_pos) = match self {
            Self::Read(file, ReadXxhDiffDataInner { cursor_pos, .. }) => (file, Some(cursor_pos)),
            Self::Write(f) => (f, None),
        };

        match cursor_pos {
            Some(c) if c.is_none() => *c = Some(file.stream_position().map_err(DataErr::IOErr)?),
            _ => {}
        }

        for result in results {
            fn write_result(
                file: &mut File,
                HashResult(path, hash): &HashResult,
            ) -> Result<(), DataErr> {
                let path_bytes = match path.try_as_bytes() {
                    Ok(p) => p,
                    Err(p) => {
                        return Err(DataErr::ParseErr(format!(
                            "Couldn't convert path buf {} to bytes",
                            p.display()
                        )))
                    }
                };
                file.write_all(&[HEAD_SIZE as u8]).map_err(DataErr::IOErr)?;
                file.write_all(&hash.to_le_bytes())
                    .map_err(DataErr::IOErr)?;
                file.write_all(&path_bytes.len().to_le_bytes())
                    .map_err(DataErr::IOErr)?;
                file.write_all(&path_bytes).map_err(DataErr::IOErr)
            }

            write_result(file, result)?;
        }

        file.flush().map_err(DataErr::IOErr)
    }
}
