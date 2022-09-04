use std::{ffi::OsString, path::PathBuf};

#[cfg(windows)]
use std::os::windows::prelude::*;

#[cfg(unix)]
use std::os::unix::prelude::*;

pub trait RawPathBytes {
    fn try_as_bytes(&self) -> Result<Vec<u8>, &Self>;
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Vec<u8>>
    where
        Self: Sized;
}

#[cfg(windows)]
impl RawPathBytes for PathBuf {
    fn try_as_bytes(&self) -> Result<Vec<u8>, &Self> {
        let bytes: Vec<_> = self.as_os_str().encode_wide().collect();
        let (prefix, bytes, suffix) = unsafe { bytes.align_to::<u8>() };
        if prefix.len() != 0 || suffix.len() != 0 {
            Err(self)
        } else {
            Ok(bytes.to_vec())
        }
    }

    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Vec<u8>> {
        let (prefix, shorts, suffix) = unsafe { bytes.align_to::<u16>() };
        if prefix.len() != 0 || suffix.len() != 0 {
            Err(bytes)
        } else {
            Ok(OsString::from_wide(shorts).into())
        }
    }
}

#[cfg(unix)]
impl RawPathBytes for PathBuf {
    fn try_as_bytes(&self) -> Result<Vec<u8>, &Self> {
        Ok(self.as_os_str().as_bytes().to_vec())
    }

    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Vec<u8>> {
        Ok(OsString::from_vec(bytes).into())
    }
}
