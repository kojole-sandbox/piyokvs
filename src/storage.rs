use std::cmp;
#[cfg(test)]
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::mem::{size_of, transmute};
use std::path::Path;
use std::ptr::NonNull;
use std::slice;

pub trait Storage {
    fn read(&mut self, key: u32, dst: NonNull<u64>) -> io::Result<()>;
    fn write(&mut self, key: u32, src: NonNull<u64>) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
}

pub struct StorageImpl {
    file: File,
}

impl StorageImpl {
    pub fn new<P>(path: P, n_data: u32) -> io::Result<StorageImpl>
    where
        P: AsRef<Path>,
    {
        assert!(n_data > 0);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;
        write_zeros(&mut file, (n_data as u64) * (size_of::<u64>() as u64))?;

        Ok(StorageImpl { file })
    }
}

impl Storage for StorageImpl {
    fn read(&mut self, key: u32, dst: NonNull<u64>) -> io::Result<()> {
        let pos = key as u64 * size_of::<u64>() as u64;
        read_at(&mut self.file, pos, dst.as_ptr())
    }

    fn write(&mut self, key: u32, src: NonNull<u64>) -> io::Result<()> {
        let pos = key as u64 * size_of::<u64>() as u64;
        write_at(&mut self.file, pos, src.as_ptr() as *const u64)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

/// Write `size` zeros
fn write_zeros<W>(writer: &mut W, mut size: u64) -> io::Result<()>
where
    W: Write,
{
    const MAX_BUF_SIZE: u64 = 4096;
    let zeros = [0u8; MAX_BUF_SIZE as usize];

    while size > 0 {
        let buf_size = cmp::min(size, MAX_BUF_SIZE);
        writer.write_all(&zeros[0..buf_size as usize])?;
        size -= buf_size;
    }

    Ok(())
}

fn read_at<R>(reader: &mut R, pos: u64, dst: *mut u64) -> io::Result<()>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(pos))?;
    read(reader, dst)
}

fn write_at<W>(writer: &mut W, pos: u64, src: *const u64) -> io::Result<()>
where
    W: Write + Seek,
{
    writer.seek(SeekFrom::Start(pos))?;
    write(writer, src)
}

fn read<R>(reader: &mut R, dst: *mut u64) -> io::Result<()>
where
    R: Read,
{
    unsafe {
        let ptr: *mut u8 = transmute(dst);
        reader.read_exact(slice::from_raw_parts_mut(ptr, size_of::<u64>()))
    }
}

fn write<W>(writer: &mut W, src: *const u64) -> io::Result<()>
where
    W: Write,
{
    unsafe {
        let ptr: *const u8 = transmute(src);
        writer.write_all(slice::from_raw_parts(ptr, size_of::<u64>()))
    }
}

#[cfg(test)]
pub struct StorageMock {
    data: HashMap<u32, u64>,
}

#[cfg(test)]
impl StorageMock {
    pub fn new() -> StorageMock {
        StorageMock {
            data: HashMap::new(),
        }
    }
}

#[cfg(test)]
impl Storage for StorageMock {
    fn read(&mut self, key: u32, mut dst: NonNull<u64>) -> io::Result<()> {
        unsafe { *dst.as_mut() = *self.data.entry(key).or_insert(0) };
        Ok(())
    }

    fn write(&mut self, key: u32, src: NonNull<u64>) -> io::Result<()> {
        self.data.insert(key, unsafe { *src.as_ref() });
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::mem::uninitialized;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use super::*;

    #[test]
    fn test_write_zeros() {
        let mut buf = Vec::with_capacity(5000);
        write_zeros(&mut buf, 42).unwrap();
        assert_eq!(buf.len(), 42);

        buf.clear();
        write_zeros(&mut buf, 4096).unwrap();
        assert_eq!(buf.len(), 4096);

        buf.clear();
        write_zeros(&mut buf, 4097).unwrap();
        assert_eq!(buf.len(), 4097);
    }

    #[test]
    fn test_read() {
        let mut reader = Cursor::new([0u8; size_of::<u64>()]);
        let mut data: u64 = 42;
        read(&mut reader, &mut data as *mut u64).unwrap();
        assert_eq!(data, 0);
    }

    #[test]
    fn test_write() {
        let mut writer = Vec::with_capacity(size_of::<u64>());
        let data: u64 = 0;
        write(&mut writer, &data as *const u64).unwrap();
        assert_eq!(&writer, &[0u8; size_of::<u64>()]);
    }

    fn assert_data(n_data: u32, storage: &mut impl Storage) {
        for key in 0..n_data {
            unsafe {
                let mut data = uninitialized();
                let dst = NonNull::new_unchecked(&mut data);
                storage.read(key, dst).unwrap();

                assert_eq!(data, key as u64);
            }
        }
    }

    #[test]
    fn single_write() {
        let n_data: u32 = 10000;
        let mut storage = StorageImpl::new("tmp/storage_1.db", n_data).unwrap();

        for key in 0..n_data {
            let mut data = key as u64;
            let src = unsafe { NonNull::new_unchecked(&mut data) };
            storage.write(key, src).unwrap();
        }

        assert_data(n_data, &mut storage);
    }

    #[test]
    fn concurrent_writes() {
        let n_data: u32 = 10000;
        let n_writers: u32 = 10;
        let n_data_per_writer = n_data / n_writers;

        let storage = StorageImpl::new("tmp/storage_2.db", n_data).unwrap();
        let storage = Arc::new(Mutex::new(storage));

        let mut writers = Vec::with_capacity(n_writers as usize);

        for i in 0..n_writers {
            let storage = storage.clone();
            let start = i * n_data_per_writer;
            let count = n_data_per_writer as usize;

            let t = thread::spawn(move || {
                for key in (start..).take(count) {
                    let mut data = key as u64;
                    let src = unsafe { NonNull::new_unchecked(&mut data) };
                    storage.lock().unwrap().write(key, src).unwrap();
                }
            });
            writers.push(t);
        }

        for t in writers {
            t.join().unwrap();
        }

        assert_data(n_data, &mut *storage.lock().unwrap());
    }

    #[test]
    fn mock_single_write() {
        let n_data: u32 = 10000;
        let mut storage = StorageMock::new();

        for key in 0..n_data {
            let mut data = key as u64;
            let src = unsafe { NonNull::new_unchecked(&mut data) };
            storage.write(key, src).unwrap();
        }

        assert_data(n_data, &mut storage);
    }

    #[test]
    fn mock_concurrent_writes() {
        let n_data: u32 = 10000;
        let n_writers: u32 = 10;
        let n_data_per_writer = n_data / n_writers;

        let storage = StorageMock::new();
        let storage = Arc::new(Mutex::new(storage));

        let mut writers = Vec::with_capacity(n_writers as usize);

        for i in 0..n_writers {
            let storage = storage.clone();
            let start = i * n_data_per_writer;
            let count = n_data_per_writer as usize;

            let t = thread::spawn(move || {
                for key in (start..).take(count) {
                    let mut data = key as u64;
                    let src = unsafe { NonNull::new_unchecked(&mut data) };
                    storage.lock().unwrap().write(key, src).unwrap();
                }
            });
            writers.push(t);
        }

        for t in writers {
            t.join().unwrap();
        }

        assert_data(n_data, &mut *storage.lock().unwrap());
    }
}
