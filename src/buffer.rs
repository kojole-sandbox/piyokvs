// use std::collections::{HashMap, HashSet, VecDeque};

// use crossbeam_channel::{Receiver, Sender};

// use cache::{Cache, Entry, LruIterator};
// use ptr::Sendable;
// use storage::{Io, IoRequest, IoResponse};

use std::io;
use std::mem::uninitialized;
use std::ptr::NonNull;
use std::sync::Mutex;

use storage::Storage;

pub trait Buffer {
    fn get(&self, key: u32) -> io::Result<u64>;
    fn set(&self, key: u32, value: u64) -> io::Result<()>;
}

pub struct BufferImpl {
    storage: Mutex<Box<Storage + Send>>,
}

impl BufferImpl {
    pub fn new(storage: Box<Storage + Send>) -> BufferImpl {
        BufferImpl {
            storage: Mutex::new(storage),
        }
    }
}

impl Buffer for BufferImpl {
    fn get(&self, key: u32) -> io::Result<u64> {
        unsafe {
            let mut data: u64 = uninitialized();
            let dst = NonNull::new_unchecked(&mut data);
            self.storage.lock().unwrap().read(key, dst)?;
            Ok(data)
        }
    }

    fn set(&self, key: u32, value: u64) -> io::Result<()> {
        let mut value = value;
        let src = unsafe { NonNull::new_unchecked(&mut value) };
        self.storage.lock().unwrap().write(key, src)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use storage::StorageMock;

    use super::*;

    fn assert_data(n_data: u32, buffer: &impl Buffer) {
        for key in 0..n_data {
            assert_eq!(buffer.get(key).unwrap(), key as u64);
        }
    }

    #[test]
    fn single_write() {
        let n_data: u32 = 10000;

        let storage = StorageMock::new();
        let buffer = BufferImpl::new(Box::new(storage));

        for key in 0..n_data {
            buffer.set(key, key as u64).unwrap();
        }

        assert_data(n_data, &buffer);
    }

    #[test]
    fn concurrent_writes() {
        let n_data: u32 = 10000;
        let n_writers: u32 = 10;
        let n_data_per_writer = n_data / n_writers;

        let storage = StorageMock::new();
        let buffer = BufferImpl::new(Box::new(storage));
        let buffer = Arc::new(buffer);

        let mut writers = Vec::with_capacity(n_writers as usize);

        for i in 0..n_writers {
            let buffer = buffer.clone();
            let start = i * n_data_per_writer;
            let count = n_data_per_writer as usize;

            let t = thread::spawn(move || {
                for key in (start..).take(count) {
                    buffer.set(key, key as u64).unwrap();
                }
            });
            writers.push(t);
        }

        for t in writers {
            t.join().unwrap();
        }

        assert_data(n_data, &*buffer);
    }
}
