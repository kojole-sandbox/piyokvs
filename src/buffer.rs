use std::io;
use std::ptr::NonNull;
use std::sync::{Mutex, MutexGuard};

use cache::Cache;
use entry::{Entry, Lazy, State};
use storage::Storage;

impl Lazy for u64 {
    fn init(&mut self) {
        *self = 0;
    }
}

pub trait Buffer {
    fn lock(&self, key: u32) -> io::Result<MutexGuard<Entry<u32, u64>>>;
    fn sync(&self) -> io::Result<()>;
}

pub struct BufferImpl {
    cache: Box<Cache<u32, u64> + Send + Sync>,
    storage: Mutex<Box<Storage + Send>>,
}

impl BufferImpl {
    pub fn new(
        cache: Box<Cache<u32, u64> + Send + Sync>,
        storage: Box<Storage + Send>,
    ) -> BufferImpl {
        BufferImpl {
            cache,
            storage: Mutex::new(storage),
        }
    }
}

impl Buffer for BufferImpl {
    fn lock(&self, key: u32) -> io::Result<MutexGuard<Entry<u32, u64>>> {
        let mut entry = self.cache.lock(key);

        match entry.state {
            State::Unloaded => {
                // Read
                let ptr = unsafe { NonNull::new_unchecked(&mut entry.value) };
                self.storage.lock().unwrap().read(key, ptr)?;
                entry.state = State::Fresh;
            }

            State::Stale(stale_key) => {
                // Write back and read
                let ptr = unsafe { NonNull::new_unchecked(&mut entry.value) };
                let mut storage = self.storage.lock().unwrap();
                storage.write(stale_key, ptr)?;
                storage.read(key, ptr)?;
                entry.state = State::Fresh;
            }

            _ => {}
        }

        Ok(entry)
    }

    fn sync(&self) -> io::Result<()> {
        let dirty_entries = self.cache.dirty_entries();
        let mut storage = self.storage.lock().unwrap();
        for mut entry in dirty_entries {
            let ptr = unsafe { NonNull::new_unchecked(&mut entry.value) };
            storage.write(entry.key, ptr)?;
            entry.state = State::Fresh;
        }
        storage.sync()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use cache::{LruCache, SingleCache};
    use storage::StorageMock;

    use super::*;

    fn assert_data(n_data: u32, buffer: &impl Buffer) {
        for key in 0..n_data {
            assert_eq!(buffer.lock(key).unwrap().value, key as u64);
        }
    }

    #[test]
    fn single_buffer() {
        let n_data: u32 = 10000;

        let cache = Box::new(SingleCache::new());
        let storage = Box::new(StorageMock::new());
        let buffer = BufferImpl::new(cache, storage);

        for key in 0..n_data {
            let mut entry = buffer.lock(key).unwrap();
            entry.value = key as u64;
            entry.state = State::Dirty;
        }

        assert_data(n_data, &buffer);
    }

    #[test]
    fn threaded_single_buffer() {
        let n_data: u32 = 10000;
        let n_writers: u32 = 10;
        let n_data_per_writer = n_data / n_writers;

        let cache = Box::new(SingleCache::new());
        let storage = Box::new(StorageMock::new());
        let buffer = Arc::new(BufferImpl::new(cache, storage));

        let mut writers = Vec::with_capacity(n_writers as usize);

        for i in 0..n_writers {
            let buffer = buffer.clone();
            let start = i * n_data_per_writer;
            let count = n_data_per_writer as usize;

            let t = thread::spawn(move || {
                for key in (start..).take(count) {
                    let mut entry = buffer.lock(key).unwrap();
                    entry.value = key as u64;
                    entry.state = State::Dirty;
                }
            });

            writers.push(t);
        }

        for t in writers {
            t.join().unwrap();
        }

        buffer.sync().unwrap();

        assert_data(n_data, &*buffer);
    }

    #[test]
    fn single_lru_buffer() {
        let n_data: u32 = 10000;

        let cache = Box::new(LruCache::new(100));
        let storage = Box::new(StorageMock::new());
        let buffer = BufferImpl::new(cache, storage);

        for key in 0..n_data {
            let mut entry = buffer.lock(key).unwrap();
            entry.value = key as u64;
            entry.state = State::Dirty;
        }

        assert_data(n_data, &buffer);
    }

    #[test]
    fn threaded_lru_bufer() {
        let n_data: u32 = 10000;
        let n_writers: u32 = 10;
        let n_data_per_writer = n_data / n_writers;

        let cache = Box::new(LruCache::new(100));
        let storage = Box::new(StorageMock::new());
        let buffer = Arc::new(BufferImpl::new(cache, storage));

        let mut writers = Vec::with_capacity(n_writers as usize);

        for i in 0..n_writers {
            let buffer = buffer.clone();
            let start = i * n_data_per_writer;
            let count = n_data_per_writer as usize;

            let t = thread::spawn(move || {
                for key in (start..).take(count) {
                    let mut entry = buffer.lock(key).unwrap();
                    entry.value = key as u64;
                    entry.state = State::Dirty;
                }
            });

            writers.push(t);
        }

        for t in writers {
            t.join().unwrap();
        }

        buffer.sync().unwrap();

        assert_data(n_data, &*buffer);
    }
}
