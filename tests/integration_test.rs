extern crate rand;

extern crate piyokvs;

use std::sync::Arc;
use std::thread;

use rand::{thread_rng, Rng};

use piyokvs::buffer::{Buffer, BufferImpl};
use piyokvs::cache::LruCache;
use piyokvs::client::Client;
use piyokvs::storage::StorageImpl;

#[test]
fn random_increments() {
    let n_writers: u32 = 4;
    let n_data: u32 = 10000;

    let storage = Box::new(StorageImpl::new("tmp/integration_1.db", n_data).unwrap());
    let cache = Box::new(LruCache::new(100));
    let buffer = Arc::new(BufferImpl::new(cache, storage));

    let mut writers = Vec::with_capacity(n_writers as usize);
    for _ in 0..n_writers {
        let buffer = buffer.clone();
        let t = thread::spawn(move || {
            let client = Client::new(buffer);
            client.start(n_data, (n_data / n_writers) as usize);
        });
        writers.push(t);
    }
    for t in writers {
        t.join().unwrap();
    }

    buffer.sync().unwrap();

    let mut sum = 0;
    for key in 0..n_data {
        let entry = buffer.lock(key).unwrap();
        sum += entry.value;
    }
    assert_eq!(sum, n_data as u64);
}

#[test]
fn random_read_and_increments() {
    let n_readers: u32 = 2;
    let n_writers: u32 = 2;
    let n_data: u32 = 10000;

    let storage = Box::new(StorageImpl::new("tmp/integration_2.db", n_data).unwrap());
    let cache = Box::new(LruCache::new(100));
    let buffer = Arc::new(BufferImpl::new(cache, storage));

    let mut clients = Vec::with_capacity((n_writers + n_readers) as usize);

    for _ in 0..n_writers {
        let buffer = buffer.clone();
        let t = thread::spawn(move || {
            let client = Client::new(buffer);
            client.start(n_data, (n_data / n_writers) as usize);
            0
        });
        clients.push(t);
    }

    for _ in 0..n_readers {
        let buffer = buffer.clone();
        let t = thread::spawn(move || {
            let mut keys: Vec<_> = (0..n_data).collect();
            thread_rng().shuffle(&mut keys);
            let mut sum = 0;
            for key in keys {
                let entry = buffer.lock(key).unwrap();
                sum += entry.value;
            }
            sum
        });
        clients.push(t);
    }

    for t in clients {
        t.join().unwrap();
    }

    buffer.sync().unwrap();

    let mut sum = 0;
    for key in 0..n_data {
        let entry = buffer.lock(key).unwrap();
        sum += entry.value;
    }
    assert_eq!(sum, n_data as u64);
}
