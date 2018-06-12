extern crate crossbeam_channel;
extern crate rand;

extern crate fakedb;

use std::thread;

use crossbeam_channel::{bounded, unbounded};
use rand::{thread_rng, Rng};

use fakedb::buffer::{Buffer, DataRequest};
use fakedb::client::Client;
use fakedb::storage::Storage;

#[test]
fn random_increments() {
    let n_writers: u32 = 4;
    let n_data: u32 = 10000;

    let (data_req_tx, data_req_rx) = bounded(0);
    // Use unbounded channel for eviction
    let (io_req_tx, io_req_rx) = unbounded();
    let (io_res_tx, io_res_rx) = bounded(0);

    let storage_thread = thread::spawn(move || {
        let mut storage = Storage::new(
            "tmp/integration_1.db".to_string(),
            n_data,
            io_req_rx,
            io_res_tx,
        ).unwrap();
        storage.start();
    });

    let buffer_thread = thread::spawn(move || {
        // Make threshold big enough
        let mut buffer = Buffer::new(200, 10, io_req_tx);
        buffer.start(data_req_rx, io_res_rx);
    });

    let mut writers = Vec::with_capacity(n_writers as usize);
    for _ in 0..n_writers {
        let data_req_tx = data_req_tx.clone();
        let t = thread::spawn(move || {
            let client = Client::new(data_req_tx);
            client.start(n_data, (n_data / n_writers) as usize);
        });
        writers.push(t);
    }
    for t in writers {
        t.join().unwrap();
    }

    let mut sum = 0;
    for key in 0..n_data {
        let (tx, rx) = bounded(0);
        let req = DataRequest::lock(key, tx);
        data_req_tx.send(req);

        let mut res = rx.recv().unwrap();
        let entry = res.as_ref();
        sum += entry.value;

        data_req_tx.send(DataRequest::unlock(key));
    }
    assert_eq!(sum, n_data as u64);

    drop(data_req_tx);
    buffer_thread.join().unwrap();
    storage_thread.join().unwrap();
}

#[test]
fn random_read_and_increments() {
    let n_readers: u32 = 2;
    let n_writers: u32 = 2;
    let n_data: u32 = 10000;

    let (data_req_tx, data_req_rx) = bounded(0);
    // Use unbounded channel for eviction
    let (io_req_tx, io_req_rx) = unbounded();
    let (io_res_tx, io_res_rx) = bounded(0);

    let storage_thread = thread::spawn(move || {
        let mut storage = Storage::new(
            "tmp/integration_2.db".to_string(),
            n_data,
            io_req_rx,
            io_res_tx,
        ).unwrap();
        storage.start();
    });

    let buffer_thread = thread::spawn(move || {
        // Make threshold big enough
        let mut buffer = Buffer::new(200, 10, io_req_tx);
        buffer.start(data_req_rx, io_res_rx);
    });

    let mut clients = Vec::with_capacity((n_writers + n_readers) as usize);

    for _ in 0..n_readers {
        let data_req_tx = data_req_tx.clone();
        let t = thread::spawn(move || {
            let mut keys: Vec<_> = (0..n_data).collect();
            thread_rng().shuffle(&mut keys);
            for key in keys {
                let (tx, rx) = bounded(0);
                data_req_tx.send(DataRequest::lock(key, tx));
                rx.recv().unwrap();
                data_req_tx.send(DataRequest::unlock(key));
            }
        });
        clients.push(t);
    }

    for _ in 0..n_writers {
        let data_req_tx = data_req_tx.clone();
        let t = thread::spawn(move || {
            let client = Client::new(data_req_tx);
            client.start(n_data, (n_data / n_writers) as usize);
        });
        clients.push(t);
    }

    for t in clients {
        if let Err(err) = t.join() {
            panic!(err);
        }
    }

    let mut sum = 0;
    for key in 0..n_data {
        let (tx, rx) = bounded(0);
        let req = DataRequest::lock(key, tx);
        data_req_tx.send(req);

        let mut res = rx.recv().unwrap();
        let entry = res.as_ref();
        sum += entry.value;

        data_req_tx.send(DataRequest::unlock(key));
    }
    assert_eq!(sum, n_data as u64);

    drop(data_req_tx);
    buffer_thread.join().unwrap();
    storage_thread.join().unwrap();
}
