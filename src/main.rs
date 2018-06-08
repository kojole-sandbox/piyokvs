extern crate crossbeam_channel;

use std::thread;

use crossbeam_channel::bounded;

extern crate fakedb;
use fakedb::buffer::Buffer;
use fakedb::client::Client;

fn main() {
    let n_threds = 20;
    let n_updates = 100;
    println!("n_threads = {}", n_threds);
    println!("n_updates = {}", n_updates);

    let (req_tx, req_rx) = bounded(0);

    let buffer = thread::spawn(move || {
        let buffer = Buffer::new(req_rx);
        buffer.start();
    });

    let mut clients = Vec::new();
    for _ in 0..n_threds {
        let req_tx = req_tx.clone();
        let t = thread::spawn(move || {
            let client = Client::new(req_tx);
            client.start(n_updates);
        });
        clients.push(t);
    }
    for client in clients {
        client.join().unwrap();
    }
    drop(req_tx);

    buffer.join().unwrap();
}
