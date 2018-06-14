use std::collections::{HashMap, HashSet, VecDeque};

use crossbeam_channel::{Receiver, Sender};

use cache::{Cache, Entry, LruIterator};
use ptr::Sendable;
use storage::{Io, IoRequest, IoResponse};

#[derive(Debug)]
enum Lock {
    Lock(u32),
    Unlock(u32),
}

#[derive(Debug)]
pub struct DataRequest {
    lock: Lock,
    res_tx: Option<Sender<DataResponse>>,
}

impl DataRequest {
    pub fn lock(key: u32, res_tx: Sender<DataResponse>) -> DataRequest {
        DataRequest {
            lock: Lock::Lock(key),
            res_tx: Some(res_tx),
        }
    }

    pub fn unlock(key: u32) -> DataRequest {
        DataRequest {
            lock: Lock::Unlock(key),
            res_tx: None,
        }
    }
}

pub type DataResponse = Sendable<Entry<u32, u64>>;

type ResponseQueue = VecDeque<Option<Sender<DataResponse>>>;

pub struct Buffer {
    io_req_tx: Sender<IoRequest>,
    /// Issuing I/O keys
    io_req_keys: HashSet<u32>,
    /// Mapping of key to queue of requesting clients
    data_res_queues: HashMap<u32, ResponseQueue>,
    cache: Cache<u32, u64>,
}

impl Buffer {
    pub fn new(capacity: usize, threthold: usize, io_req_tx: Sender<IoRequest>) -> Buffer {
        Buffer {
            io_req_tx,
            io_req_keys: HashSet::new(),
            data_res_queues: HashMap::with_capacity(capacity),
            cache: Cache::new(capacity, threthold),
        }
    }

    pub fn start(&mut self, data_req_rx: Receiver<DataRequest>, io_res_rx: Receiver<IoResponse>) {
        'main: loop {
            // Handle available I/O responses first
            'handle_io: loop {
                select! {
                    recv(io_res_rx, io_res) => self.handle_io_res(io_res.unwrap()),
                    default => break 'handle_io,
                }
            }

            select! {
                recv(io_res_rx, io_res) => self.handle_io_res(io_res.unwrap()),
                recv(data_req_rx, client_req) => {
                    if let Some(req) = client_req {
                        self.handle_data_req(req);
                    } else {
                        // Connection is closed
                        break 'main;
                    }
                }
            }
        }

        // Ensure that issued I/Os have finished
        while !self.io_req_keys.is_empty() {
            select! {
                recv(io_res_rx, io_res) => self.handle_io_res(io_res.unwrap()),
            }
        }

        self.write_back(io_res_rx);
    }

    fn handle_data_req(&mut self, req: DataRequest) {
        match req.lock {
            Lock::Lock(key) => {
                let can_be_locked = {
                    let mut queue = self.data_res_queues.entry(key).or_insert(VecDeque::new());
                    let can_be_locked = queue.is_empty();
                    queue.push_back(req.res_tx.clone());
                    can_be_locked
                };

                // Respond only when no other clients are requesting the key
                if can_be_locked {
                    if self.cache.contains(key) {
                        // The entry is in cache.
                        self.cache.touch(key);
                        let entry = self.cache.get_untouched(key);
                        req.res_tx.unwrap().send(Sendable::new(&*entry));
                    } else {
                        // The entry is not in cache.
                        let (entry, evicting) = self.cache.get_with_evicting(key);

                        // Evict old entry first
                        if let Some(evicting) = evicting {
                            request_eviction(
                                &mut self.data_res_queues,
                                &evicting,
                                &self.io_req_tx,
                                &mut self.io_req_keys,
                            );
                        }

                        // Read the entry from storage
                        let req = IoRequest::read(key, Sendable::new(&entry.value));
                        self.io_req_tx.send(req);
                        self.io_req_keys.insert(key);
                    }
                } else {
                    // Assume that the entry is in cache
                    self.cache.touch(key);
                }
            }

            Lock::Unlock(key) => {
                let mut queue = self.data_res_queues.get_mut(&key).unwrap();

                // Assume that the client is in the front of the queue
                queue.pop_front().unwrap();

                // Respond to another requesting client if exists
                match queue.front() {
                    Some(Some(res)) => {
                        let entry = self.cache.get_untouched(key);
                        res.send(Sendable::new(&*entry));
                    }
                    _ => {}
                }
            }
        }
    }

    fn handle_io_res(&mut self, res: IoResponse) {
        match res.io {
            Io::Read(key) => {
                res.result.unwrap();
                self.io_req_keys.remove(&key);

                // Respond to the requesting client
                let queue = self.data_res_queues.get_mut(&key).unwrap();
                match queue.front().unwrap() {
                    Some(res) => {
                        let entry = self.cache.get_untouched(key);
                        res.send(Sendable::new(&*entry));
                    }
                    _ => unreachable!(),
                }
            }

            Io::Write(key) => {
                res.result.unwrap();
                self.io_req_keys.remove(&key);

                let will_evict_other = {
                    let mut queue = self.data_res_queues.get_mut(&key).unwrap();

                    // Delete writing lock
                    queue.pop_front().unwrap();

                    match queue.front() {
                        Some(Some(res)) => {
                            // If another client requested lock while evicting, respond to it
                            let entry = self.cache.get_untouched(key);
                            res.send(Sendable::new(&*entry));
                            true
                        }
                        Some(None) => unreachable!(),
                        None => {
                            self.cache.evicting_done(key);
                            false
                        }
                    }
                };

                if will_evict_other {
                    // Evict other old entry
                    if let Some(evicting) = self.cache.evicting_new() {
                        request_eviction(
                            &mut self.data_res_queues,
                            &evicting,
                            &self.io_req_tx,
                            &mut self.io_req_keys,
                        );
                    }
                } else {
                    // Prevent HashMap from bloating
                    self.data_res_queues.remove(&key);
                }
            }
        }
    }

    fn write_back(&self, io_res_rx: Receiver<IoResponse>) {
        let iter = LruIterator::new(&self.cache);
        for r in iter {
            let entry = r.borrow();
            if entry.dirty {
                let req = IoRequest::write(entry.key, Sendable::new(&entry.value));
                self.io_req_tx.send(req);

                let res = io_res_rx.recv().unwrap();
                res.result.unwrap();
            }
        }
    }
}

fn request_eviction(
    queues: &mut HashMap<u32, ResponseQueue>,
    entry: &Entry<u32, u64>,
    io_req_tx: &Sender<IoRequest>,
    io_req_keys: &mut HashSet<u32>,
) {
    // Add writing lock
    let queue = queues.get_mut(&entry.key).unwrap();
    queue.push_front(None);

    let req = IoRequest::write(entry.key, Sendable::new(&entry.value));
    io_req_tx.send(req);
    io_req_keys.insert(entry.key);
}

#[cfg(test)]
mod tests {
    use std::iter::repeat;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;

    use crossbeam_channel::{bounded, unbounded};

    use super::*;
    use storage::MockStorage;

    #[test]
    fn start_ends() {
        let (data_req_tx, data_req_rx) = bounded(0);
        let (io_req_tx, io_req_rx) = bounded(0);
        let (io_res_tx, io_res_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            for _ in io_req_rx {}
            let _ = io_res_tx;
        });

        let buffer_thread = thread::spawn(move || {
            let mut buffer = Buffer::new(2, 1, io_req_tx);
            buffer.start(data_req_rx, io_res_rx);
        });

        drop(data_req_tx);

        buffer_thread.join().unwrap();
        storage_thread.join().unwrap();
    }

    #[test]
    fn single() {
        let n_data: u32 = 100;
        let data: Arc<Vec<AtomicU64>> = Arc::new(
            repeat(0)
                .take(n_data as usize)
                .map(|v| AtomicU64::new(v))
                .collect(),
        );

        let (data_req_tx, data_req_rx) = bounded(0);
        let (io_req_tx, io_req_rx) = bounded(0);
        let (io_res_tx, io_res_rx) = unbounded();

        let storage_thread = {
            let data = data.clone();
            thread::spawn(move || {
                let mut storage = MockStorage::new(data);
                storage.start(io_req_rx, io_res_tx);
            })
        };

        let buffer_thread = thread::spawn(move || {
            let mut buffer = Buffer::new(10, 2, io_req_tx);
            buffer.start(data_req_rx, io_res_rx);
        });

        for key in 0..n_data {
            let (tx, rx) = bounded(0);
            data_req_tx.send(DataRequest::lock(key, tx));

            let mut res = rx.recv().unwrap();
            if key % 2 == 0 {
                let entry = res.as_mut();
                entry.dirty = true;
                entry.value = key as u64;
            }

            data_req_tx.send(DataRequest::unlock(key));
        }
        drop(data_req_tx);

        buffer_thread.join().unwrap();
        storage_thread.join().unwrap();

        for key in 0..n_data {
            let expected = if key % 2 == 0 { key as u64 } else { 0 };
            assert_eq!(data[key as usize].load(Ordering::Relaxed), expected);
        }
    }

    #[test]
    fn concurrent() {
        let n_data: u32 = 100;
        let data: Arc<Vec<AtomicU64>> = Arc::new(
            repeat(0)
                .take(n_data as usize)
                .map(|v| AtomicU64::new(v))
                .collect(),
        );

        let (data_req_tx, data_req_rx) = bounded(0);
        let (io_req_tx, io_req_rx) = bounded(0);
        let (io_res_tx, io_res_rx) = unbounded();

        let storage_thread = {
            let data = data.clone();
            thread::spawn(move || {
                let mut storage = MockStorage::new(data);
                storage.start(io_req_rx, io_res_tx);
            })
        };

        let buffer_thread = thread::spawn(move || {
            let mut buffer = Buffer::new(20, 5, io_req_tx);
            buffer.start(data_req_rx, io_res_rx);
        });

        let n_clients = 4;
        let mut clients = Vec::with_capacity(n_clients);
        for _ in 0..n_clients {
            let data_req_tx = data_req_tx.clone();

            let t = thread::spawn(move || {
                for key in 0..n_data {
                    let (tx, rx) = bounded(0);
                    data_req_tx.send(DataRequest::lock(key, tx));

                    let mut res = rx.recv().unwrap();
                    let entry = res.as_mut();
                    entry.dirty = true;
                    entry.value += 1;

                    data_req_tx.send(DataRequest::unlock(key));
                }
            });
            clients.push(t);
        }
        for t in clients {
            t.join().unwrap();
        }
        drop(data_req_tx);

        buffer_thread.join().unwrap();
        storage_thread.join().unwrap();

        for key in 0..n_data {
            assert_eq!(data[key as usize].load(Ordering::Relaxed), n_clients as u64);
        }
    }
}
