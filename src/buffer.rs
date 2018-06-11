use std::collections::{HashMap, VecDeque};

use crossbeam_channel::{Receiver, Sender};

use cache::{Cache, Entry};
use ptr::Sendable;
use storage::{Io, IoRequest, IoResponse};

enum Lock {
    Lock(u32),
    Unlock(u32),
}

pub struct DataRequest {
    lock: Lock,
    res: Option<Sender<DataResponse>>,
}

impl DataRequest {
    pub fn lock(key: u32, res: Sender<DataResponse>) -> DataRequest {
        DataRequest {
            lock: Lock::Lock(key),
            res: Some(res),
        }
    }

    pub fn unlock(key: u32) -> DataRequest {
        DataRequest {
            lock: Lock::Unlock(key),
            res: None,
        }
    }
}

pub type DataResponse = Sendable<Entry<u32, u64>>;
type ResponseQueue = VecDeque<Option<Sender<DataResponse>>>;

pub struct Buffer {
    data_req_rx: Receiver<DataRequest>,
    data_end_rx: Receiver<()>,
    io_req_tx: Sender<IoRequest>,
    io_res_rx: Receiver<IoResponse>,
    /// Mapping of key to queue of requesting clients
    data_res_queues: HashMap<u32, ResponseQueue>,
    cache: Cache<u32, u64>,
}

impl Buffer {
    pub fn new(
        data_req_rx: Receiver<DataRequest>,
        data_end_rx: Receiver<()>,
        io_req_tx: Sender<IoRequest>,
        io_res_rx: Receiver<IoResponse>,
    ) -> Buffer {
        Buffer {
            data_req_rx,
            data_end_rx,
            io_req_tx,
            io_res_rx,
            data_res_queues: HashMap::new(),
            cache: Cache::new(100, 5),
        }
    }

    pub fn start(&mut self) {
        'outer: loop {
            select_loop! {
                recv(self.data_req_rx, client_req) => {
                    self.handle_data_req(client_req);
                }

                recv(self.data_end_rx, _) => {
                    self.io_req_tx.disconnect();
                }

                recv(self.io_res_rx, io_res) => {
                    self.handle_io_res(io_res);
                }

                disconnected() => break 'outer,
            }
        }
    }

    fn handle_data_req(&mut self, req: DataRequest) {
        match req.lock {
            Lock::Lock(key) => {
                let can_be_locked = {
                    let mut queue = self.data_res_queues.entry(key).or_insert(VecDeque::new());
                    let can_be_locked = queue.is_empty();
                    queue.push_back(req.res.clone());
                    can_be_locked
                };

                // Respond only when no other clients are requesting the key
                if can_be_locked {
                    if self.cache.contains(key) {
                        // The entry is in cache.
                        self.cache.touch(key);
                        let entry = self.cache.get_untouched(key);
                        req.res.unwrap().send(Sendable::new(&*entry)).unwrap();
                    } else {
                        // The entry is not in cache.
                        let (entry, evicting) = self.cache.get_with_evicting(key);

                        // Read the entry from storage
                        let req = IoRequest::read(key, Sendable::new(&entry.value));
                        self.io_req_tx.send(req).unwrap();

                        // Evict old entry
                        if let Some(evicting) = evicting {
                            request_eviction(&mut self.data_res_queues, &evicting, &self.io_req_tx);
                        }
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
                        res.send(Sendable::new(&*entry)).unwrap();
                    }
                    _ => {}
                }
            }
        }
    }

    fn handle_io_res(&mut self, res: IoResponse) {
        match res.io {
            Io::Read(key) => {
                if let Err(err) = res.result {
                    panic!(err);
                }

                // Respond to the requesting client
                let queue = self.data_res_queues.get_mut(&key).unwrap();
                match queue.front().unwrap() {
                    Some(res) => {
                        let entry = self.cache.get_untouched(key);
                        res.send(Sendable::new(&*entry)).unwrap();
                    }
                    _ => unreachable!(),
                }
            }

            Io::Write(key) => {
                if let Err(err) = res.result {
                    panic!(err);
                }

                let will_evict_other = {
                    let mut queue = self.data_res_queues.get_mut(&key).unwrap();

                    // Delete writing lock
                    queue.pop_front().unwrap();

                    match queue.front() {
                        Some(Some(res)) => {
                            // If another client requested lock while evicting, respond to it
                            let entry = self.cache.get_untouched(key);
                            res.send(Sendable::new(&*entry)).unwrap();
                            true
                        }
                        Some(None) => unreachable!(),
                        None => {
                            self.cache.evicting_done(key);
                            false
                        }
                    }
                };

                // Evict other old entry
                if will_evict_other {
                    let evicting = self.cache.evicting_new();
                    request_eviction(&mut self.data_res_queues, &evicting, &self.io_req_tx);
                }
            }
        }
    }
}

fn request_eviction(
    queues: &mut HashMap<u32, ResponseQueue>,
    entry: &Entry<u32, u64>,
    io_req_tx: &Sender<IoRequest>,
) {
    // Add writing lock
    let queue = queues.get_mut(&entry.key).unwrap();
    queue.push_front(None);

    let req = IoRequest::write(entry.key, Sendable::new(&entry.value));
    io_req_tx.send(req).unwrap();
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crossbeam_channel::bounded;

    use super::*;

    #[test]
    fn start_ends() {
        let (data_req_tx, data_req_rx) = bounded(0);
        let (data_end_tx, data_end_rx) = bounded(0);
        let (io_req_tx, io_req_rx) = bounded(0);
        let (io_res_tx, io_res_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            for _ in io_req_rx {}
            let _ = io_res_tx;
        });

        let buffer_thread = thread::spawn(move || {
            let mut buffer = Buffer::new(data_req_rx, data_end_rx, io_req_tx, io_res_rx);
            buffer.start();
        });

        data_req_tx.disconnect();
        data_end_tx.send(()).unwrap();
        data_end_tx.disconnect();

        buffer_thread.join().unwrap();
        storage_thread.join().unwrap();
    }
}
