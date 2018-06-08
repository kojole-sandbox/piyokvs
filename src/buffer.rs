use std::collections::{HashMap, VecDeque};
use std::marker::Send;
use std::ptr::NonNull;

use crossbeam_channel::{Receiver, Sender};

#[derive(Copy, Clone)]
pub struct DataPtr(pub NonNull<u64>);

unsafe impl Send for DataPtr {}

enum Lock {
    Lock(usize),
    Unlock(usize),
}

pub struct Request {
    lock: Lock,
    res: Option<Sender<DataPtr>>,
}

impl Request {
    pub fn lock(id: usize, res: Sender<DataPtr>) -> Request {
        Request {
            lock: Lock::Lock(id),
            res: Some(res),
        }
    }

    pub fn unlock(id: usize) -> Request {
        Request {
            lock: Lock::Unlock(id),
            res: None,
        }
    }
}

pub struct Buffer {
    req_rx: Receiver<Request>,
}

impl Buffer {
    pub fn new(req_rx: Receiver<Request>) -> Buffer {
        Buffer { req_rx }
    }

    pub fn start(&self) {
        let mut data = vec![0u64; 10];
        let mut locks: HashMap<usize, VecDeque<Sender<DataPtr>>> = HashMap::new();

        for req in self.req_rx.iter() {
            match req.lock {
                Lock::Lock(id) => {
                    let mut queue = locks.entry(id).or_insert(VecDeque::new());
                    let res = req.res.unwrap();

                    // Serve only when no other threads are locking the id
                    if queue.is_empty() {
                        let p = DataPtr(NonNull::new(&mut data[id]).unwrap());
                        res.send(p).unwrap();
                    }

                    queue.push_back(res.clone());
                }

                Lock::Unlock(id) => {
                    let mut queue = locks.get_mut(&id).unwrap();

                    // Assume that the thread is in the front of the queue
                    queue.pop_front().unwrap();

                    // Serve another thread if exists
                    if let Some(res) = queue.front() {
                        let p = DataPtr(NonNull::new(&mut data[id]).unwrap());
                        res.send(p).unwrap();
                    }
                }
            }
        }

        println!("data: {:?}", data);
        println!("sum = {}", data.into_iter().sum::<u64>());
    }
}
