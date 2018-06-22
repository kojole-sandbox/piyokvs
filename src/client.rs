use std::sync::Arc;

use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

use buffer::Buffer;
use entry::State;

pub struct Client {
    buffer: Arc<Buffer>,
}

impl Client {
    pub fn new(buffer: Arc<Buffer>) -> Client {
        Client { buffer }
    }

    pub fn start(&self, n_data: u32, n_increments: usize) {
        let u = Uniform::new(0, n_data);

        for key in thread_rng().sample_iter(&u).take(n_increments) {
            let mut entry = self.buffer.lock(key).unwrap();
            entry.value += 1;
            entry.state = State::Dirty;
        }
    }
}
