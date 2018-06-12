use crossbeam_channel::{bounded, Sender};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

use buffer::DataRequest;

pub struct Client {
    req_tx: Sender<DataRequest>,
}

impl Client {
    pub fn new(req_tx: Sender<DataRequest>) -> Client {
        Client { req_tx }
    }

    pub fn start(&self, n_data: u32, n_increments: usize) {
        let u = Uniform::new(0, n_data);

        for key in thread_rng().sample_iter(&u).take(n_increments) {
            let (res_tx, res_rx) = bounded(0);
            let req = DataRequest::lock(key, res_tx);
            self.req_tx.send(req);

            let mut res = res_rx.recv().unwrap();
            let entry = res.as_mut();
            entry.dirty = true;
            entry.value += 1;

            self.req_tx.send(DataRequest::unlock(key));
        }
    }
}
