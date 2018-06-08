use crossbeam_channel::{bounded, Sender};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

use buffer::Request;

pub struct Client {
    req_tx: Sender<Request>,
}

impl Client {
    pub fn new(req_tx: Sender<Request>) -> Client {
        Client { req_tx }
    }

    pub fn start(&self, n: usize) {
        let (res_tx, res_rx) = bounded(0);
        let u = Uniform::new(0usize, 10);

        for id in thread_rng().sample_iter(&u).take(n) {
            let req = Request::lock(id, res_tx.clone());
            self.req_tx.send(req).unwrap();

            let mut data_wrapper = res_rx.recv().unwrap();
            unsafe {
                *data_wrapper.0.as_mut() += 1;
            }

            let req = Request::unlock(id);
            self.req_tx.send(req).unwrap();
        }
    }
}
