use std::cmp;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::mem::{size_of, transmute};
use std::slice;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use ptr::Sendable;

#[derive(Debug)]
pub enum Io {
    Read(u32),
    Write(u32),
}

#[derive(Debug)]
pub struct IoRequest {
    io: Io,
    ptr: Sendable<u64>,
}

impl IoRequest {
    pub fn read(key: u32, ptr: Sendable<u64>) -> IoRequest {
        IoRequest {
            io: Io::Read(key),
            ptr,
        }
    }

    pub fn write(key: u32, ptr: Sendable<u64>) -> IoRequest {
        IoRequest {
            io: Io::Write(key),
            ptr,
        }
    }
}

#[derive(Debug)]
pub struct IoResponse {
    pub io: Io,
    pub result: io::Result<()>,
}

impl IoResponse {
    fn new(io: Io, result: io::Result<()>) -> IoResponse {
        IoResponse { io, result }
    }
}

pub struct Storage {
    path: String,
}

impl Storage {
    pub fn new(path: String, n_data: u32) -> io::Result<Storage> {
        assert!(n_data > 0);

        {
            let mut file = File::create(&path)?;
            write_zeros(&mut file, (n_data as u64) * (size_of::<u64>() as u64))?;
        }

        Ok(Storage { path })
    }

    pub fn start(
        &mut self,
        n_threads: usize,
        req_rx: Receiver<IoRequest>,
        res_tx: Sender<IoResponse>,
    ) {
        // If # of threads is one, the response channel must be unbounded.
        assert!((n_threads == 1 && res_tx.capacity().is_none()) || n_threads > 1);

        let mut threads = Vec::with_capacity(n_threads);

        for _ in 0..n_threads {
            let path = self.path.clone();
            let req_rx = req_rx.clone();
            let res_tx = res_tx.clone();

            let t = thread::spawn(move || {
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .unwrap();

                for mut req in req_rx {
                    let result = match req.io {
                        Io::Read(key) => {
                            let pos = key as u64 * size_of::<u64>() as u64;
                            read_at(&mut file, pos, req.ptr.as_mut())
                        }
                        Io::Write(key) => {
                            let pos = key as u64 * size_of::<u64>() as u64;
                            write_at(&mut file, pos, req.ptr.as_ref())
                        }
                    };

                    let response = IoResponse::new(req.io, result);
                    res_tx.send(response);
                }
            });

            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }
    }
}

/// Write `size` zeros
fn write_zeros<W>(writer: &mut W, mut size: u64) -> io::Result<()>
where
    W: Write,
{
    const MAX_BUF_SIZE: u64 = 4096;
    let zeros = [0u8; MAX_BUF_SIZE as usize];

    while size > 0 {
        let buf_size = cmp::min(size, MAX_BUF_SIZE);
        writer.write_all(&zeros[0..buf_size as usize])?;
        size -= buf_size;
    }

    Ok(())
}

fn read_at<R>(reader: &mut R, pos: u64, dst: *mut u64) -> io::Result<()>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(pos))?;
    read(reader, dst)
}

fn write_at<W>(writer: &mut W, pos: u64, src: *const u64) -> io::Result<()>
where
    W: Write + Seek,
{
    writer.seek(SeekFrom::Start(pos))?;
    write(writer, src)
}

fn read<R>(reader: &mut R, dst: *mut u64) -> io::Result<()>
where
    R: Read,
{
    unsafe {
        let ptr: *mut u8 = transmute(dst);
        reader.read_exact(slice::from_raw_parts_mut(ptr, size_of::<u64>()))
    }
}

fn write<W>(writer: &mut W, src: *const u64) -> io::Result<()>
where
    W: Write,
{
    unsafe {
        let ptr: *const u8 = transmute(src);
        writer.write_all(slice::from_raw_parts(ptr, size_of::<u64>()))
    }
}

#[cfg(test)]
pub(crate) struct MockStorage {
    data: Arc<Vec<AtomicU64>>,
}

#[cfg(test)]
impl MockStorage {
    pub(crate) fn new(data: Arc<Vec<AtomicU64>>) -> MockStorage {
        MockStorage { data }
    }

    pub(crate) fn start(&mut self, req_rx: Receiver<IoRequest>, res_tx: Sender<IoResponse>) {
        // Response channel must be unbounded.
        assert!(res_tx.capacity().is_none());

        for mut req in req_rx {
            match req.io {
                Io::Read(key) => {
                    *req.ptr.as_mut() = self.data[key as usize].load(Ordering::Relaxed);
                }
                Io::Write(key) => {
                    self.data[key as usize].store(*req.ptr.as_ref(), Ordering::Relaxed);
                }
            }
            res_tx.send(IoResponse::new(req.io, Ok(())));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::iter::repeat;
    use std::thread::JoinHandle;

    use crossbeam_channel::{bounded, unbounded};

    use super::*;

    #[test]
    fn test_write_zeros() {
        let mut buf = Vec::with_capacity(5000);
        write_zeros(&mut buf, 42).unwrap();
        assert_eq!(buf.len(), 42);

        buf.clear();
        write_zeros(&mut buf, 4096).unwrap();
        assert_eq!(buf.len(), 4096);

        buf.clear();
        write_zeros(&mut buf, 4097).unwrap();
        assert_eq!(buf.len(), 4097);
    }

    #[test]
    fn test_read() {
        let mut reader = Cursor::new([0u8; size_of::<u64>()]);
        let mut data: u64 = 42;
        read(&mut reader, &mut data as *mut u64).unwrap();
        assert_eq!(data, 0);
    }

    #[test]
    fn test_write() {
        let mut writer = Vec::with_capacity(size_of::<u64>());
        let data: u64 = 0;
        write(&mut writer, &data as *const u64).unwrap();
        assert_eq!(&writer, &[0u8; size_of::<u64>()]);
    }

    fn assert_data(n_data: u32, req_tx: Sender<IoRequest>, res_rx: Receiver<IoResponse>) {
        for key in 0..n_data {
            let mut data = u64::max_value();
            req_tx.send(IoRequest::read(key, Sendable::new(&mut data)));
            assert!(res_rx.recv().unwrap().result.is_ok());
            assert_eq!(data, key as u64);
        }
    }

    fn spawn_writer(
        start: u64,
        count: usize,
        req_tx: Sender<IoRequest>,
        res_rx: Option<Receiver<IoResponse>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            let data: Vec<u64> = (start..).take(count).collect();
            for i in 0..data.len() {
                req_tx.send(IoRequest::write(data[i] as u32, Sendable::new(&data[i])));

                if let Some(ref res_rx) = res_rx {
                    assert!(res_rx.recv().unwrap().result.is_ok());
                }
            }
        })
    }

    #[test]
    fn single_req_single_io() {
        let n_data: u32 = 10000;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = unbounded();

        let storage_thread = thread::spawn(move || {
            let mut storage = Storage::new("tmp/storage_1.db".to_string(), n_data).unwrap();
            storage.start(1, req_rx, res_tx);
        });

        let writer = spawn_writer(0, n_data as usize, req_tx.clone(), Some(res_rx.clone()));
        writer.join().unwrap();

        assert_data(n_data, req_tx.clone(), res_rx.clone());

        drop(req_tx);
        storage_thread.join().unwrap();
    }

    #[test]
    fn single_nonblock_req_single_io() {
        let n_data: u32 = 100;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = unbounded();

        let storage_thread = thread::spawn(move || {
            let mut storage = Storage::new("tmp/storage_2.db".to_string(), n_data).unwrap();
            storage.start(1, req_rx, res_tx);
        });

        let writer = spawn_writer(0, n_data as usize, req_tx.clone(), None);
        for _ in 0..n_data {
            assert!(res_rx.recv().unwrap().result.is_ok());
        }
        writer.join().unwrap();

        assert_data(n_data, req_tx.clone(), res_rx.clone());

        drop(req_tx);
        storage_thread.join().unwrap();
    }

    #[test]
    fn concurrent_req_single_io() {
        let n_writers: u32 = 10;
        let n_data: u32 = 10000;
        let n_data_per_writer = n_data / n_writers;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = unbounded();

        let storage_thread = thread::spawn(move || {
            let mut storage = Storage::new("tmp/storage_3.db".to_string(), n_data).unwrap();
            storage.start(1, req_rx, res_tx);
        });

        let writers = (0..n_writers)
            .map(|i| {
                spawn_writer(
                    (i * n_data_per_writer) as u64,
                    n_data_per_writer as usize,
                    req_tx.clone(),
                    None,
                )
            })
            .collect::<Vec<_>>();

        for _ in 0..n_data {
            assert!(res_rx.recv().unwrap().result.is_ok());
        }

        for writer in writers {
            writer.join().unwrap();
        }

        assert_data(n_data, req_tx.clone(), res_rx.clone());

        drop(req_tx);
        storage_thread.join().unwrap();
    }

    #[test]
    fn concurrent_req_concurrent_io() {
        let n_writers: u32 = 10;
        let n_data: u32 = 10000;
        let n_data_per_writer = n_data / n_writers;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            let mut storage = Storage::new("tmp/storage_4.db".to_string(), n_data).unwrap();
            storage.start(4, req_rx, res_tx);
        });

        let writers = (0..n_writers)
            .map(|i| {
                spawn_writer(
                    (i * n_data_per_writer) as u64,
                    n_data_per_writer as usize,
                    req_tx.clone(),
                    None,
                )
            })
            .collect::<Vec<_>>();

        for _ in 0..n_data {
            assert!(res_rx.recv().unwrap().result.is_ok());
        }

        for writer in writers {
            writer.join().unwrap();
        }

        assert_data(n_data, req_tx.clone(), res_rx.clone());

        drop(req_tx);
        storage_thread.join().unwrap();
    }

    #[test]
    fn mock_single_req_single_io() {
        let n_data: u32 = 100;
        let data: Arc<Vec<AtomicU64>> = Arc::new(
            repeat(0)
                .take(n_data as usize)
                .map(|v| AtomicU64::new(v))
                .collect(),
        );

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = unbounded();

        let storage_thread = {
            let data = data.clone();
            thread::spawn(move || {
                let mut storage = MockStorage::new(data);
                storage.start(req_rx, res_tx);
            })
        };

        let writer = spawn_writer(0, n_data as usize, req_tx, Some(res_rx));
        writer.join().unwrap();

        storage_thread.join().unwrap();

        for key in 0..n_data {
            assert_eq!(data[key as usize].load(Ordering::Relaxed), key as u64);
        }
    }

    #[test]
    fn mock_single_nonblock_req_single_io() {
        let n_data: u32 = 100;
        let data: Arc<Vec<AtomicU64>> = Arc::new(
            repeat(0)
                .take(n_data as usize)
                .map(|v| AtomicU64::new(v))
                .collect(),
        );

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = unbounded();

        let storage_thread = {
            let data = data.clone();
            thread::spawn(move || {
                let mut storage = MockStorage::new(data);
                storage.start(req_rx, res_tx);
            })
        };

        let writer = spawn_writer(0, n_data as usize, req_tx, None);

        for _ in 0..n_data {
            assert!(res_rx.recv().unwrap().result.is_ok());
        }
        drop(res_rx);

        writer.join().unwrap();
        storage_thread.join().unwrap();

        for key in 0..n_data {
            assert_eq!(data[key as usize].load(Ordering::Relaxed), key as u64);
        }
    }
}
