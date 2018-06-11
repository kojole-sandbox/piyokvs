use std::cmp;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::mem::{size_of, transmute};
use std::slice;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use ptr::Sendable;

const N_THREADS: usize = 4;

pub enum Io {
    Read(u32),
    Write(u32),
}

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
    req_rx: Receiver<IoRequest>,
    res_tx: Sender<IoResponse>,
}

impl Storage {
    pub fn new(
        path: String,
        n_data: u32,
        req_rx: Receiver<IoRequest>,
        res_tx: Sender<IoResponse>,
    ) -> io::Result<Storage> {
        assert!(n_data > 0);

        {
            let mut file = File::create(&path)?;
            write_zeros(&mut file, (n_data as u64) * (size_of::<u64>() as u64))?;
        }

        Ok(Storage {
            path,
            req_rx,
            res_tx,
        })
    }

    pub fn start(&mut self) {
        let mut threads = Vec::new();

        for _ in 0..N_THREADS {
            let path = self.path.clone();
            let req_rx = self.req_rx.clone();
            let res_tx = self.res_tx.clone();

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
                    res_tx.send(response).unwrap();
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
mod tests {
    use std::io::Cursor;

    use crossbeam_channel::bounded;

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

    #[test]
    fn storage_single() {
        let n_data: u32 = 10000;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            let mut storage =
                Storage::new("tmp/storage_1.db".to_string(), n_data, req_rx, res_tx).unwrap();
            storage.start();
        });

        {
            let req_tx = req_tx.clone();
            let res_rx = res_rx.clone();

            for key in 0..n_data {
                let data = key as u64;
                let ptr = Sendable::new(&data);
                let req = IoRequest::write(key, ptr);
                req_tx.send(req).unwrap();
                assert!(res_rx.recv().unwrap().result.is_ok());
            }
        };

        for key in 0..n_data {
            let mut data = u64::max_value();
            let ptr = Sendable::new(&mut data);
            let req = IoRequest::read(key, ptr);
            req_tx.send(req).unwrap();
            assert!(res_rx.recv().unwrap().result.is_ok());
            assert_eq!(data, key as u64);
        }

        drop(req_tx);
        storage_thread.join().unwrap();
    }

    #[test]
    fn storage_single_nonblock() {
        let n_data: u32 = 100;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            let mut storage =
                Storage::new("tmp/storage_2.db".to_string(), n_data, req_rx, res_tx).unwrap();
            storage.start();
        });

        let writer = {
            let req_tx = req_tx.clone();

            thread::spawn(move || {
                let data: Vec<u64> = (0..n_data as u64).collect();
                for key in 0..n_data {
                    let ptr = Sendable::new(&data[key as usize]);
                    let req = IoRequest::write(key, ptr);
                    req_tx.send(req).unwrap();
                }
            })
        };

        for _ in 0..n_data {
            assert!(res_rx.recv().unwrap().result.is_ok());
        }
        writer.join().unwrap();

        for key in 0..n_data {
            let mut data = u64::max_value();
            let ptr = Sendable::new(&mut data);
            let req = IoRequest::read(key, ptr);
            req_tx.send(req).unwrap();
            assert!(res_rx.recv().unwrap().result.is_ok());
            assert_eq!(data, key as u64);
        }

        drop(req_tx);
        storage_thread.join().unwrap();
    }

    #[test]
    fn storage_concurrent() {
        let n_writers: u32 = 10;
        let n_data: u32 = 10000;
        let n_data_per_writer = n_data / n_writers;

        let (req_tx, req_rx) = bounded(0);
        let (res_tx, res_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            let mut storage =
                Storage::new("tmp/storage_3.db".to_string(), n_data, req_rx, res_tx).unwrap();
            storage.start();
        });

        let mut writers = Vec::with_capacity(n_writers as usize);
        for i in 0..n_writers {
            let req_tx = req_tx.clone();

            let t = thread::spawn(move || {
                let data: Vec<u64> = ((i * n_data_per_writer) as u64..)
                    .take(n_data_per_writer as usize)
                    .collect();

                for j in 0..n_data_per_writer {
                    let key = data[j as usize] as u32;
                    let ptr = Sendable::new(&data[j as usize]);
                    let req = IoRequest::write(key, ptr);
                    req_tx.send(req).unwrap();
                }
            });

            writers.push(t);
        }

        for _ in 0..n_data {
            assert!(res_rx.recv().unwrap().result.is_ok());
        }

        for writer in writers {
            writer.join().unwrap();
        }

        for key in 0..n_data {
            let mut data = u64::max_value();
            let ptr = Sendable::new(&mut data);
            let req = IoRequest::read(key, ptr);
            req_tx.send(req).unwrap();
            assert!(res_rx.recv().unwrap().result.is_ok());
            assert_eq!(data, key as u64);
        }

        drop(req_tx);
        storage_thread.join().unwrap();
    }
}
