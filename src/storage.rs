use std::cmp;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::mem::{size_of, transmute};
use std::slice;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use buffer::DataPtr;

const N_THREADS: usize = 4;

pub enum Io {
    Read(u32),
    Write(u32),
}

pub struct IoRequest {
    io: Io,
    buf: DataPtr,
    res: Sender<io::Result<Io>>,
}

impl IoRequest {
    pub fn new(io: Io, buf: DataPtr, res: Sender<io::Result<Io>>) -> IoRequest {
        IoRequest { io, buf, res }
    }
}

pub struct Storage {
    path: String,
    io_rx: Receiver<IoRequest>,
}

impl Storage {
    pub fn new(path: String, n_data: u32, io_rx: Receiver<IoRequest>) -> io::Result<Storage> {
        assert!(n_data > 0);

        {
            let mut file = File::create(&path)?;
            write_zeros(&mut file, (n_data as u64) * (size_of::<u64>() as u64))?;
        }

        Ok(Storage { path, io_rx })
    }

    pub fn start(&mut self) {
        let mut threads = Vec::new();

        for _ in 0..N_THREADS {
            let path = self.path.clone();
            let io_rx = self.io_rx.clone();

            let t = thread::spawn(move || {
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .unwrap();

                for mut req in io_rx {
                    let result = match req.io {
                        Io::Read(id) => {
                            let pos = id as u64 * size_of::<u64>() as u64;
                            let dst = unsafe { req.buf.0.as_mut() } as *mut u64;
                            read_at(&mut file, pos, dst)
                        }
                        Io::Write(id) => {
                            let pos = id as u64 * size_of::<u64>() as u64;
                            let src = unsafe { req.buf.0.as_ref() } as *const u64;
                            write_at(&mut file, pos, src)
                        }
                    };

                    req.res.send(result.and(Ok(req.io))).unwrap();
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
    use std::ptr::NonNull;

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
    fn test_storage() {
        let (io_tx, io_rx) = bounded(0);

        let storage_thread = thread::spawn(move || {
            let mut storage = Storage::new("1.db".to_string(), 10000, io_rx).unwrap();
            storage.start();
        });

        let mut writers = Vec::new();
        for i in 0..100 {
            let io_tx = io_tx.clone();

            let t = thread::spawn(move || {
                let (res_tx, res_rx) = bounded(0);

                for j in 0..100 {
                    let mut data: u64 = i * 100 + j;
                    let io = Io::Write(data as u32);
                    let buf = DataPtr(NonNull::new(&mut data as *mut u64).unwrap());
                    let req = IoRequest::new(io, buf, res_tx.clone());
                    io_tx.send(req).unwrap();
                    res_rx.recv().unwrap().unwrap();
                }
            });

            writers.push(t);
        }

        for writer in writers {
            writer.join().unwrap();
        }

        let (res_tx, res_rx) = bounded(0);
        for id in 0u64..10000 {
            let mut data: u64 = u64::max_value();
            let io = Io::Read(id as u32);
            let buf = DataPtr(NonNull::new(&mut data as *mut u64).unwrap());
            let req = IoRequest::new(io, buf, res_tx.clone());
            io_tx.send(req).unwrap();
            res_rx.recv().unwrap().unwrap();
            assert_eq!(data, id);
        }

        drop(io_tx);
        storage_thread.join().unwrap();
    }
}
