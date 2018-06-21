#![feature(integer_atomics)]

#[macro_use]
extern crate crossbeam_channel;
extern crate rand;

pub mod buffer;
pub mod cache;
pub mod client;
pub mod ptr;
pub mod entry;
pub mod storage;
