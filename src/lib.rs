#![feature(integer_atomics)]

#[macro_use(select_loop)]
extern crate crossbeam_channel;
extern crate rand;

pub mod buffer;
pub mod cache;
pub mod client;
pub mod ptr;
pub mod storage;
