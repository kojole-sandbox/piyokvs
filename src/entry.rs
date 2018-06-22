use std::mem::uninitialized;

pub trait Lazy {
    fn init(&mut self);
}

#[derive(Debug, PartialEq)]
pub enum State<K> {
    Uninitialized,
    Unloaded,
    Fresh,
    Dirty,
    Stale(K),
}

#[derive(Debug)]
pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
    pub state: State<K>,
}

impl<K, V> Entry<K, V>
where
    V: Default,
{
    pub fn new(key: K) -> Entry<K, V> {
        Entry {
            key,
            value: Default::default(),
            state: State::Unloaded,
        }
    }
}

impl<K, V> Default for Entry<K, V>
where
    K: Default,
{
    fn default() -> Entry<K, V> {
        Entry {
            key: Default::default(),
            value: unsafe { uninitialized() },
            state: State::Uninitialized,
        }
    }
}

impl<K, V> Lazy for Entry<K, V>
where
    V: Default,
{
    fn init(&mut self) {
        self.value = Default::default();
        self.state = State::Unloaded;
    }
}
