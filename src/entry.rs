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

impl<K, V> Default for Entry<K, V>
where
    K: Default,
    V: Default,
{
    fn default() -> Entry<K, V> {
        Entry {
            key: Default::default(),
            value: Default::default(),
            state: State::Uninitialized,
        }
    }
}

impl<K, V> Lazy for Entry<K, V>
where
    V: Lazy,
{
    fn init(&mut self) {
        self.value.init();
        self.state = State::Unloaded;
    }
}
