#[derive(Debug, PartialEq)]
pub enum State<K> {
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
    V: Default,
{
    fn default() -> Entry<K, V> {
        Entry {
            key: Default::default(),
            value: Default::default(),
            state: State::Unloaded,
        }
    }
}
