use std::ptr::NonNull;

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

impl<K, V> Entry<K, V> {
    pub fn as_ptr(&mut self) -> NonNull<V> {
        match self.state {
            State::Unloaded | State::Dirty | State::Stale(_) => {}
            _ => panic!("invalid state"),
        }
        self.state = State::Fresh;
        unsafe { NonNull::new_unchecked(&mut self.value) }
    }
}

impl<K, V> AsRef<V> for Entry<K, V> {
    fn as_ref(&self) -> &V {
        match self.state {
            State::Fresh | State::Dirty => {}
            _ => panic!("invalid state"),
        }
        &self.value
    }
}

impl<K, V> AsMut<V> for Entry<K, V> {
    fn as_mut(&mut self) -> &mut V {
        match self.state {
            State::Fresh | State::Dirty => {}
            _ => panic!("invalid state"),
        }
        self.state = State::Dirty;
        &mut self.value
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
