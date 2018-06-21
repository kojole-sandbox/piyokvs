use std::sync::{Mutex, MutexGuard};

use entry::{Entry, State};

pub trait Cache<K, V> {
    fn lock(&self, key: K) -> MutexGuard<Entry<K, V>>;
}

pub struct SingleCache<K, V> {
    entry: Mutex<Entry<K, V>>,
}

impl<K, V> SingleCache<K, V>
where
    K: Default,
    V: Default,
{
    pub fn new() -> SingleCache<K, V> {
        SingleCache {
            entry: Mutex::new(Default::default()),
        }
    }
}

impl<K, V> Cache<K, V> for SingleCache<K, V>
where
    K: Copy + PartialEq,
{
    fn lock(&self, key: K) -> MutexGuard<Entry<K, V>> {
        let mut entry = self.entry.lock().unwrap();
        if key != entry.key {
            match entry.state {
                State::Fresh => entry.state = State::Unloaded,
                State::Dirty => entry.state = State::Stale(entry.key),
                State::Stale(_) => panic!("stale entry must be evicted"),
                _ => {}
            }
            entry.key = key;
        }
        entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_cache_state_changes() {
        let cache: SingleCache<i32, i32> = SingleCache::new();

        {
            let mut guard = cache.lock(1);
            assert_eq!(guard.state, State::Unloaded);
            guard.state = State::Fresh;
        }
        assert_eq!(cache.lock(1).state, State::Fresh);

        {
            let mut guard = cache.lock(2);
            assert_eq!(guard.state, State::Unloaded);
            guard.state = State::Dirty;
        }
        assert_eq!(cache.lock(2).state, State::Dirty);

        assert_eq!(cache.lock(3).state, State::Stale(2));
    }
}
