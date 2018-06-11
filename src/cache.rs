use std::cmp::Eq;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
    pub dirty: bool,
}

impl<K, V> Entry<K, V>
where
    V: Default,
{
    pub fn new(key: K) -> Entry<K, V> {
        Entry {
            key,
            value: Default::default(),
            dirty: false,
        }
    }
}

struct CacheEntry<K, V> {
    inner: Entry<K, V>,
    next: Option<usize>,
    prev: Option<usize>,
    evicting: bool,
}

impl<K, V> CacheEntry<K, V>
where
    V: Default,
{
    fn new(key: K) -> CacheEntry<K, V> {
        CacheEntry {
            inner: Entry::new(key),
            next: None,
            prev: None,
            evicting: false,
        }
    }
}

pub struct Cache<K, V> {
    capacity: usize,
    threshold: usize,
    head: Option<usize>,
    tail: Option<usize>,
    keys: HashMap<K, usize>,
    entries: Vec<CacheEntry<K, V>>,
    evicted_indices: VecDeque<usize>,
}

impl<K, V> Cache<K, V>
where
    K: Copy + Eq + Hash,
    V: Default,
{
    pub fn new(capacity: usize, threshold: usize) -> Cache<K, V> {
        assert!(capacity >= 2);
        assert!(threshold >= 1);
        assert!(capacity > threshold);

        Cache {
            capacity,
            threshold,
            head: None,
            tail: None,
            keys: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            evicted_indices: VecDeque::new(),
        }
    }

    /// Return new entry of the id and one to be evicted
    pub fn get_with_evicting(&mut self, key: K) -> (&Entry<K, V>, Option<&Entry<K, V>>) {
        assert!(!self.keys.contains_key(&key));

        let new_i = self.entry_new(key);
        self.keys.insert(key, new_i);
        self.push_front(new_i);

        let evicting_i =
            if self.entries.len() == self.capacity && self.evicted_indices.len() < self.threshold {
                self.reclaim()
            } else {
                None
            };

        if let Some(evicting_i) = evicting_i {
            (
                &self.entries[new_i].inner,
                Some(&self.entries[evicting_i].inner),
            )
        } else {
            (&self.entries[new_i].inner, None)
        }
    }

    /// Return entry of the id without touching if exists
    pub fn get_untouched(&mut self, key: K) -> Option<&Entry<K, V>> {
        if let Some(i) = self.keys.get(&key) {
            Some(&self.entries[*i].inner)
        } else {
            None
        }
    }

    pub fn touch(&mut self, key: K) {
        let i = *self.keys.get(&key).unwrap();
        if i != self.head.unwrap() {
            // Remove evicting flag
            self.entries[i].evicting = false;

            // Move to head
            self.unlink(i);
            self.push_front(i);
        }
    }

    /// Return next evicting entry
    pub fn evicting_new(&mut self) -> &Entry<K, V> {
        if let Some(i) = self.reclaim() {
            &self.entries[i].inner
        } else {
            panic!("cannot evict");
        }
    }

    /// Confirm evicting
    pub fn evicting_done(&mut self, key: K) {
        let i = self.keys.remove(&key).unwrap();
        self.unlink(i);
        self.entries[i].evicting = false;
        self.evicted_indices.push_back(i);
    }

    fn entry_new(&mut self, key: K) -> usize {
        let len = self.entries.len();
        if len < self.capacity {
            // Add new entry
            self.entries.push(CacheEntry::new(key));
            return len;
        } else if len == self.capacity {
            // Reuse evicted entry
            if let Some(i) = self.evicted_indices.pop_front() {
                self.entries[i].inner = Entry::new(key);
                return i;
            }
        }
        panic!("no space");
    }

    fn unlink(&mut self, i: usize) {
        let next = self.entries[i].next;
        let prev = self.entries[i].prev;

        if i == self.head.unwrap() {
            self.head = next;
        } else {
            self.entries[prev.unwrap()].next = next;
        }

        if i == self.tail.unwrap() {
            self.tail = prev;
        } else {
            self.entries[next.unwrap()].prev = prev;
        }
    }

    fn push_front(&mut self, i: usize) {
        let new_head = Some(i);
        if self.tail.is_none() {
            self.tail = new_head;
        } else {
            self.entries[i].next = self.head;
            self.entries[i].prev = None;
            if let Some(head_i) = self.head {
                self.entries[head_i].prev = new_head;
            }
        }
        self.head = new_head;
    }

    /// Return next evicting entry index while marking the entry as evicting
    fn reclaim(&mut self) -> Option<usize> {
        let mut pos = self.tail;
        while let Some(i) = pos {
            if !self.entries[i].evicting {
                self.entries[i].evicting = true;
                return Some(i);
            }
            pos = self.entries[i].prev;
        }
        None
    }
}

pub struct LruIterator<'a, K: 'a, V: 'a> {
    cache: &'a Cache<K, V>,
    pos: Option<usize>,
}

impl<'a, K, V> LruIterator<'a, K, V> {
    pub fn new(cache: &'a Cache<K, V>) -> LruIterator<'a, K, V> {
        LruIterator {
            cache,
            pos: cache.tail,
        }
    }
}

impl<'a, K, V> Iterator for LruIterator<'a, K, V> {
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(i) = self.pos {
            self.pos = self.cache.entries[i].prev;
            Some(&self.cache.entries[i].inner)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_cache() -> Cache<usize, bool> {
        Cache::new(4, 1)
    }

    fn lru_keys<K: Copy, V>(cache: &Cache<K, V>) -> Vec<K> {
        LruIterator::new(cache).map(|entry| entry.key).collect()
    }

    #[test]
    fn evict() {
        let mut cache = default_cache();

        let input_keys = &[0, 1, 2, 3, 4, 5, 6];
        let mut evicted_keys = Vec::new();

        for key in input_keys {
            let evicting_key = match cache.get_with_evicting(*key) {
                (_, Some(evicting)) => Some(evicting.key),
                _ => None,
            };

            if let Some(key) = evicting_key {
                cache.evicting_done(key);
                evicted_keys.push(key);
            }
        }

        assert_eq!(evicted_keys, [0, 1, 2, 3]);
        assert_eq!(lru_keys(&cache), [4, 5, 6]);
    }

    #[test]
    fn touch_head() {
        let mut cache = default_cache();

        let input_keys = &[0, 1, 2, 3, 3, 4, 5, 6];
        let mut evicted_keys = Vec::new();

        for key in input_keys {
            if cache.get_untouched(*key).is_some() {
                cache.touch(*key);
                continue;
            }

            let evicting_key = match cache.get_with_evicting(*key) {
                (_, Some(evicting)) => Some(evicting.key),
                _ => None,
            };

            if let Some(key) = evicting_key {
                cache.evicting_done(key);
                evicted_keys.push(key);
            }
        }

        assert_eq!(evicted_keys, [0, 1, 2, 3]);
        assert_eq!(lru_keys(&cache), [4, 5, 6]);
    }

    #[test]
    fn touch_inbetween() {
        let mut cache = default_cache();

        let input_keys = &[0, 1, 2, 3, 2, 4, 5, 6];
        let mut evicted_keys = Vec::new();

        for key in input_keys {
            if cache.get_untouched(*key).is_some() {
                cache.touch(*key);
                continue;
            }

            let evicting_key = match cache.get_with_evicting(*key) {
                (_, Some(evicting)) => Some(evicting.key),
                _ => None,
            };

            if let Some(key) = evicting_key {
                cache.evicting_done(key);
                evicted_keys.push(key);
            }
        }

        assert_eq!(evicted_keys, [0, 1, 3, 2]);
        assert_eq!(lru_keys(&cache), [4, 5, 6]);
    }

    #[test]
    fn touch_tail() {
        let mut cache = default_cache();

        let input_keys = &[0, 1, 2, 3, 1, 4, 5, 6];
        let mut evicted_keys = Vec::new();

        for key in input_keys {
            if cache.get_untouched(*key).is_some() {
                cache.touch(*key);
                continue;
            }

            let evicting_key = match cache.get_with_evicting(*key) {
                (_, Some(evicting)) => Some(evicting.key),
                _ => None,
            };

            if let Some(key) = evicting_key {
                cache.evicting_done(key);
                evicted_keys.push(key);
            }
        }

        assert_eq!(evicted_keys, [0, 2, 3, 1]);
        assert_eq!(lru_keys(&cache), [4, 5, 6]);
    }

    #[test]
    fn touch_evicting() {
        let mut cache = default_cache();

        cache.get_with_evicting(0);
        cache.get_with_evicting(1);
        cache.get_with_evicting(2);

        let evicting_key = {
            let (_, evicting) = cache.get_with_evicting(3);
            evicting.unwrap().key
        };
        assert_eq!(evicting_key, 0);

        let i = *cache.keys.get(&evicting_key).unwrap();
        assert!(cache.entries[i].evicting);

        cache.touch(evicting_key);
        assert!(!cache.entries[i].evicting);

        let key = cache.evicting_new().key;
        assert_eq!(key, 1);
        cache.evicting_done(key);

        let evicting_key = {
            let (_, evicting) = cache.get_with_evicting(4);
            evicting.unwrap().key
        };
        assert_eq!(evicting_key, 2);
        cache.evicting_done(evicting_key);

        assert_eq!(lru_keys(&cache), [3, 0, 4]);
    }
}
