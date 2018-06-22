use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, MutexGuard};

use entry::{Entry, Lazy, State};

pub trait Cache<K, V> {
    fn lock(&self, key: K) -> MutexGuard<Entry<K, V>>;
    fn dirty_entries(&self) -> Vec<MutexGuard<Entry<K, V>>>;
}

fn prepare_entry<K, V>(entry: &mut MutexGuard<Entry<K, V>>, key: K)
where
    K: Copy + PartialEq,
    V: Lazy,
{
    match entry.state {
        State::Uninitialized => {
            entry.init();
            entry.key = key;
        }

        State::Unloaded => panic!("initialized entry must be loaded"),

        State::Fresh => {
            if entry.key != key {
                entry.state = State::Unloaded;
                entry.key = key;
            }
        }

        State::Dirty => {
            if entry.key != key {
                entry.state = State::Stale(entry.key);
                entry.key = key;
            }
        }

        State::Stale(_) => panic!("stale entry must be evicted"),
    }
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
    V: Lazy,
{
    fn lock(&self, key: K) -> MutexGuard<Entry<K, V>> {
        let mut entry = self.entry.lock().unwrap();
        prepare_entry(&mut entry, key);
        entry
    }

    fn dirty_entries(&self) -> Vec<MutexGuard<Entry<K, V>>> {
        let entry = self.entry.lock().unwrap();
        if entry.state == State::Dirty {
            vec![entry]
        } else {
            Vec::new()
        }
    }
}

pub struct LruCache<K, V> {
    entries: Vec<Mutex<Entry<K, V>>>,
    lane: Mutex<LruLane<K>>,
}

impl<K, V> LruCache<K, V>
where
    K: Copy + Default + Eq + Hash,
    V: Default,
{
    pub fn new(capacity: usize) -> LruCache<K, V> {
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(Default::default());
        }
        LruCache {
            entries,
            lane: Mutex::new(LruLane::new(capacity)),
        }
    }
}

impl<K, V> Cache<K, V> for LruCache<K, V>
where
    K: Copy + Eq + Hash + PartialEq,
    V: Lazy,
{
    fn lock(&self, key: K) -> MutexGuard<Entry<K, V>> {
        let (i, uninitialized) = self.lane.lock().unwrap().index(key);

        let mut entry = self.entries[i].lock().unwrap();
        if uninitialized {
            entry.state = State::Uninitialized;
        }
        prepare_entry(&mut entry, key);
        entry
    }

    fn dirty_entries(&self) -> Vec<MutexGuard<Entry<K, V>>> {
        let lane = self.lane.lock().unwrap();

        let mut entries = Vec::with_capacity(lane.entries.len());

        let iter = LruLaneIterator::new(&lane);
        for i in iter {
            let entry = self.entries[i].lock().unwrap();
            if entry.state == State::Dirty {
                entries.push(entry);
            }
        }

        entries
    }
}

struct LruLaneEntry<K> {
    key: K,
    next: Option<usize>,
    prev: Option<usize>,
}

impl<K> LruLaneEntry<K> {
    fn new(key: K) -> LruLaneEntry<K> {
        LruLaneEntry {
            key,
            next: None,
            prev: None,
        }
    }
}

struct LruLane<K> {
    capacity: usize,
    keys: HashMap<K, usize>,
    entries: Vec<LruLaneEntry<K>>,
    head: Option<usize>,
    tail: Option<usize>,
}

impl<K> LruLane<K>
where
    K: Copy + Eq + Hash,
{
    fn new(capacity: usize) -> LruLane<K> {
        LruLane {
            capacity,
            keys: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            head: None,
            tail: None,
        }
    }

    fn index(&mut self, key: K) -> (usize, bool) {
        if let Some(i) = self.keys.get(&key).map(|i| *i) {
            self.touch(i);
            return (i, false);
        }

        let mut uninitialized = false;

        let new_head_i = if self.entries.len() == self.capacity {
            let i = self.pop_back();
            self.keys.remove(&self.entries[i].key).unwrap();
            self.entries[i].key = key;
            i
        } else {
            uninitialized = true;
            self.entries.push(LruLaneEntry::new(key));
            self.entries.len() - 1
        };

        self.push_front(new_head_i);
        self.keys.insert(key, new_head_i);

        (new_head_i, uninitialized)
    }

    fn pop_back(&mut self) -> usize {
        let old_tail_i = self.tail.unwrap();
        self.tail = self.entries[old_tail_i].prev;
        old_tail_i
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

    fn touch(&mut self, i: usize) {
        if i != self.head.unwrap() {
            self.unlink(i);
            self.push_front(i);
        }
    }
}

struct LruLaneIterator<'a, K: 'a> {
    lru_lane: &'a LruLane<K>,
    pos: Option<usize>,
}

impl<'a, K> LruLaneIterator<'a, K> {
    fn new(lru_lane: &'a LruLane<K>) -> LruLaneIterator<'a, K> {
        LruLaneIterator {
            lru_lane,
            pos: lru_lane.tail,
        }
    }
}

impl<'a, K> Iterator for LruLaneIterator<'a, K> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if let Some(i) = self.pos {
            self.pos = self.lru_lane.entries[i].prev;
            Some(i)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Lazy for i32 {
        fn init(&mut self) {
            *self = 0;
        }
    }

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

    #[test]
    fn lru_cache_lane_evict() {
        let mut lane: LruLane<i32> = LruLane::new(3);

        let input_keys = [0, 1, 2, 3, 4, 5, 6];
        let expected = [
            (0, true),
            (1, true),
            (2, true),
            (0, false),
            (1, false),
            (2, false),
            (0, false),
        ];

        for i in 0..input_keys.len() {
            assert_eq!(lane.index(input_keys[i]), expected[i]);
        }

        let mut keys = lane.keys.keys().map(|key| *key).collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, [4, 5, 6]);

        let indices = LruLaneIterator::new(&lane).collect::<Vec<_>>();
        assert_eq!(indices, [1, 2, 0]);
    }

    #[test]
    fn lru_cache_lane_touch_head() {
        let mut lane: LruLane<i32> = LruLane::new(3);

        let input_keys = [0, 1, 2, 3, 3, 4, 5, 6];
        let expected = [
            (0, true),
            (1, true),
            (2, true),
            (0, false),
            (0, false),
            (1, false),
            (2, false),
            (0, false),
        ];

        for i in 0..input_keys.len() {
            assert_eq!(lane.index(input_keys[i]), expected[i]);
        }

        let mut keys = lane.keys.keys().map(|key| *key).collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, [4, 5, 6]);

        let indices = LruLaneIterator::new(&lane).collect::<Vec<_>>();
        assert_eq!(indices, [1, 2, 0]);
    }

    #[test]
    fn lru_cache_lane_touch_inbetween() {
        let mut lane: LruLane<i32> = LruLane::new(3);

        let input_keys = [0, 1, 2, 3, 2, 4, 5, 6];
        let expected = [
            (0, true),
            (1, true),
            (2, true),
            (0, false),
            (2, false),
            (1, false),
            (0, false),
            (2, false),
        ];

        for i in 0..input_keys.len() {
            assert_eq!(lane.index(input_keys[i]), expected[i]);
        }

        let mut keys = lane.keys.keys().map(|key| *key).collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, [4, 5, 6]);

        let indices = LruLaneIterator::new(&lane).collect::<Vec<_>>();
        assert_eq!(indices, [1, 0, 2]);
    }

    #[test]
    fn lru_cahce_lane_touch_tail() {
        let mut lane: LruLane<i32> = LruLane::new(3);

        let input_keys = [0, 1, 2, 3, 1, 4, 5, 6];
        let expected = [
            (0, true),
            (1, true),
            (2, true),
            (0, false),
            (1, false),
            (2, false),
            (0, false),
            (1, false),
        ];

        for i in 0..input_keys.len() {
            assert_eq!(lane.index(input_keys[i]), expected[i]);
        }

        let mut keys = lane.keys.keys().map(|key| *key).collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, [4, 5, 6]);

        let indices = LruLaneIterator::new(&lane).collect::<Vec<_>>();
        assert_eq!(indices, [2, 0, 1]);
    }
}
