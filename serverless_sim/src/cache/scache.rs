use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::fn_dag::FnId;

use super::InstanceCachePolicy;

#[derive(Clone, Copy)]
struct FnProfile {
    cold_start_time: f32,
    size: f32,
}

struct CacheEntry {
    priority: f32,
    last_touch_tick: u64,
}

fn profile_registry() -> &'static Mutex<HashMap<FnId, FnProfile>> {
    static REGISTRY: OnceLock<Mutex<HashMap<FnId, FnProfile>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register_fn_profile(fnid: FnId, cold_start_time: f32, size: f32) {
    let mut registry = profile_registry().lock().unwrap();
    registry.insert(
        fnid,
        FnProfile {
            cold_start_time: cold_start_time.max(1.0),
            size: size.max(1.0),
        },
    );
}

fn fn_profile(fnid: FnId) -> FnProfile {
    let registry = profile_registry().lock().unwrap();
    registry.get(&fnid).copied().unwrap_or(FnProfile {
        cold_start_time: 1.0,
        size: 1.0,
    })
}

pub struct SCache {
    capacity: usize,
    clock: f32,
    tick: u64,
    interval_ops: u64,
    interval_len: u64,
    interval_freq: HashMap<FnId, u64>,
    entries: HashMap<FnId, CacheEntry>,
}

impl SCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            clock: 0.0,
            tick: 0,
            interval_ops: 0,
            interval_len: 200,
            interval_freq: HashMap::new(),
            entries: HashMap::new(),
        }
    }

    fn next_tick(&mut self) {
        self.tick += 1;
        self.interval_ops += 1;
        if self.interval_ops >= self.interval_len {
            self.interval_ops = 0;
            self.interval_freq.clear();
        }
    }

    fn compute_priority(&self, fnid: FnId, freq: u64) -> f32 {
        let profile = fn_profile(fnid);
        self.clock + (freq as f32) * profile.cold_start_time / profile.size
    }

    fn touch_entry(&mut self, fnid: FnId) {
        let freq = {
            let freq = self
                .interval_freq
                .entry(fnid)
                .and_modify(|v| *v += 1)
                .or_insert(1);
            *freq
        };
        let priority = self.compute_priority(fnid, freq);
        self.entries
            .entry(fnid)
            .and_modify(|entry| {
                entry.priority = priority;
                entry.last_touch_tick = self.tick;
            })
            .or_insert(CacheEntry {
                priority,
                last_touch_tick: self.tick,
            });
    }

    fn evict_one(&mut self, mut can_be_evict: impl FnMut(&FnId) -> bool) -> Option<FnId> {
        let victim = self
            .entries
            .iter()
            .filter(|(fnid, _)| can_be_evict(fnid))
            .min_by(|(fnid_a, entry_a), (fnid_b, entry_b)| {
                entry_a
                    .priority
                    .partial_cmp(&entry_b.priority)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| entry_a.last_touch_tick.cmp(&entry_b.last_touch_tick))
                    .then_with(|| fnid_a.cmp(fnid_b))
            })
            .map(|(fnid, _)| *fnid);

        if let Some(victim) = victim {
            let removed = self.entries.remove(&victim).unwrap();
            self.clock = self.clock.max(removed.priority);
            self.interval_freq.remove(&victim);
            return Some(victim);
        }

        None
    }
}

impl InstanceCachePolicy<FnId> for SCache {
    fn get(&mut self, key: FnId) -> Option<FnId> {
        self.next_tick();
        if self.entries.contains_key(&key) {
            self.touch_entry(key);
            return Some(key);
        }
        None
    }

    fn put(
        &mut self,
        key: FnId,
        mut can_be_evict: Box<dyn FnMut(&FnId) -> bool>,
    ) -> (Option<FnId>, bool) {
        self.next_tick();

        if self.entries.contains_key(&key) {
            self.touch_entry(key);
            return (None, true);
        }

        let mut evicted = None;
        while self.entries.len() >= self.capacity {
            let Some(victim) = self.evict_one(|fnid| can_be_evict(fnid)) else {
                return (None, false);
            };
            evicted = Some(victim);
        }

        self.touch_entry(key);
        (evicted, true)
    }

    fn remove_all(&mut self, key: &FnId) -> bool {
        let removed = self.entries.remove(key).is_some();
        if removed {
            self.interval_freq.remove(key);
        }
        removed
    }
}

unsafe impl Send for SCache {}
