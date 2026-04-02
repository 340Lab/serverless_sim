use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use crate::fn_dag::FnId;

use super::InstanceCachePolicy;

#[derive(Clone, Copy, PartialEq, Eq)]
enum CacheSpace {
    Protected,
    Temporary,
}

struct CacheEntry {
    space: CacheSpace,
    hit_count: u64,
    last_touch_tick: u64,
}

struct GlobalHotController {
    region_size: f32,
    period_ops: u64,
    op_in_period: u64,
    current_counts: HashMap<FnId, u64>,
    scores: HashMap<FnId, f32>,
    hot_set: HashSet<FnId>,
}

impl GlobalHotController {
    fn new() -> Self {
        Self {
            region_size: 0.5,
            period_ops: 200,
            op_in_period: 0,
            current_counts: HashMap::new(),
            scores: HashMap::new(),
            hot_set: HashSet::new(),
        }
    }

    fn record_access(&mut self, fnid: FnId) {
        self.current_counts
            .entry(fnid)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.op_in_period += 1;
        if self.op_in_period >= self.period_ops {
            self.refresh();
        }
    }

    fn refresh(&mut self) {
        let mut all_fn_ids: HashSet<FnId> = HashSet::new();
        all_fn_ids.extend(self.scores.keys().copied());
        all_fn_ids.extend(self.current_counts.keys().copied());

        for fnid in all_fn_ids {
            let current = self.current_counts.remove(&fnid).unwrap_or(0) as f32;
            let prev = self.scores.get(&fnid).copied().unwrap_or(0.0);
            let score = current + 0.5 * prev;
            if score > 0.00001 {
                self.scores.insert(fnid, score);
            } else {
                self.scores.remove(&fnid);
            }
        }

        self.op_in_period = 0;
        self.rebuild_hot_set();
    }

    fn rebuild_hot_set(&mut self) {
        let mut sorted = self
            .scores
            .iter()
            .map(|(fnid, score)| (*fnid, *score))
            .collect::<Vec<_>>();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        self.hot_set.clear();
        if sorted.is_empty() {
            return;
        }

        let total = sorted.iter().map(|(_, score)| *score).sum::<f32>();
        if total <= 0.00001 {
            return;
        }

        let mut accum = 0.0;
        for (fnid, score) in sorted {
            self.hot_set.insert(fnid);
            accum += score;
            if accum >= self.region_size * total {
                break;
            }
        }
    }

    fn is_hot(&self, fnid: FnId) -> bool {
        self.hot_set.contains(&fnid)
    }

    fn score(&self, fnid: FnId) -> f32 {
        self.scores.get(&fnid).copied().unwrap_or(0.0)
    }
}

fn global_controller() -> &'static Mutex<GlobalHotController> {
    static CTRL: OnceLock<Mutex<GlobalHotController>> = OnceLock::new();
    CTRL.get_or_init(|| Mutex::new(GlobalHotController::new()))
}

fn global_record_access(fnid: FnId) {
    let mut ctrl = global_controller().lock().unwrap();
    ctrl.record_access(fnid);
}

pub fn global_is_hot(fnid: FnId) -> bool {
    let ctrl = global_controller().lock().unwrap();
    ctrl.is_hot(fnid)
}

pub fn global_fn_score(fnid: FnId) -> f32 {
    let ctrl = global_controller().lock().unwrap();
    ctrl.score(fnid)
}

pub struct FlameCache {
    capacity: usize,
    entries: HashMap<FnId, CacheEntry>,
    tick: u64,
    ttl_ticks: u64,
}

impl FlameCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            tick: 0,
            // Flame keeps non-hot functions in temporary space with keep-alive.
            // We approximate the keep-alive window with operation ticks.
            ttl_ticks: 300,
        }
    }

    fn next_tick(&mut self) {
        self.tick += 1;
    }

    fn sync_spaces(&mut self) {
        for (fnid, entry) in self.entries.iter_mut() {
            entry.space = if global_is_hot(*fnid) {
                CacheSpace::Protected
            } else {
                CacheSpace::Temporary
            };
        }
    }

    fn is_expired_temporary(&self, entry: &CacheEntry) -> bool {
        entry.space == CacheSpace::Temporary
            && self.tick.saturating_sub(entry.last_touch_tick) >= self.ttl_ticks
    }

    fn evict_temporary(
        &mut self,
        mut can_be_evict: impl FnMut(&FnId) -> bool,
    ) -> Option<FnId> {
        let victim = self
            .entries
            .iter()
            .filter(|(fnid, entry)| {
                entry.space == CacheSpace::Temporary && can_be_evict(fnid)
            })
            .min_by(|(fnid_a, entry_a), (fnid_b, entry_b)| {
                let expired_a = self.is_expired_temporary(entry_a);
                let expired_b = self.is_expired_temporary(entry_b);

                (!expired_a)
                    .cmp(&(!expired_b))
                    .then_with(|| entry_a.hit_count.cmp(&entry_b.hit_count))
                    .then_with(|| entry_a.last_touch_tick.cmp(&entry_b.last_touch_tick))
                    .then_with(|| fnid_a.cmp(fnid_b))
            })
            .map(|(fnid, _)| *fnid);

        if let Some(victim) = victim {
            self.entries.remove(&victim);
            return Some(victim);
        }
        None
    }

    fn reclaim_protected(
        &mut self,
        mut can_be_evict: impl FnMut(&FnId) -> bool,
    ) -> Option<FnId> {
        let victim = self
            .entries
            .iter()
            .filter(|(fnid, entry)| {
                entry.space == CacheSpace::Protected && can_be_evict(fnid)
            })
            .min_by(|(fnid_a, entry_a), (fnid_b, entry_b)| {
                let age_a = self.tick.saturating_sub(entry_a.last_touch_tick).max(1) as f32;
                let age_b = self.tick.saturating_sub(entry_b.last_touch_tick).max(1) as f32;
                let reuse_a = (entry_a.hit_count as f32) / age_a;
                let reuse_b = (entry_b.hit_count as f32) / age_b;

                reuse_a
                    .partial_cmp(&reuse_b)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| entry_a.last_touch_tick.cmp(&entry_b.last_touch_tick))
                    .then_with(|| fnid_a.cmp(fnid_b))
            })
            .map(|(fnid, _)| *fnid);

        if let Some(victim) = victim {
            self.entries.remove(&victim);
            return Some(victim);
        }
        None
    }
}

impl InstanceCachePolicy<FnId> for FlameCache {
    fn get(&mut self, key: FnId) -> Option<FnId> {
        self.next_tick();
        global_record_access(key);
        self.sync_spaces();

        if let Some(entry) = self.entries.get_mut(&key) {
            entry.hit_count += 1;
            entry.last_touch_tick = self.tick;
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
        global_record_access(key);
        self.sync_spaces();

        if let Some(entry) = self.entries.get_mut(&key) {
            entry.hit_count += 1;
            entry.last_touch_tick = self.tick;
            return (None, true);
        }

        let target_space = if global_is_hot(key) {
            CacheSpace::Protected
        } else {
            CacheSpace::Temporary
        };

        let mut evicted = None;
        while self.entries.len() >= self.capacity {
            let victim = self.evict_temporary(|fnid| can_be_evict(fnid)).or_else(|| {
                if target_space == CacheSpace::Protected {
                    self.reclaim_protected(|fnid| can_be_evict(fnid))
                } else {
                    None
                }
            });

            let Some(victim) = victim else {
                return (None, false);
            };
            evicted = Some(victim);
        }

        self.entries.insert(
            key,
            CacheEntry {
                space: target_space,
                hit_count: 1,
                last_touch_tick: self.tick,
            },
        );

        (evicted, true)
    }

    fn remove_all(&mut self, key: &FnId) -> bool {
        self.entries.remove(key).is_some()
    }
}

unsafe impl Send for FlameCache {}
