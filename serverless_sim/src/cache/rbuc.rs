use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};

use crate::fn_dag::FnId;

use super::InstanceCachePolicy;

struct CacheEntry {
    hit_count: u64,
    last_touch_tick: u64,
}

struct GlobalBudgetController {
    alpha: f32,
    period_ops: u64,
    op_in_period: u64,
    budgets: HashMap<FnId, usize>,
    max_replica_seen: HashMap<FnId, usize>,
    cold_inserts: HashMap<FnId, usize>,
    replicas: HashMap<FnId, HashSet<u64>>,
}

impl GlobalBudgetController {
    fn new() -> Self {
        Self {
            alpha: 0.3,
            period_ops: 200,
            op_in_period: 0,
            budgets: HashMap::new(),
            max_replica_seen: HashMap::new(),
            cold_inserts: HashMap::new(),
            replicas: HashMap::new(),
        }
    }

    fn step_period(&mut self) {
        self.op_in_period += 1;
        if self.op_in_period >= self.period_ops {
            self.refresh();
        }
    }

    fn observe_replica_count(&mut self, fnid: FnId) {
        let cur = self.replica_count(fnid);
        self.max_replica_seen
            .entry(fnid)
            .and_modify(|v| *v = (*v).max(cur))
            .or_insert(cur);
    }

    fn refresh(&mut self) {
        let mut fnids = HashSet::new();
        fnids.extend(self.budgets.keys().copied());
        fnids.extend(self.max_replica_seen.keys().copied());
        fnids.extend(self.cold_inserts.keys().copied());
        fnids.extend(self.replicas.keys().copied());

        for fnid in fnids {
            let run_pressure = self.max_replica_seen.remove(&fnid).unwrap_or(0);
            let cold_pressure = self.cold_inserts.remove(&fnid).unwrap_or(0);
            let demand = run_pressure + cold_pressure;
            let prev_budget = self.budgets.get(&fnid).copied().unwrap_or(1) as f32;
            let budget = (self.alpha * (demand as f32) + (1.0 - self.alpha) * prev_budget).ceil()
                as usize;
            self.budgets.insert(fnid, budget.max(1));
        }

        self.op_in_period = 0;
    }

    fn record_hit(&mut self, fnid: FnId) {
        self.observe_replica_count(fnid);
        self.step_period();
    }

    fn record_insert(&mut self, fnid: FnId, node_token: u64) {
        let inserted = self.replicas.entry(fnid).or_default().insert(node_token);
        if inserted {
            self.cold_inserts
                .entry(fnid)
                .and_modify(|v| *v += 1)
                .or_insert(1);
        }
        self.observe_replica_count(fnid);
        self.step_period();
    }

    fn record_remove(&mut self, fnid: FnId, node_token: u64) {
        if let Some(nodes) = self.replicas.get_mut(&fnid) {
            nodes.remove(&node_token);
            if nodes.is_empty() {
                self.replicas.remove(&fnid);
            }
        }
        self.observe_replica_count(fnid);
        self.step_period();
    }

    fn budget(&self, fnid: FnId) -> usize {
        self.budgets.get(&fnid).copied().unwrap_or(1)
    }

    fn replica_count(&self, fnid: FnId) -> usize {
        self.replicas.get(&fnid).map(|v| v.len()).unwrap_or(0)
    }

    fn delta(&self, fnid: FnId) -> isize {
        self.budget(fnid) as isize - self.replica_count(fnid) as isize
    }
}

fn global_controller() -> &'static Mutex<GlobalBudgetController> {
    static CTRL: OnceLock<Mutex<GlobalBudgetController>> = OnceLock::new();
    CTRL.get_or_init(|| Mutex::new(GlobalBudgetController::new()))
}

fn next_node_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, AtomicOrdering::Relaxed)
}

pub fn global_budget(fnid: FnId) -> usize {
    let ctrl = global_controller().lock().unwrap();
    ctrl.budget(fnid)
}

pub fn global_replica_delta(fnid: FnId) -> isize {
    let ctrl = global_controller().lock().unwrap();
    ctrl.delta(fnid)
}

pub struct RBUCCache {
    capacity: usize,
    node_token: u64,
    entries: HashMap<FnId, CacheEntry>,
    tick: u64,
    ttl_ticks: u64,
}

impl RBUCCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            node_token: next_node_token(),
            entries: HashMap::new(),
            tick: 0,
            ttl_ticks: 120,
        }
    }

    fn next_tick(&mut self) {
        self.tick += 1;
    }

    fn is_expired(&self, entry: &CacheEntry) -> bool {
        self.tick.saturating_sub(entry.last_touch_tick) >= self.ttl_ticks
    }

    fn remove_local(&mut self, fnid: FnId) -> bool {
        if self.entries.remove(&fnid).is_some() {
            let mut ctrl = global_controller().lock().unwrap();
            ctrl.record_remove(fnid, self.node_token);
            return true;
        }
        false
    }

    fn select_victim(
        &self,
        mut can_be_evict: impl FnMut(&FnId) -> bool,
        surplus_only: bool,
    ) -> Option<FnId> {
        let ctrl = global_controller().lock().unwrap();
        self.entries
            .iter()
            .filter(|(fnid, _)| can_be_evict(fnid))
            .filter(|(fnid, _)| !surplus_only || ctrl.delta(**fnid) < 0)
            .min_by(|(fnid_a, entry_a), (fnid_b, entry_b)| {
                let delta_a = ctrl.delta(**fnid_a);
                let delta_b = ctrl.delta(**fnid_b);
                let expired_a = self.is_expired(entry_a);
                let expired_b = self.is_expired(entry_b);

                (!expired_a)
                    .cmp(&(!expired_b))
                    .then_with(|| delta_a.cmp(&delta_b))
                    .then_with(|| {
                        let ua = entry_a.hit_count as f32;
                        let ub = entry_b.hit_count as f32;
                        ua.partial_cmp(&ub).unwrap_or(Ordering::Equal)
                    })
                    .then_with(|| entry_a.last_touch_tick.cmp(&entry_b.last_touch_tick))
                    .then_with(|| fnid_a.cmp(fnid_b))
            })
            .map(|(fnid, _)| *fnid)
    }
}

impl InstanceCachePolicy<FnId> for RBUCCache {
    fn get(&mut self, key: FnId) -> Option<FnId> {
        self.next_tick();
        {
            let mut ctrl = global_controller().lock().unwrap();
            ctrl.record_hit(key);
        }

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

        if let Some(entry) = self.entries.get_mut(&key) {
            {
                let mut ctrl = global_controller().lock().unwrap();
                ctrl.record_hit(key);
            }
            entry.hit_count += 1;
            entry.last_touch_tick = self.tick;
            return (None, true);
        }

        let mut evicted = None;
        while self.entries.len() >= self.capacity {
            let victim = self
                .select_victim(|fnid| can_be_evict(fnid), true)
                .or_else(|| self.select_victim(|fnid| can_be_evict(fnid), false));
            let Some(victim) = victim else {
                return (None, false);
            };
            if self.remove_local(victim) {
                evicted = Some(victim);
            } else {
                return (None, false);
            }
        }

        self.entries.insert(
            key,
            CacheEntry {
                hit_count: 0,
                last_touch_tick: self.tick,
            },
        );
        let mut ctrl = global_controller().lock().unwrap();
        ctrl.record_insert(key, self.node_token);

        (evicted, true)
    }

    fn remove_all(&mut self, key: &FnId) -> bool {
        self.remove_local(*key)
    }
}

unsafe impl Send for RBUCCache {}
