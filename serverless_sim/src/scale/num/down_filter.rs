use std::collections::{HashMap, VecDeque};

use crate::fn_dag::FnId;

pub trait ScaleFilter: Send {
    fn filter_desired(&mut self, fnid: FnId, desired: usize, current: usize) -> usize;
}

pub struct CarefulScaleDownFilter {
    history_desired_container_cnt: HashMap<FnId, VecDeque<usize>>,
}

impl CarefulScaleDownFilter {
    pub fn new() -> Self {
        CarefulScaleDownFilter {
            history_desired_container_cnt: HashMap::new(),
        }
    }
    fn smaller_than_history(&self, fnid: FnId, desired_container_cnt: usize) -> bool {
        if let Some(history) = self.history_desired_container_cnt.get(&fnid) {
            history.iter().all(|&cnt| cnt <= desired_container_cnt)
        } else {
            false
        }
    }
    fn record_history(&mut self, fnid: FnId, desired: usize) {
        let history = self
            .history_desired_container_cnt
            .entry(fnid)
            .or_insert_with(|| VecDeque::new());
        history.push_back(desired);
        if history.len() > 100 {
            history.pop_front();
        }
    }
}

impl ScaleFilter for CarefulScaleDownFilter {
    fn filter_desired(&mut self, fnid: FnId, desired: usize, current: usize) -> usize {
        // log::info!("do careful scale down filter");
        if desired < current {
            let ret = if self.smaller_than_history(fnid, desired) {
                desired
            } else {
                current
            };
            self.record_history(fnid, desired);
            ret
        } else {
            desired
        }
    }
}
