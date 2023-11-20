use super::ScalePreLoader;
use crate::{fn_dag::FnId, sim_env::SimEnv};

pub struct NoPreLoader;

impl ScalePreLoader for NoPreLoader {
    fn pre_load(&self, target_cnt: usize, fnid: FnId, env: &SimEnv) {}
}
