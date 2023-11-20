use crate::{fn_dag::FnId, sim_env::SimEnv};

pub mod least_task;
pub mod no;
pub trait ScalePreLoader {
    fn pre_load(&self, target_cnt: usize, fnid: FnId, env: &SimEnv);
}
