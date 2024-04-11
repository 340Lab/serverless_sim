use crate::{fn_dag::FnId, sim_env::SimEnv};

pub mod least_task;
pub mod no;
pub trait ScaleUpExec: Send {
    fn exec_scale_up(&self, target_cnt: usize, fnid: FnId, env: &SimEnv);
}
