use super::ScaleUpExec;
use crate::{fn_dag::FnId, mechanism::UpCmd, sim_env::SimEnv};

pub struct NoScaleUpExec;

impl ScaleUpExec for NoScaleUpExec {
    fn exec_scale_up(&self, target_cnt: usize, fnid: FnId, env: &SimEnv) -> Vec<UpCmd> {
        vec![]
    }
}
