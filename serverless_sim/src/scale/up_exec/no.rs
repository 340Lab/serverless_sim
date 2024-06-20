use super::ScaleUpExec;
use crate::{
    fn_dag::FnId,
    mechanism::{SimEnvObserve, UpCmd},
};

pub struct NoScaleUpExec;

impl ScaleUpExec for NoScaleUpExec {
    fn exec_scale_up(&self, _target_cnt: usize, _fnid: FnId, _env: &SimEnvObserve) -> Vec<UpCmd> {
        vec![]
    }
}
