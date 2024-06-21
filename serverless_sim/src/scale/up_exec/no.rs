use super::ScaleUpExec;
use crate::{
    fn_dag::FnId,
    mechanism::{SimEnvObserve, UpCmd},
    mechanism_thread::MechCmdDistributor,
};

pub struct NoScaleUpExec;

impl ScaleUpExec for NoScaleUpExec {
    fn exec_scale_up(
        &self,
        _target_cnt: usize,
        _fnid: FnId,
        _env: &SimEnvObserve,
        _cmd_distributor: &MechCmdDistributor,
    ) -> Vec<UpCmd> {
        vec![]
    }
}
