use crate::{fn_dag::FnId, scale::up_exec::no::NoScaleUpExec, sim_env::SimEnv};

use super::ScaleNum;

pub struct NoScaleNum {
    preloader: NoScaleUpExec,
}

impl NoScaleNum {
    pub fn new() -> Self {
        NoScaleNum {
            preloader: NoScaleUpExec {},
        }
    }
}

impl ScaleNum for NoScaleNum {
    fn scale_for_fn(
        &mut self,
        env: &crate::sim_env::SimEnv,
        fnid: crate::fn_dag::FnId,
        action: &crate::actions::ESActionWrapper,
    ) -> usize {
        return 0;
    }
}
