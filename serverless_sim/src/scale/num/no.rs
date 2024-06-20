use crate::{
    mechanism::SimEnvObserve, scale::up_exec::no::NoScaleUpExec,
};

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
        _env: &SimEnvObserve,
        _fnid: crate::fn_dag::FnId,
        _action: &crate::actions::ESActionWrapper,
    ) -> usize {
        return 0;
    }
}
