use crate::{fn_dag::FnId, scale::up_exec::no::NoPreLoader, sim_env::SimEnv};

use super::ScaleNum;

pub struct NoScaleNum {
    preloader: NoPreLoader,
}

impl NoScaleNum {
    pub fn new() -> Self {
        NoScaleNum {
            preloader: NoPreLoader {},
        }
    }
}

impl ScaleNum for NoScaleNum {
    fn scale_for_fn(
        &mut self,
        env: &crate::sim_env::SimEnv,
        fnid: crate::fn_dag::FnId,
        metric: &crate::algos::ContainerMetric,
        action: &crate::actions::ESActionWrapper,
    ) -> (f32, bool) {
        return (0.0, false);
    }

    fn fn_available_count(&self, fnid: FnId, env: &SimEnv) -> usize {
        env.node_cnt()
    }
}
