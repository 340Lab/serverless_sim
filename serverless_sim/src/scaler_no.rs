use crate::{es::ESScaler, fn_dag::FnId, sim_env::SimEnv};

pub struct ScalerNo;

impl ScalerNo {
    pub fn new() -> Self {
        ScalerNo {}
    }
}

impl ESScaler for ScalerNo {
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
