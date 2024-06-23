use crate::{
    fn_dag::{EnvFnExt, FnId},
    node::NodeId,
    request::{ReqId, Request},
    sim_env::SimEnv,
};

impl SimEnv {
    pub fn on_task_scheduled(&self, req: &mut Request, fnid: FnId, nodeid: NodeId) {
        // ReqFnMetric - sche_time
        assert!(req
            .fn_metric
            .get_mut(&fnid)
            .unwrap()
            .sche_time
            .replace(self.current_frame())
            .is_none());
    }
}
