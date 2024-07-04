use crate::{
    fn_dag::{FnId},
    node::NodeId,
    request::{Request},
    sim_env::SimEnv,
};

impl SimEnv {
    pub fn on_task_scheduled(&self, req: &mut Request, fnid: FnId, _nodeid: NodeId) {
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
