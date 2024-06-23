use crate::{fn_dag::FnId, request::Request, sim_env::SimEnv};

impl SimEnv {
    pub fn on_task_ready_sche(&self, req: &mut Request, fnid: FnId) {
        assert!(req
            .fn_metric
            .get_mut(&fnid)
            .unwrap()
            .ready_sche_time
            .is_none());
        req.fn_metric.get_mut(&fnid).unwrap().ready_sche_time = Some(self.current_frame());
        // Happend in this frame. So real ready is next frame
    }
}
