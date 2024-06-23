use crate::{fn_dag::FnId, request::ReqId, sim_env::SimEnv};

impl SimEnv {
    pub fn on_task_data_recved(&self, reqid: ReqId, func: FnId) {
        assert!(self
            .request_mut(reqid)
            .fn_metric
            .get_mut(&func)
            .unwrap()
            .data_recv_done_time
            .replace(self.current_frame())
            .is_none());
    }
}
