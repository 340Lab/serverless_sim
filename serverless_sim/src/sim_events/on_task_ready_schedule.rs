use crate::{fn_dag::FnId, request::Request, sim_env::SimEnv};

impl SimEnv {
    pub fn on_task_ready_sche(&self, req: &mut Request, fnid: FnId) {
        let fnmetric = req.fn_metric.get_mut(&fnid).unwrap();
        assert!(fnmetric.ready_sche_time.is_none());
        fnmetric.ready_sche_time = Some(self.current_frame());
        assert!(fnmetric.data_recv_done_time.is_none());
        // Happend in this frame. So real ready is next frame
    }
}
