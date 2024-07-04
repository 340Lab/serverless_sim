use crate::{fn_dag::FnContainer, sim_env::SimEnv};

impl SimEnv {
    pub fn on_fnins_cold_started(&self, con: &mut FnContainer) {
        for (req_id, _task) in &mut con.req_fn_state {
            let mut req = self.request_mut(*req_id);
            let metric = req.fn_metric.get_mut(&con.fn_id).unwrap();
            assert!(metric
                .cold_start_done_time
                .replace(self.current_frame())
                .is_none());
        }
    }
}
