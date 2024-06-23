use crate::{
    fn_dag::{EnvFnExt, FnId},
    request::Request,
    sim_env::SimEnv,
};

impl SimEnv {
    fn check_sub_task_ready_sche(&self, req: &mut Request, fnid: FnId) {
        let parents = self.func(fnid).parent_fns(self);
        for p in parents {
            if !req.done_fns.contains_key(&p) {
                return;
            }
        }
        // parents all done
        self.on_task_ready_sche(req, fnid);
    }
    fn check_sub_tasks_ready_sche(&self, req: &mut Request, fnid: FnId) {
        let subfns = self.func(fnid).sub_fns(self);
        for subfn in subfns {
            self.check_sub_task_ready_sche(req, subfn);
        }
    }
    pub fn on_task_done(&self, req: &mut Request, fnid: FnId) {
        self.check_sub_tasks_ready_sche(req, fnid);
        assert!(req
            .fn_metric
            .get_mut(&fnid)
            .unwrap()
            .fn_done_time
            .replace(self.current_frame())
            .is_none());
    }
}
