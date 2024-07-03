use crate::{
    fn_dag::EnvFnExt,
    mechanism::{DownCmd, MechanismImpl, ScheCmd, SimEnvObserve, UpCmd},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::EnvNodeExt,
    request::Request,
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

pub struct RotateScheduler {
    last_schedule_node_id: usize,
}

impl RotateScheduler {
    pub fn new() -> Self {
        Self { last_schedule_node_id: 0 }
    }

    fn schedule_one_req_fns(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        req: &mut Request,
        cmd_distributor: &MechCmdDistributor,
    ) {
        let fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::All,
        );

        for fnid in fns {

            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    nid: self.last_schedule_node_id,
                    reqid: req.req_id,
                    fnid,
                    memlimit: None,
                }))
                .unwrap();

            self.last_schedule_node_id = (self.last_schedule_node_id + 1) % env.node_cnt();

        }
    }
}

impl Scheduler for RotateScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            // let (sub_up, sub_down, sub_sche) =
            self.schedule_one_req_fns(env, mech, req, cmd_distributor);
        }
    }
}
