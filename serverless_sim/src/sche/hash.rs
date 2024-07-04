use crate::{
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::EnvNodeExt,
    request::Request,
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub struct HashScheduler {
    upper_limit: f32,
}

impl HashScheduler {
    pub fn new() -> Self {
        Self { upper_limit: 0.8 }
    }

    fn schedule_one_req_fns(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        req: &mut Request,
        cmd_distributor: &MechCmdDistributor,
    ) {
        let fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::All,
        );

        for fnid in fns {

            let mut hasher = DefaultHasher::new();
            fnid.hash(&mut hasher);
            let node_id = hasher.finish() as usize % env.node_cnt();

            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    nid: node_id,
                    reqid: req.req_id,
                    fnid,
                    memlimit: None,
                }))
                .unwrap();
        }
    }
}

impl Scheduler for HashScheduler {
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
