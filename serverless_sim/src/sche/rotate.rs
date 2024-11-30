use std::collections::HashSet;

use crate::{
    fn_dag::EnvFnExt, mechanism::{MechanismImpl, ScheCmd, SimEnvObserve}, mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, node::EnvNodeExt, request::Request, sim_run::{schedule_helper, Scheduler}, with_env_sub::WithEnvCore
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
        _mech: &MechanismImpl,
        req: &mut Request,
        cmd_distributor: &MechCmdDistributor,
    ) {
        let fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::All,
        );

        if _mech.mech_type().is_no_scale() {
            for fnid in fns {
                let node_id = self.last_schedule_node_id;

                cmd_distributor
                    .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                        nid: node_id,
                        reqid: req.req_id,
                        fnid,
                        memlimit: None,
                    }))
                    .unwrap();

                self.last_schedule_node_id = (self.last_schedule_node_id + 1) % env.node_cnt();
            }
        } else {
            for fnid in fns {
                let mut nodes = HashSet::new();
                env.fn_containers_for_each(fnid, |container| {
                    nodes.insert(container.node_id);
                });

                let mut node_list = Vec::new();
                for node_id in nodes.iter() {
                    node_list.push(*node_id);
                }

                let mut node_id = self.last_schedule_node_id;

                if !node_list.is_empty() {
                    node_id = node_list[(self.last_schedule_node_id + 1) % node_list.len()];
                    self.last_schedule_node_id = node_id;
                }

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
