use crate::node::EnvNodeExt;
use crate::with_env_sub::WithEnvCore;
use crate::{
    mechanism::{DownCmd, MechType, MechanismImpl, ScheCmd, SimEnvObserve, UpCmd},
    sim_run::{schedule_helper, Scheduler},
};
use rand::prelude::SliceRandom;
use std::borrow::Borrow;

pub struct RandomScheduler {}

impl RandomScheduler {
    pub fn new() -> Self {
        Self {}
    }
}

impl Scheduler for RandomScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
    ) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        let mut sche_cmds = Vec::new();
        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );

            for fnid in fns {
                let nodesid = match mech.mech_type() {
                    MechType::ScaleScheSeparated => env
                        .nodes()
                        .iter()
                        .filter(|n| n.fn_containers.borrow().contains_key(&fnid))
                        .map(|n| n.node_id())
                        .collect::<Vec<_>>(),
                    _ => env
                        .nodes()
                        .borrow()
                        .iter()
                        .map(|n| n.node_id())
                        .collect::<Vec<_>>(),
                };

                let nodeid = if let Some(node) = nodesid.choose(&mut rand::thread_rng()) {
                    node
                } else {
                    // 处理没有可用节点的情况，例如记录日志或返回错误
                    eprintln!("No available nodes for scheduling");
                    return (vec![], vec![], vec![]);
                };

                // 创建调度命令
                sche_cmds.push(ScheCmd {
                    nid: *nodeid,
                    reqid: req.req_id,
                    fnid,
                    memlimit: None,
                });
            }
        }
        (vec![], sche_cmds, vec![])
    }
}
