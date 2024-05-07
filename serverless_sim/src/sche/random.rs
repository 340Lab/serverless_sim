use rand::prelude::SliceRandom;

use crate::{
    mechanism::{DownCmd, ScheCmd, UpCmd}, 
    sim_env::SimEnv,
    sim_run::{schedule_helper, Scheduler},
};


pub struct RandomScheduler {
}

impl RandomScheduler {
    pub fn new() -> Self {
        Self {
        }
    }
}

impl Scheduler for RandomScheduler {
    fn schedule_some(&mut self, env: &SimEnv) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        let mut sche_cmds = Vec::new();
        for (_req_id, req) in env.core.requests_mut().iter_mut() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            let nodes = env.nodes();
            for fnid in fns {
                let node = nodes.choose(&mut rand::thread_rng()).expect("No available nodes for scheduling");
                
                // 创建调度命令
                sche_cmds.push(ScheCmd {
                    nid: node.node_id(),
                    reqid: req.req_id,
                    fnid,
                    memlimit: None,
                });
            }
        }
        (vec![], sche_cmds, vec![])
    }
}
