use std::cmp::Ordering;

use crate::{
    mechanism::{DownCmd, ScheCmd, UpCmd},
    sim_env::SimEnv,
    sim_run::{schedule_helper, Scheduler},
    request::{ReqId, Request},
};

use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{self, Hash, Hasher},
};

pub struct ConsistentHashScheduler{
    upper_limit: f32
}

impl ConsistentHashScheduler {
    pub fn new() -> Self {
        Self{
            upper_limit: 0.8
        }
    }

    fn schedule_one_req_fns(
        &mut self,
        env: &SimEnv,
        req: &mut Request,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        let fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::All,
        );

        let mut sche_cmds = vec![];
        let mut scale_up_cmds = vec![];

        for fnid in fns {
            let mut target_cnt = env.new_mech.scale_num(fnid);
            if target_cnt == 0 {
                target_cnt = 1;
            }

            let mut hasher = DefaultHasher::new();
            fnid.hash(&mut hasher);
            let mut node_id = hasher.finish() as usize % env.node_cnt();
            let mut node = env.node(node_id);
            let mut node_mem_use_rate = node.unready_mem() / node.rsc_limit.mem;
            let mut nodes_left_mem = env
                .core
                .nodes()
                .iter()
                .map(|n| n.left_mem_for_place_container())
                .collect::<Vec<_>>();
            while node_mem_use_rate > self.upper_limit {
                node_id = (node_id + 1) % env.node_cnt();
                node = env.node(node_id);
                node_mem_use_rate = node.unready_mem() / node.rsc_limit.mem;
            }
            sche_cmds.push(ScheCmd {
                nid: node_id,
                reqid: req.req_id,
                fnid,
                memlimit: None,
            });
            while target_cnt != 0 {
                if node.container(fnid).is_none() {
                    scale_up_cmds.push(UpCmd {
                        nid: node_id,
                        fnid,
                    });
                }
                node_id = (node_id + 1) % env.node_cnt();
                node = env.node(node_id);
                target_cnt -= 1;
            }
        }
        (scale_up_cmds, vec![], sche_cmds)
    }
}

impl Scheduler for ConsistentHashScheduler {
    fn schedule_some(&mut self, env: &SimEnv) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        let mut up_cmds = vec![];
        let mut sche_cmds = vec![];
        let mut down_cmds = vec![];

        for func in env.core.fns().iter() {
            let target = env.new_mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target < cur {
                down_cmds.extend(env.new_mech.scale_down_exec().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                ));
            }
        }

        for (_req_id, req) in env.core.requests_mut().iter_mut() {
            let (sub_up, sub_down, sub_sche) = self.schedule_one_req_fns(env, req);
            up_cmds.extend(sub_up);
            down_cmds.extend(sub_down);
            sche_cmds.extend(sub_sche);
        }

        (up_cmds, sche_cmds, down_cmds)
    }
}