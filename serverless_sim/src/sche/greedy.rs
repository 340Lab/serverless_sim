use std::{borrow::Borrow, cmp::Ordering};

use crate::{
    mechanism::{DownCmd, MechType, ScheCmd, UpCmd}, node::Node, sim_env::SimEnv, sim_run::{schedule_helper, Scheduler}
};

pub struct GreedyScheduler{

}

impl GreedyScheduler {
    pub fn new() -> Self {
        Self{}
    }
}

impl Scheduler for GreedyScheduler {
    fn schedule_some(&mut self, env: &SimEnv) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        let mut sche_cmds = Vec::new();
        for (_req_id, req) in env.core.requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );

            let all_nodes = env.nodes();
            //迭代请求中的函数，选择最合适的节点进行调度
            for fnid in fns {
                let nodes = match env.new_mech.mech_type(env) {
                    MechType::ScaleScheSeparated => {
                        all_nodes
                            .iter()
                            .filter(|n| n.fn_containers.borrow().contains_key(&fnid))
                            .collect::<Vec<_>>()
                    }
                    _ => all_nodes
                            .iter()
                            .collect::<Vec<_>>(),
                };
                
                //使用贪婪算法选择最合适的节点
                let best_node = nodes.iter()
                    .filter(|node| node.mem_enough_for_container(&env.func(fnid)))
                    .min_by(|a, b| {
                        //优先考虑剩余内存最多的节点
                        let memory_order = a.left_mem().partial_cmp(&b.left_mem()).unwrap_or(Ordering::Equal);
                        //如果内存相同，比较正在运行的任务数量
                        match memory_order {
                            Ordering::Equal => a.running_task_cnt().cmp(&b.running_task_cnt()),
                            _ => memory_order,
                        }
                    });

                if let Some(node) = best_node {
                    sche_cmds.push(ScheCmd {
                        nid: node.node_id(),
                        reqid: req.req_id,
                        fnid,
                        memlimit: None,
                    });
                } else {
                    log::warn!("No suitable node found for function {}", fnid);
                }
            }
        }
        (vec![], sche_cmds, vec![])
    }
}