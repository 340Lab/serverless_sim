use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crate::{
    fn_dag::{EnvFnExt, FnId}, mechanism::{DownCmd, MechType, MechanismImpl, ScheCmd, SimEnvObserve}, mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, node::{self, EnvNodeExt, Node, NodeId}, sche, sim_run::{schedule_helper, Scheduler}, with_env_sub::WithEnvCore
};

pub struct LoadLeastScheduler {
    fn_nodes: HashMap<FnId, HashSet<NodeId>>,
    node_cpu_usage: HashMap<NodeId, usize>,
}

impl LoadLeastScheduler {
    pub fn new() -> Self {
        Self {
            fn_nodes: HashMap::new(),
            node_cpu_usage: HashMap::new(),
        }
    }

    fn select_best_node_to_fn(&self, fnid: usize, _env: &SimEnvObserve) -> usize {

        let nodes = self.fn_nodes.get(&fnid).unwrap();

        let mut best_nodeid = 9999;
        let mut min_tasks_cnt = 9999;

        for nodeid in nodes {
            if best_nodeid == 9999 {
                best_nodeid = *nodeid;
            }

            let iter_node_tasks = self.node_cpu_usage.get(nodeid).unwrap();

            if min_tasks_cnt > *iter_node_tasks {
                best_nodeid = *nodeid;
                min_tasks_cnt = *iter_node_tasks;
            }

        }
        
        best_nodeid
    }
}

impl Scheduler for LoadLeastScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,) 
    {
        // 遍历每个节点，更新其资源使用情况
        for node in env.core().nodes().iter() {
            // 任务数量
            let all_task_cnt = node.all_task_cnt();
            self.node_cpu_usage.insert(node.node_id(), all_task_cnt);
        }

        for func in env.core().fns().iter() {
            let nodes = env
                .core().fn_2_nodes()
                .get(&func.fn_id)
                .map(|v| { v.clone() })
                .unwrap_or(HashSet::new());

            // log::info!("fn {}, nodes.len() = {}", func.fn_id, nodes.len());
            self.fn_nodes.insert(func.fn_id, nodes.clone());
        }

        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );

            //迭代请求中的函数，选择最合适的节点进行调度
            for fnid in fns {
                let sche_nodeid = self.select_best_node_to_fn(fnid, env);

                log::info!("schedule fn {} to node {}", fnid, sche_nodeid);

                if sche_nodeid != 9999 {
                    cmd_distributor
                        .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                            nid: sche_nodeid,
                            reqid: req.req_id,
                            fnid,
                            memlimit: None,
                        }))
                        .unwrap();

                    let tasks_cnt = self.node_cpu_usage.get(&sche_nodeid).unwrap();
                    self.node_cpu_usage.insert(sche_nodeid, tasks_cnt + 1);
                }
            }

        }

    }
}
