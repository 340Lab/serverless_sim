use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{DownCmd, MechType, MechanismImpl, ScheCmd, SimEnvObserve, UpCmd},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, Node, NodeId},
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

pub struct GreedyScheduler {
    node_mem_usage: HashMap<NodeId, f32>,
    node_task_count: HashMap<NodeId, usize>,
    node_funcs: HashMap<NodeId, HashSet<FnId>>,
}

impl GreedyScheduler {
    pub fn new() -> Self {
        Self {
            node_mem_usage: HashMap::new(),
            node_task_count: HashMap::new(),
            node_funcs: HashMap::new(),
        }
    }

    fn initialize_node_state(&mut self, env: &SimEnvObserve) {
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            // Initialize node memory usage based on unready_mem_mut()
            self.node_mem_usage.insert(node_id, *node.unready_mem_mut());

            // Initialize task count based on all_task_cnt()
            self.node_task_count.insert(node_id, node.all_task_cnt());

            // Initialize functions running on the node (only FnId)
            let fnids = node
                .fn_containers
                .borrow()
                .keys()
                .cloned()
                .collect::<HashSet<_>>();
            self.node_funcs.insert(node_id, fnids);
        }
    }

    fn update_node_state(&mut self, node_id: NodeId, env: &SimEnvObserve, fnid: FnId) {
        // Update memory usage
        let current_mem = self.node_mem_usage.entry(node_id).or_insert(0.0);
        *current_mem += env.func(fnid).mem;

        // Update task count
        let current_task_count = self.node_task_count.entry(node_id).or_insert(0);
        *current_task_count += 1;

        // Update functions
        let current_funcs = self.node_funcs.entry(node_id).or_insert_with(HashSet::new);
        let is_new_container = current_funcs.is_empty();
        current_funcs.insert(fnid);

        // Update memory usage for new container
        if is_new_container {
            *current_mem += env.func(fnid).container_mem();
        }
    }

    fn get_node_mem(&self, node: &Node) -> f32 {
        self.node_mem_usage
            .get(&node.node_id())
            .cloned()
            .unwrap_or(0.0)
    }

    fn get_node_task_count(&self, node: &Node) -> usize {
        self.node_task_count
            .get(&node.node_id())
            .cloned()
            .unwrap_or(0)
    }
}

impl Scheduler for GreedyScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        self.node_mem_usage.clear();
        self.node_task_count.clear();
        self.node_funcs.clear();
        self.initialize_node_state(env);
        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );

            let all_nodes = env.nodes();
            //迭代请求中的函数，选择最合适的节点进行调度
            for fnid in fns {
                let nodes = match mech.mech_type() {
                    MechType::ScaleScheSeparated => all_nodes
                        .iter()
                        .filter(|n| n.fn_containers.borrow().contains_key(&fnid))
                        .collect::<Vec<_>>(),
                    _ => all_nodes.iter().collect::<Vec<_>>(),
                };

                //使用贪婪算法选择最合适的节点
                let best_node = nodes.iter().min_by(|a, b| {
                    //优先考虑剩余内存最多的节点
                    let memory_order = self
                        .get_node_mem(a)
                        .partial_cmp(&self.get_node_mem(b))
                        .unwrap_or(Ordering::Equal);
                    //如果内存相同，比较所有任务数量
                    match memory_order {
                        Ordering::Equal => self
                            .get_node_task_count(a)
                            .cmp(&self.get_node_task_count(b)),
                        _ => memory_order,
                    }
                });

                if let Some(node) = best_node {
                    let node_id = node.node_id();
                    self.update_node_state(node_id, env, fnid);
                    cmd_distributor
                        .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                            nid: node_id,
                            reqid: req.req_id,
                            fnid,
                            memlimit: None,
                        }))
                        .unwrap();
                } else {
                    log::warn!("No suitable node found for function {}", fnid);
                }
            }
        }
    }
}
