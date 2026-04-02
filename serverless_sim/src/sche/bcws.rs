use std::collections::HashMap;

use daggy::Walker;

use crate::{
    fn_dag::{DagId, EnvFnExt, FnDAG, FnId},
    mechanism::{MechType, MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::Request,
    sim_run::Scheduler,
    with_env_sub::WithEnvCore,
};

pub struct BCWSScheduler {
    dag_fn_ranks: HashMap<DagId, HashMap<FnId, f32>>,
}

impl BCWSScheduler {
    pub fn new() -> Self {
        Self {
            dag_fn_ranks: HashMap::new(),
        }
    }

    fn prepare_rank_for_dag(&mut self, req: &Request, env: &SimEnvObserve) {
        if self.dag_fn_ranks.contains_key(&req.dag_i) {
            return;
        }

        let dag = env.dag(req.dag_i).clone();
        let avg_node_cpu = {
            let nodes = env.core().nodes();
            let total_cpu = nodes.iter().map(|node| node.rsc_limit.cpu).sum::<f32>();
            (total_cpu / (nodes.len().max(1) as f32)).max(0.0001)
        };
        let min_bandwidth = env.node_btw_get_lowest().max(0.0001);

        let mut ranks = HashMap::<FnId, f32>::new();
        let mut topo_order = Vec::new();
        let mut walker = dag.new_dag_walker();

        while let Some(func_g_i) = walker.next(&dag.dag_inner) {
            topo_order.push(func_g_i);
            let fnid = dag.dag_inner[func_g_i];
            let compute_cost = env.func(fnid).cpu / avg_node_cpu;
            ranks.insert(fnid, compute_cost);
        }

        while let Some(func_g_i) = topo_order.pop() {
            let fnid = dag.dag_inner[func_g_i];
            let mut max_succ_cost: f32 = 0.0;

            for (edge, child_g_i) in dag.dag_inner.children(func_g_i).iter(&dag.dag_inner) {
                let child_fnid = dag.dag_inner[child_g_i];
                let transfer_cost = dag
                    .dag_inner
                    .edge_weight(edge)
                    .copied()
                    .unwrap_or(0.0)
                    .max(0.0)
                    / min_bandwidth;
                let child_rank = *ranks.get(&child_fnid).unwrap_or(&0.0);
                max_succ_cost = max_succ_cost.max(transfer_cost + child_rank);
            }

            if let Some(rank) = ranks.get_mut(&fnid) {
                *rank += max_succ_cost;
            }
        }

        self.dag_fn_ranks.insert(req.dag_i, ranks);
    }

    fn schedulable_fns(
        &self,
        req: &Request,
        dag: &FnDAG,
        env: &SimEnvObserve,
        planned_nodes: &HashMap<FnId, NodeId>,
    ) -> Vec<FnId> {
        let mut ready = Vec::new();
        let mut walker = dag.new_dag_walker();

        'next_fn: while let Some(func_g_i) = walker.next(&dag.dag_inner) {
            let fnid = dag.dag_inner[func_g_i];
            if planned_nodes.contains_key(&fnid) {
                continue;
            }

            for parent in env.func(fnid).parent_fns(env) {
                if !planned_nodes.contains_key(&parent) && req.get_fn_node(parent).is_none() {
                    continue 'next_fn;
                }
            }

            ready.push(fnid);
        }

        ready
    }

    fn candidate_nodes(
        &self,
        fnid: FnId,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
    ) -> Vec<NodeId> {
        let mut nodes = match mech.mech_type() {
            MechType::ScaleScheSeparated => env
                .core()
                .fn_2_nodes()
                .get(&fnid)
                .map(|node_ids| node_ids.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default(),
            _ => Vec::new(),
        };

        if nodes.is_empty() {
            nodes = env.core().nodes().iter().map(|node| node.node_id()).collect();
        }

        nodes
    }

    fn estimate_finish_time(
        &self,
        fnid: FnId,
        candidate_node: NodeId,
        env: &SimEnvObserve,
        planned_nodes: &HashMap<FnId, NodeId>,
        node_loads: &HashMap<NodeId, usize>,
    ) -> f32 {
        let func = env.func(fnid);
        let node = env.node(candidate_node);
        let compute_time = func.cpu / node.rsc_limit.cpu.max(0.0001);
        let queue_delay = (*node_loads.get(&candidate_node).unwrap_or(&0) as f32) * compute_time;

        let mut transfer_ready_time: f32 = 0.0;
        for parent in env.func(fnid).parent_fns(env) {
            let Some(parent_node) = planned_nodes.get(&parent).copied() else {
                continue;
            };
            if parent_node == candidate_node {
                continue;
            }

            let parent_output = env.func(parent).out_put_size;
            let bandwidth = env.node_get_speed_btwn(parent_node, candidate_node).max(0.0001);
            transfer_ready_time = transfer_ready_time.max(parent_output / bandwidth);
        }

        transfer_ready_time + queue_delay + compute_time
    }

    fn select_node_for_fn(
        &self,
        fnid: FnId,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        planned_nodes: &HashMap<FnId, NodeId>,
        node_loads: &HashMap<NodeId, usize>,
    ) -> NodeId {
        let candidates = self.candidate_nodes(fnid, env, mech);
        let mut best = None::<(f32, usize, NodeId)>;

        for candidate in candidates {
            let finish_time =
                self.estimate_finish_time(fnid, candidate, env, planned_nodes, node_loads);
            let node_tasks = *node_loads.get(&candidate).unwrap_or(&0);
            let score = (finish_time, node_tasks, candidate);

            if let Some(cur_best) = best {
                if score < cur_best {
                    best = Some(score);
                } else {
                    best = Some(cur_best);
                }
            } else {
                best = Some(score);
            }
        }

        best.map(|(_, _, node_id)| node_id).unwrap_or(0)
    }

    fn schedule_for_one_req(
        &mut self,
        req: &Request,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        self.prepare_rank_for_dag(req, env);

        let dag = env.dag(req.dag_i).clone();
        let dag_ranks = self.dag_fn_ranks.get(&req.dag_i).unwrap();
        let mut planned_nodes = req.fn_node.clone();
        let mut node_loads = env
            .core()
            .nodes()
            .iter()
            .map(|node| (node.node_id(), node.all_task_cnt()))
            .collect::<HashMap<_, _>>();

        loop {
            let mut ready = self.schedulable_fns(req, &dag, env, &planned_nodes);
            if ready.is_empty() {
                break;
            }

            ready.sort_by(|a, b| {
                let rank_a = *dag_ranks.get(a).unwrap_or(&0.0);
                let rank_b = *dag_ranks.get(b).unwrap_or(&0.0);
                rank_b
                    .total_cmp(&rank_a)
                    .then_with(|| {
                        let cpu_a = env.func(*a).cpu;
                        let cpu_b = env.func(*b).cpu;
                        cpu_b.total_cmp(&cpu_a)
                    })
                    .then_with(|| a.cmp(b))
            });

            for fnid in ready {
                if planned_nodes.contains_key(&fnid) {
                    continue;
                }

                let node_id =
                    self.select_node_for_fn(fnid, env, mech, &planned_nodes, &node_loads);
                planned_nodes.insert(fnid, node_id);
                node_loads
                    .entry(node_id)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);

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

impl Scheduler for BCWSScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        for (_, req) in env.core().requests().iter() {
            if req.fn_node.len() == req.fn_count(env) {
                continue;
            }
            self.schedule_for_one_req(req, env, mech, cmd_distributor);
        }
    }
}
