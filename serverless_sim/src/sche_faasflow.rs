use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{self, Hash, Hasher},
};

use daggy::{
    petgraph::visit::{EdgeRef, IntoEdgeReferences},
    EdgeIndex,
};
use rand::{thread_rng, Rng};

use crate::{
    fn_dag::FnId,
    node::NodeId,
    request::{ReqId, Request},
    scale_executor::{ScaleExecutor, ScaleOption},
    schedule::Scheduler,
    sim_env::SimEnv,
    util,
};

struct RequestSchedulePlan {
    fn_nodes: HashMap<FnId, NodeId>,
}

pub struct FaasFlowScheduler {
    // request_schedule_state: HashMap<ReqId, RequestSchedulePlan>,
    scheduled_reqs: HashSet<ReqId>,
}

impl FaasFlowScheduler {
    pub fn new() -> Self {
        Self {
            scheduled_reqs: HashSet::new(),
        }
    }

    fn schedule_one_req(&mut self, req: &mut Request, env: &SimEnv) {
        log::info!("faasflow start generate schedule for req {}", req.req_id);
        let mut nodes_left_mem = env
            .nodes
            .borrow()
            .iter()
            .map(|n| n.left_mem_for_place_container())
            .collect::<Vec<_>>();
        //1.为请求的所有函数随机分配节点
        let mut fn_poses = HashMap::new();
        {
            let dag = env.dag(req.dag_i);
            let mut walker = dag.new_dag_walker();
            while let Some(fnode) = walker.next(&dag.dag_inner) {
                let fnid = dag.dag_inner[fnode];
                let mut hasher = DefaultHasher::new();
                fnid.hash(&mut hasher);
                let node_id = hasher.finish() as usize % env.node_cnt(); //thread_rng().gen_range(0..nodes_left_mem.len());
                                                                         // let node_id = (0, nodes_left_mem.len());
                fn_poses.insert(fnid, node_id);
                nodes_left_mem[node_id] -= env.func(fnid).container_mem();
            }
        }
        //2.遍历收集关键路径
        let dag = env.dag(req.dag_i);
        let critical_path_nodes = util::graph::critical_path(&dag.dag_inner);
        log::info!("C");
        let mut cri_paths = vec![];
        for i in 0..critical_path_nodes.len() - 1 {
            cri_paths.push(
                dag.dag_inner
                    .find_edge(critical_path_nodes[i], critical_path_nodes[i + 1])
                    .unwrap(),
            );
            // non_cti_paths.remove(&(critical_path_nodes[i], critical_path_nodes[i+1]));
        }
        let mut non_cri_paths = dag
            .dag_inner
            .edge_references()
            .map(|e| e.id())
            .filter(|e| !cri_paths.contains(e))
            .collect::<Vec<_>>();
        let cmp_edge = |e1: &EdgeIndex, e2: &EdgeIndex| {
            let e1_weight = *dag.dag_inner.edge_weight(*e1).unwrap();
            let e2_weight = *dag.dag_inner.edge_weight(*e2).unwrap();
            e2_weight.partial_cmp(&e1_weight).unwrap()
        };
        cri_paths.sort_by(cmp_edge);
        non_cri_paths.sort_by(cmp_edge);

        if cri_paths.len() > 1 {
            assert!(
                *dag.dag_inner.edge_weight(cri_paths[0]).unwrap()
                    >= *dag.dag_inner.edge_weight(cri_paths[1]).unwrap()
            );
        }

        let mut try_merge_e = |e: EdgeIndex| {
            let (nbegin, nend) = dag.dag_inner.edge_endpoints(e).unwrap();
            let fnbegin = dag.dag_inner[nbegin];
            let fnend = dag.dag_inner[nend];
            let old_node_begin = *fn_poses.get(&fnbegin).unwrap();
            let old_node_end = *fn_poses.get(&fnend).unwrap();
            if old_node_begin == old_node_end {
                return;
            }
            if nodes_left_mem[old_node_begin] > env.func(fnend).container_mem() {
                nodes_left_mem[old_node_begin] -= env.func(fnend).container_mem();
                nodes_left_mem[old_node_end] += env.func(fnend).container_mem();
                fn_poses.insert(fnend, old_node_begin);
            }
        };

        for e in cri_paths {
            try_merge_e(e);
        }
        for e in non_cri_paths {
            try_merge_e(e);
        }

        // self.request_schedule_state
        //     .insert(req.req_id, RequestSchedulePlan { fn_nodes: fn_poses });
        log::info!("faasflow end generate schedule for req {}", req.req_id);
        for (fnid, nodeid) in fn_poses {
            env.schedule_reqfn_on_node(req, fnid, nodeid)
        }
        self.scheduled_reqs.insert(req.req_id);
    }

    // fn do_some_schedule(&self, req: &mut Request, env: &SimEnv) {
    //     let dag = env.dag(req.dag_i);
    //     let plan = self.request_schedule_state.get(&req.req_id).unwrap();
    //     let mut walker = dag.new_dag_walker();
    //     while let Some(fnode) = walker.next(&dag.dag_inner) {
    //         let fnid = dag.dag_inner[fnode];
    //         // Already scheduled
    //         if req.get_fn_node(fnid).is_some() {
    //             continue;
    //         }
    //         // Not schduled but not all parents done
    //         if !req.parents_all_done(env, fnid) {
    //             continue;
    //         }
    //         // Ready to be scheduled
    //         let fn_node = *plan.fn_nodes.get(&fnid).unwrap();
    //         if env.node(fn_node).container(fnid).is_none() {
    //             if env
    //                 .scale_executor
    //                 .borrow_mut()
    //                 .scale_up_fn_to_nodes(env, fnid, &vec![fn_node])
    //                 == 0
    //             {
    //                 continue;
    //             }
    //         }
    //         // if env.node(fn_node).mem_enough_for_container(&env.func(fnid)) {
    //         env.schedule_reqfn_on_node(req, fnid, fn_node);
    //         // }
    //     }
    // }

    fn schedule_for_one_req(&mut self, req: &mut Request, env: &SimEnv) {
        if !self.scheduled_reqs.contains(&req.req_id) {
            self.schedule_one_req(req, env);
        }
        // self.do_some_schedule(req, env);
    }
}

// 图形调度器中分组和调度算法的关键步骤如下所示。
// 在初始化阶段，每个函数节点都作为单独的组进行初始化，并且工作节点是随机分配的（第1-2行）。
// 首先，算法从拓扑排序和迭代开始。在每次迭代的开始，它将使用贪婪方法来定位DAG图中关键路径上具有最长边的两个函数，
// 并确定这两个函数是否可以合并到同一组（第3-8行）。
// 如果这两个函数被分配到不同的组中，它们将被合并（第9行）。
// 在合并组时，需要考虑额外的因素。
//  首先，算法需要确保合并的函数组不超过工作节点的最大容量（第10-12行）。
//  否则，合并的组将无法部署在任何节点上。其次，组内局部化的数据总量不能违反内存约束（第13-18行）。
//  同时，在合并的组中不能存在任何资源竞争的函数对𝑐𝑜𝑛𝑡 (𝐺) = {(𝑓𝑖, 𝑓𝑗 )}（第19-20行）。
//  最后，调度算法将采用装箱策略，根据节点容量为每个函数组选择适当的工作节点（第21-23行）。
// 根据上述逻辑，算法迭代直到收敛，表示函数组不再更新。
impl Scheduler for FaasFlowScheduler {
    fn schedule_some(&mut self, env: &SimEnv) {
        for (_, req) in env.requests.borrow_mut().iter_mut() {
            self.schedule_for_one_req(req, env);
        }

        let mut to_scale_down = vec![];
        // 回收空闲container
        for n in env.nodes.borrow().iter() {
            for (_, c) in n.fn_containers.borrow().iter() {
                if c.recent_frame_is_idle(3) && c.req_fn_state.len() == 0 {
                    to_scale_down.push((n.node_id(), c.fn_id));
                }
            }
        }
        for (n, f) in to_scale_down {
            env.scale_executor
                .borrow_mut()
                .scale_down(env, ScaleOption::ForSpecNodeFn(n, f));
        }
    }

    fn prepare_this_turn_will_schedule(&mut self, env: &SimEnv) {}

    fn this_turn_will_schedule(&self, fnid: FnId) -> bool {
        panic!("not support");
    }
}
