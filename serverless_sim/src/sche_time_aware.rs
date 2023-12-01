use std::{collections::HashMap, u128::MAX};

use daggy::{
    petgraph::visit::{EdgeRef, IntoEdgeReferences},
    EdgeIndex, Walker,
};
use rand::{thread_rng, Rng};

use crate::{
    fn_dag::{DagId, FnId},
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

pub struct TimeScheduler {
    // dag_fn_prorities: HashMap<DagId, Vec<(FnId, f32)>>,
    dag_fn_prorities_: HashMap<DagId, HashMap<FnId, f32>>,
}

// 基于时间感知的函数调度算法
impl TimeScheduler {
    pub fn new() -> Self {
        Self {
            dag_fn_prorities: HashMap::new(),
        }
    }

    fn prepare_priority_for_dag(&mut self, req: &mut Request, env: &SimEnv) {
        let dag = env.dag(req.dag_i);

        //计算函数的优先级P：
        if !self.dag_fn_prorities.contains_key(&dag.dag_i) {
            //map存储每个函数的优先级
            let mut map: HashMap<usize, f32> = HashMap::new();
            let mut walker = dag.new_dag_walker();
            // P = 函数的资源消耗量×(启动时间+函数执行时间)
            while let Some(func_g_i) = walker.next(&dag.dag_inner) {
                let fnid = dag.dag_inner[func_g_i];
                let t_exe = func.cpu / node.rsc_limit.cpu;

                let consume_mem = func.mem;
                let p = consume_mem * (t_exe + func.cold_start_time);

                map.insert(fnid, p);
            }
            let mut prio_order = map.into_iter().collect::<Vec<_>>();
            // Sort the vector by the value in the second element of the tuple.
            // 升序排序优先级
            prio_order.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            self.dag_fn_prorities.insert(dag.dag_i, prio_order);
        }
    }

    fn select_node_for_fn(
        &self,
        // 为f分配的node
        schedule_to_map: &mut HashMap<FnId, NodeId>,
        schedule_to: &mut Vec<(FnId, NodeId)>,
        func_id: FnId,
        req: &mut Request,
        env: &SimEnv,
    ) {
        let func = env.func(func_id);
        let nodes = env.nodes.borrow();

        for nodeid in 0..nodes.len() {
            let node = env.node(nodeid);
            let limit_cpu = node.rsc_limit.cpu;
            // 将满足资源需求的node分配给func
            if (func.cpu < limit_cpu) {
                schedule_to_map.insert(func_id, nodeid);
                schedule_to.push((func_id, nodeid));
                // 更新节点的资源
                nodes.rsc_limit.cpu = limit_cpu - func.cpu;
            }
        }
    }
    //实现Time算法
    fn schedule_for_one_req(&mut self, req: &mut Request, env: &SimEnv) {
        self.prepare_priority_for_dag(req, env);

        let dag = env.dag(req.dag_i);
        let mut schedule_to = Vec::<(FnId, NodeId)>::new();
        let mut schedule_to_map = HashMap::<FnId, NodeId>::new();

        // 获取优先级
        let prio_order = self.dag_fn_prorities.get(&dag.dag_i).unwrap();

        log::info!("prio order: {:?}", prio_order);
        for (func_id, _fun_prio) in prio_order {
            self.select_node_for_fn(&mut schedule_to_map, &mut schedule_to, *func_id, req, env);
        }

        for (fnid, nodeid) in schedule_to {
            // if env.node(nodeid).fn_containers.get(&fnid).is_none() {
            //     if env
            //         .scale_executor
            //         .borrow_mut()
            //         .scale_up_fn_to_nodes(env, fnid, &vec![nodeid])
            //         == 0
            //     {
            //         panic!("can't fail");
            //     }
            // }
            // if env.node(fn_node).mem_enough_for_container(&env.func(fnid)) {
            env.schedule_reqfn_on_node(req, fnid, nodeid);
        }
    }
}

impl Scheduler for TimeScheduler {
    fn schedule_some(&mut self, env: &SimEnv) {
        for (_, req) in env.requests.borrow_mut().iter_mut() {
            if req.fn_node.len() == 0 {
                self.schedule_for_one_req(req, env);
            }
        }

        // let mut to_scale_down = vec![];
        // // 回收空闲container
        // for n in env.nodes.borrow().iter() {
        //     for (_, c) in n.fn_containers.iter() {
        //         if c.recent_frame_is_idle(3) && c.req_fn_state.len() == 0 {
        //             to_scale_down.push((n.node_id(), c.fn_id));
        //         }
        //     }
        // }
        // for (n, f) in to_scale_down {
        //     env.scale_executor
        //         .borrow_mut()
        //         .scale_down(env, ScaleOption::ForSpecNodeFn(n, f));
        // }
    }

    fn prepare_this_turn_will_schedule(&mut self, env: &SimEnv) {}
    fn this_turn_will_schedule(&self, fnid: FnId) -> bool {
        false
    }
}
