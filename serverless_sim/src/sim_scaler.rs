use std::collections::HashSet;

use clap::ValueEnum;
use daggy::NodeIndex;
use enum_dispatch::enum_dispatch;

use crate::{
    actions::Action,
    fn_dag::{FnContainer, FnContainerState, FnId},
    node::{Node, NodeId},
    request::ReqId,
    sim_env::SimEnv,
    sim_scaler_ai::AIScaler,
    sim_scaler_hpa::HpaScaler,
};

pub enum ScaleArg {
    AIScaler(Action),
    HPAScaler,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ScalerType {
    AiScaler,
    HpaScaler,
}

#[enum_dispatch]
pub trait Scaler {
    fn scale(&mut self, sim_env: &SimEnv, arg: ScaleArg);
}

#[enum_dispatch(Scaler)]
pub enum ScalerImpl {
    AIScaler(AIScaler),
    HpaScaler(HpaScaler),
}

impl SimEnv {
    // /// 从模型拿到action，然后执行对应步骤
    // pub fn scale(&self, action: Action) {
    //     log::info!("scale with action {action:?}");
    //     match action {
    //         Action::ScaleUpWithElem => {
    //             // self.scale_up_rule_based(true);
    //         }
    //         Action::ScaleUpWithoutElem => {
    //             // self.scale_up_rule_based(false);
    //         }
    //         Action::ProactiveScaleDown => {
    //             // self.scale_down_rule_based(None);
    //         }
    //         Action::DoNothing => {}
    //     }
    // }

    // pub fn set_scale_down_result2(
    //     &self,
    //     fn_nodes: &mut HashSet<NodeId>,
    //     fnid: FnId,
    //     nodeid: NodeId,
    // ) {
    //     self.nodes.borrow_mut()[nodeid].fn_containers.remove(&fnid);
    //     fn_nodes.remove(&nodeid);
    //     self.nodes.borrow_mut()[nodeid].mem -= self.func(fnid).cold_start_container_mem_use;
    // }

    pub fn set_scale_down_result(&self, fnid: FnId, nodeid: NodeId) {
        log::info!("scale down fn {fnid} from node {nodeid}");
        let cont = self.nodes.borrow_mut()[nodeid]
            .fn_containers
            .remove(&fnid)
            .unwrap();
        self.fn_2_nodes
            .borrow_mut()
            .get_mut(&fnid)
            .unwrap()
            .remove(&nodeid);
        match cont.state() {
            FnContainerState::Starting { .. } => {
                self.node_mut(nodeid).mem -= self.func(fnid).cold_start_container_mem_use
            }
            FnContainerState::Running => {
                self.node_mut(nodeid).mem -= self.func(fnid).container_mem()
            }
        }
    }

    // pub fn set_scale_up_result2(&self, fn_id: FnId, node: &mut Node) {
    //     let node_id = node.node_id();
    //     log::info!("expand fn: {fn_id} to node: {node_id}");
    //     // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
    //     self.fn_2_nodes
    //         .borrow_mut()
    //         .entry(fn_id)
    //         .and_modify(|v| {
    //             v.insert(node_id);
    //         })
    //         .or_insert_with(|| {
    //             let mut set = HashSet::new();
    //             set.insert(node_id);
    //             set
    //         });

    //     node.fn_containers
    //         .entry(fn_id)
    //         .and_modify(|_| panic!("fn container already exists"))
    //         .or_insert(FnContainer::new(fn_id, self));

    //     node.mem += self.func(fn_id).cold_start_container_mem_use;
    // }

    pub fn set_scale_up_result(&self, fn_id: FnId, node_id: NodeId) {
        log::info!("expand fn: {fn_id} to node: {node_id}");
        // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
        self.fn_2_nodes
            .borrow_mut()
            .entry(fn_id)
            .and_modify(|v| {
                v.insert(node_id);
            })
            .or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(node_id);
                set
            });

        self.nodes.borrow_mut()[node_id]
            .fn_containers
            .entry(fn_id)
            .and_modify(|_| panic!("fn container already exists"))
            .or_insert(FnContainer::new(fn_id, self));

        self.nodes.borrow_mut()[node_id].mem += self.func(fn_id).cold_start_container_mem_use;
    }

    pub fn get_request_first_unscheduled_fn(&self) -> Option<(ReqId, FnId, NodeIndex)> {
        // 1. 从请求队列中拿到一个请求
        let env_reqs = self.requests.borrow();
        let mut iter = env_reqs.iter();
        while let Some((req_id, req)) = iter.next() {
            // 2. 从请求中拿到一个fn
            if let Some((fn_id, fngid)) = req.fn_2_bind_node() {
                return Some((*req_id, fn_id, fngid));
            }
        }

        None
    }

    // fn expand_random(&self) {}

    // // fn scale_down_random(&self) {}

    // // fn scale_down_rule_based(&self, specify_node: Option<NodeId>) {
    // //     let fn_container_2_remove_on_node = |n: NodeId| {
    // //         self.node(n)
    // //             .fn_containers
    // //             .iter()
    // //             // no running fn instance
    // //             .filter(|v| v.1.req_fn_state.len() == 0)
    // //             .min_by(|a, b| a.1.use_freq(self).partial_cmp(&b.1.use_freq(self)).unwrap())
    // //             .map(|fc| *fc.0)
    // //     };
    // //     if let Some(specify_node) = specify_node {
    // //         // node 上找出空闲且使用频率最低的容器
    // //         if let Some(fn_2_remove) = fn_container_2_remove_on_node(specify_node) {
    // //             self.node_mut(specify_node)
    // //                 .fn_containers
    // //                 .remove(&fn_2_remove)
    // //                 .unwrap();
    // //         }
    // //     } else {
    // //         let mut node_fns = vec![];
    // //         for n in 0..self.nodes.borrow().len() {
    // //             if let Some(fn_2_remove) = fn_container_2_remove_on_node(n) {
    // //                 node_fns.push((n, fn_2_remove));
    // //             }
    // //         }
    // //         let node_fn_2_remove = node_fns.iter().min_by(|node_fn_a, node_fn_b| {
    // //             self.node(node_fn_a.0)
    // //                 .fn_containers
    // //                 .get(&node_fn_a.1)
    // //                 .unwrap()
    // //                 .use_freq(self)
    // //                 .partial_cmp(
    // //                     &self
    // //                         .node(node_fn_b.0)
    // //                         .fn_containers
    // //                         .get(&node_fn_b.1)
    // //                         .unwrap()
    // //                         .use_freq(self),
    // //                 )
    // //                 .unwrap()
    // //         });

    // //         if let Some((nodeid, fnid)) = node_fn_2_remove {
    // //             self.node_mut(*nodeid).fn_containers.remove(fnid).unwrap();
    // //         } else {
    // //             log::info!("no fn to remove, scale down failed");
    // //         }
    // //     }
    // // }

    // // 针对最前面的请求的某一个fn，进行扩容
    // // 一个fn从0->1扩容和1-n扩容，所得的分应该是不同的, 0->1更重要
    // fn scale_up_rule_based(&self, elem_if_need: bool) -> Option<NodeId> {
    //     // 有请求
    //     if let Some((cur_req_i, fn_id, fn_node_id)) = self.get_request_fn_for_expand() {
    //         let cur_req_dag_i: DagId = self.requests.borrow().get(&cur_req_i).unwrap().dag_i;
    //         let parents = self.dags.borrow()[cur_req_dag_i].dag.parents(fn_node_id);
    //         // let children = self.dags[cur_req_dag_i].dag.children(fn_node_id);
    //         let mut parent_fns: Vec<FnId> = vec![];
    //         // let mut child_fns: Vec<FnId> = vec![];
    //         for (_edge_i, node_i) in parents.iter(&self.dags.borrow()[cur_req_dag_i].dag) {
    //             parent_fns.push(self.dags.borrow()[cur_req_dag_i].dag[node_i]);
    //         }

    //         let filter = |n: &&Node| {
    //             !n.fn_containers.contains_key(&fn_id)
    //                 && n.left_mem() >= CONTAINER_BASIC_MEM + NODE_LEFT_MEM_THRESHOLD
    //         };
    //         if parent_fns.len() == 0
    //         // && child_fns.len() == 0
    //         {
    //             // ====================================================
    //             // 扩容fn到资源最多的节点
    //             // ====================================================
    //             // let find_node = || {
    //             //     self.algo_find_the_most_idle_node(filter).map_or_else(
    //             //         || {
    //             //             log::warn!("sim scale up for req{cur_req_i} fn{fn_id} failed");
    //             //             None
    //             //         },
    //             //         |most_idle_node| {
    //             //             log::info!(
    //             //                 "sim scale up for req{cur_req_i} fn{fn_id} to most idle node{most_idle_node}"
    //             //             );
    //             //             self.set_scale_up_result(fn_id, most_idle_node);
    //             //             Some(most_idle_node)
    //             //         },)
    //             // };
    //             // let mut res = find_node();
    //             // if res.is_none() && elem_if_need {
    //             //     let all_node_lack_mem = self
    //             //         .nodes
    //             //         .borrow()
    //             //         .iter()
    //             //         .filter(|n| n.left_mem() >= CONTAINER_BASIC_MEM + NODE_LEFT_MEM_THRESHOLD)
    //             //         .count()
    //             //         == 0;
    //             //     if all_node_lack_mem {
    //             //         //select one fn container to scale_down
    //             //         self.scale_down_rule_based(None);
    //             //         res = find_node();
    //             //         if res.is_none() {
    //             //             log::warn!(
    //             //                 "sim scale up for req{cur_req_i} fn{fn_id} failed after elem"
    //             //             );
    //             //         }
    //             //     }
    //             // }
    //             // res
    //         } else {
    //             // ====================================================
    //             // 找最快的节点，如果存在若干节点速度相差不大，找资源最空闲的节点
    //             // ====================================================
    //             let find_node = || {
    //                 self.algo_find_the_most_fast_node_for_fn(&parent_fns, filter)
    //                 .map_or_else(
    //                     || {
    //                         log::warn!("sim scale up for req{cur_req_i} fn{fn_id} failed");
    //                         None
    //                     },
    //                     |most_fast_node| {
    //                         log::info!(
    //                             "sim scale up for req{cur_req_i} fn{fn_id} to most fast node{most_fast_node}"
    //                         );
    //                         self.set_scale_up_result(fn_id, most_fast_node);
    //                         Some(most_fast_node)
    //                     },
    //                 )
    //             };
    //             let mut res = find_node();
    //             if res.is_none() && elem_if_need {
    //                 let all_node_lack_mem = self
    //                     .nodes
    //                     .borrow()
    //                     .iter()
    //                     .filter(|n| n.left_mem() >= CONTAINER_BASIC_MEM + NODE_LEFT_MEM_THRESHOLD)
    //                     .count()
    //                     == 0;
    //                 if all_node_lack_mem {
    //                     //select one fn container to scale_down
    //                     self.scale_down_rule_based(None);
    //                     res = find_node();
    //                     if res.is_none() {
    //                         log::warn!(
    //                             "sim scale up for req{cur_req_i} fn{fn_id} failed after elem"
    //                         );
    //                     }
    //                 }
    //             }
    //             res
    //         }
    //     } else {
    //         log::warn!("failed to expand because no request");
    //         None
    //     }
    // }
}
