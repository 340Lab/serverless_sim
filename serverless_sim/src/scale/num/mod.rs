pub mod ai;
pub mod down_filter;
pub mod hpa;
pub mod lass;
pub mod no;

use crate::{
    actions::ESActionWrapper,
    algos::ContainerMetric,
    config::Config,
    fn_dag::{FnContainer, FnContainerState, FnId},
    node::NodeId,
    sim_env::SimEnv,
};
use std::{
    cell::{Ref, RefMut},
    collections::HashSet,
};

use self::{ai::AIScaleNum, hpa::HpaScaleNum, lass::LassScaleNum, no::NoScaleNum};

pub trait ScaleNum: Send {
    /// return (action, action_is_done)
    /// - action_is_done: need prepare next state and wait for new action
    fn scale_for_fn(
        &mut self,
        env: &SimEnv,
        fnid: FnId,
        metric: &ContainerMetric,
        action: &ESActionWrapper,
    ) -> (f32, bool);

    fn fn_available_count(&self, fnid: FnId, env: &SimEnv) -> usize;
}

pub const SCALE_NUM_NAMES: [&'static str; 4] = ["ai", "no", "hpa", "lass"];

pub fn new_scale_num(c: &Config) -> Option<Box<dyn ScaleNum + Send>> {
    let es = &c.es;
    let (scale_num_name, scale_num_attr) = es.scale_num_conf();

    match &*scale_num_name {
        "ai" => {
            return Some(Box::new(AIScaleNum::new()));
        }
        "no" => {
            return Some(Box::new(NoScaleNum::new()));
        }
        "hpa" => {
            return Some(Box::new(HpaScaleNum::new()));
        }
        "lass" => {
            return Some(Box::new(LassScaleNum::new()));
        }
        _ => {
            return None;
        }
    }
}

impl SimEnv {
    pub fn spec_scaler_mut<'a>(&'a self) -> RefMut<'a, Box<dyn ScaleNum + Send>> {
        let r = self.mechanisms.spec_scale_num_mut();
        RefMut::map(r, |map| map.as_mut().unwrap())
    }

    pub fn spec_scaler<'a>(&'a self) -> Ref<'a, Box<dyn ScaleNum + Send>> {
        let r = self.mechanisms.spec_scale_num();
        Ref::map(r, |map| map.as_ref().unwrap())
    }

    pub fn set_scale_down_result(&self, fnid: FnId, nodeid: NodeId) {
        // log::info!("scale down fn {fnid} from node {nodeid}");
        let cont = self.core.nodes_mut()[nodeid]
            .fn_containers
            .borrow_mut()
            .remove(&fnid)
            .unwrap();
        self.core
            .fn_2_nodes_mut()
            .get_mut(&fnid)
            .unwrap()
            .remove(&nodeid);
        match cont.state() {
            FnContainerState::Starting { .. } => {
                *self.node_mut(nodeid).mem.borrow_mut() -=
                    self.func(fnid).cold_start_container_mem_use;
            }
            FnContainerState::Running => {
                *self.node_mut(nodeid).mem.borrow_mut() -= self.func(fnid).container_mem();
            }
        }
    }

    // pub fn set_scale_up_result(&self, fn_id: FnId, node_id: NodeId) {
    //     // log::info!("expand fn: {fn_id} to node: {node_id}");
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

    //     self.nodes.borrow_mut()[node_id]
    //         .fn_containers
    //         .borrow_mut()
    //         .entry(fn_id)
    //         .and_modify(|_| panic!("fn container already exists"))
    //         .or_insert(FnContainer::new(fn_id, node_id, self));

    //     self.nodes.borrow_mut()[node_id].mem += self.func(fn_id).cold_start_container_mem_use;
    // }
}
