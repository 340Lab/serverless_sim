use std::collections::HashSet;

use crate::{ fn_dag::{ FnContainer, FnContainerState, FnId }, node::{ NodeId }, sim_env::SimEnv };

impl SimEnv {
    pub fn set_scale_down_result(&self, fnid: FnId, nodeid: NodeId) {
        // log::info!("scale down fn {fnid} from node {nodeid}");
        let cont = self.nodes.borrow_mut()[nodeid].fn_containers.remove(&fnid).unwrap();
        self.fn_2_nodes.borrow_mut().get_mut(&fnid).unwrap().remove(&nodeid);
        match cont.state() {
            FnContainerState::Starting { .. } => {
                self.node_mut(nodeid).mem -= self.func(fnid).cold_start_container_mem_use;
            }
            FnContainerState::Running => {
                self.node_mut(nodeid).mem -= self.func(fnid).container_mem();
            }
        }
    }

    pub fn set_scale_up_result(&self, fn_id: FnId, node_id: NodeId) {
        // log::info!("expand fn: {fn_id} to node: {node_id}");
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

        self.nodes
            .borrow_mut()
            [node_id].fn_containers.entry(fn_id)
            .and_modify(|_| panic!("fn container already exists"))
            .or_insert(FnContainer::new(fn_id, node_id, self));

        self.nodes.borrow_mut()[node_id].mem += self.func(fn_id).cold_start_container_mem_use;
    }
}
