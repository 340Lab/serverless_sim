use std::collections::HashSet;

use crate::{
    actions::Action,
    fn_dag::{FnContainer, FnId},
    node::NodeId,
    sim_env::SimEnv,
};

impl SimEnv {
    fn get_action_from_model(&mut self) {}

    /// 从模型拿到action，然后执行对应步骤
    pub fn scale(&mut self) {
        let action = Action::ExpandGreedy;
        match action {
            Action::ExpandGreedy => self.expand_rule_based(),
            Action::ExpandRandom => self.expand_random(),
            Action::ShrinkRandom => self.shrink_random(),
            Action::ShrinkRuleBased => self.shrink_rule_based(),
            Action::DoNothing => {}
        }
    }

    pub fn set_expand_result(&mut self, fn_id: FnId, node_id: NodeId) {
        // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
        self.fn_2_nodes
            .entry(node_id)
            .and_modify(|v| {
                v.insert(fn_id);
            })
            .or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(fn_id);
                set
            });

        self.nodes[node_id]
            .fn_containers
            .insert(fn_id, FnContainer::new(fn_id));
    }

    pub fn get_request_fn_for_expand(&mut self) -> (,FnId) {
        // 1. 从请求队列中拿到一个请求
        let req = self.req_queue.pop_front().unwrap();
        // 2. 从请求中拿到一个fn
        let fn_id = req.fn_id;
        // 3. 从fn中拿到一个node
        let node_id = self.fns[fn_id].nodes.iter().next().unwrap().clone();
        // 4. 返回fn和node
        (fn_id, node_id)
    }

    fn expand_random(&mut self){

    }

    fn shrink_random(&mut self){

    }
    
    fn shrink_rule_based(&mut self){

    }

    // 针对最前面的请求的某一个fn，进行扩容
    // 一个fn从0->1扩容和1-n扩容，所得的分应该是不同的, 0->1更重要
    fn expand_rule_based(&mut self) {
        let fn_id = self.dags[cur_req_dag_i].dag[fn_node_id];
        let parents = self.dags[cur_req_dag_i].dag.parents(fn_node_id);
        let children = self.dags[cur_req_dag_i].dag.children(fn_node_id);
        let mut parent_fns: Vec<FnId> = vec![];
        let mut child_fns: Vec<FnId> = vec![];
        for (_edge_i, node_i) in parents.iter(&self.dags[cur_req_dag_i].dag) {
            parent_fns.push(self.dags[cur_req_dag_i].dag[node_i]);
        }
        for (_edge_i, node_i) in children.iter(&self.dags[cur_req_dag_i].dag) {
            child_fns.push(self.dags[cur_req_dag_i].dag[node_i]);
        }
        if parent_fns.len() == 0 && child_fns.len() == 0 {
            // ====================================================
            // 扩容fn到资源最多的节点
            // ====================================================
            let most_idle_node: NodeId = self.find_the_most_idle_node();
            self.set_expand_result(fn_id, most_idle_node);
            most_idle_node
        } else {
            // ====================================================
            // 找最快的节点，如果存在若干节点速度相差不大，找资源最空闲的节点
            // ====================================================
            let most_fast_node: NodeId =
                self.find_the_most_fast_node_for_fn(&parent_fns, &child_fns);
            self.set_expand_result(fn_id, most_fast_node);
            most_fast_node
        }
    }
}
