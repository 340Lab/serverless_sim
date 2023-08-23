use crate::{
    fn_dag::FnId,
    node::{Node, NodeId},
    sim_env::SimEnv,
    SPEED_SIMILAR_THRESHOLD,
};

impl SimEnv {
    /// return None if all nodes has the fn container
    pub fn algo_find_the_most_fast_node_for_req_fn<F>(
        &self,
        parent_fns_nodes: &Vec<(FnId, NodeId)>,
        node_filter: F, // child_fns: &Vec<FnId>,
    ) -> Option<NodeId>
    where
        F: Fn(&&Node) -> bool,
    {
        let mut from_node_speeds = vec![];

        //计算节点到关联fn的传输时间，取最小的
        fn calc_node_2_rela_fn_transtime(
            env: &SimEnv,
            cur_node: NodeId,
            parent_fn_node: NodeId,
            parent_fn_data: f32,
        ) -> f32 {
            parent_fn_data / env.node_get_speed_btwn(parent_fn_node, cur_node)
        }
        for node in self.nodes.borrow().iter().filter(node_filter) {
            let mut time_cost = 0.0;
            for &(p_fn, p_node) in parent_fns_nodes {
                let parent_data = self.func(p_fn).out_put_size;
                time_cost += calc_node_2_rela_fn_transtime(
                    self,
                    node.node_id(),
                    p_node,
                    self.func(p_fn).out_put_size,
                );
            }
            // for child_fn in child_fns {
            //     speed += calc_node_2_rela_fn_transtime(self, node, *child_fn, None);
            // }
            from_node_speeds.push((node.node_id(), time_cost));
        }
        from_node_speeds.sort_by(|a, b| {
            let a = a.1;
            let b = b.1;
            std::cmp::PartialOrd::partial_cmp(&a, &b).unwrap()
        });

        // 取出排序完后最开始的，即最快的
        from_node_speeds.first().map(|node_id_speed| {
            assert!(node_id_speed.0 < self.node_cnt());
            node_id_speed.0
        })
    }
    // /// return None if all nodes has the fn container
    // pub fn algo_find_the_most_fast_node_for_fn<F>(
    //     &self,
    //     parent_fns: &Vec<FnId>,
    //     filter: F, // child_fns: &Vec<FnId>,
    // ) -> Option<NodeId>
    // where
    //     F: Fn(&&Node) -> bool,
    // {
    //     let mut from_node_speeds = vec![];

    //     //计算节点到关联fn的传输时间，取最小的
    //     fn calc_node_2_rela_fn_transtime(
    //         env: &SimEnv,
    //         node: &Node,
    //         rela_fn: FnId,
    //         parent_fn_data: Option<f32>,
    //     ) -> f32 {
    //         let env_fn_2_nodes = env.fn_2_nodes.borrow();
    //         let rela_fn_nodes = env_fn_2_nodes
    //             .get(&rela_fn)
    //             .expect("前驱fn一定已经被扩容了");
    //         // 对于每一个fn都找最近的，如果存在一样快的fn实例，选择负载更低的node
    //         let fastest_node: NodeId = *rela_fn_nodes
    //             .iter()
    //             .min_by(|&&a, &&b| {
    //                 assert!(a < env.node_cnt());
    //                 assert!(b < env.node_cnt());
    //                 let speed_a = env.node_get_speed_btwn(a, node.node_id());
    //                 let speed_b = env.node_get_speed_btwn(b, node.node_id());

    //                 if (speed_a - speed_b).abs() < SPEED_SIMILAR_THRESHOLD {
    //                     // 如果速度相差不大,比较资源
    //                     env.nodes.borrow()[a].cmp_rsc_used(&env.nodes.borrow()[b])
    //                 } else {
    //                     speed_a.partial_cmp(&speed_b).unwrap()
    //                 }
    //             })
    //             .expect("父fn至少有一个fn实例");
    //         if let Some(parent_data) = parent_fn_data {
    //             parent_data / env.node_get_speed_btwn(fastest_node, node.node_id())
    //         } else {
    //             env.fns.borrow()[rela_fn].out_put_size
    //                 / env.node_get_speed_btwn(fastest_node, node.node_id())
    //         }
    //     }
    //     for node in self.nodes.borrow().iter().filter(filter) {
    //         let mut speed = 0.0;
    //         for parent_fn in parent_fns {
    //             let parent_data = self.fns.borrow()[*parent_fn].out_put_size;
    //             speed += calc_node_2_rela_fn_transtime(self, node, *parent_fn, Some(parent_data));
    //         }
    //         // for child_fn in child_fns {
    //         //     speed += calc_node_2_rela_fn_transtime(self, node, *child_fn, None);
    //         // }
    //         from_node_speeds.push((node.node_id(), speed));
    //     }
    //     from_node_speeds.sort_by(|a, b| {
    //         let a = a.1;
    //         let b = b.1;
    //         std::cmp::PartialOrd::partial_cmp(&a, &b).unwrap()
    //     });

    //     // 取出排序完后最开始的，即最快的
    //     from_node_speeds.first().map(|node_id_speed| {
    //         assert!(node_id_speed.0 < self.node_cnt());
    //         node_id_speed.0
    //     })
    // }

    ///找到有对应容器的，资源最空闲的节点
    pub fn algo_find_the_most_idle_node_for_fn(&self, fnid: FnId) -> Option<NodeId> {
        let env_fn_2_nodes = self.fn_2_nodes.borrow();
        let fn_nodes = env_fn_2_nodes.get(&fnid).unwrap();
        // let mut node_id = *fn_nodes.iter().next().unwrap();

        // for fn_node in fn_nodes {
        //     // 选出资源占用最小的
        //     if self.nodes[*fn_node].cmp_rsc(&self.nodes[node_id]).is_lt() {
        //         node_id = *fn_node;
        //     }
        // }

        // node_id
        let res = fn_nodes
            .iter()
            .min_by(|a, b| self.nodes.borrow()[**a].cmp_rsc_used(&self.nodes.borrow()[**b]))
            .map(|v| *v);

        res
    }

    pub fn algo_find_the_most_idle_node<F: FnMut(&&Node) -> bool>(
        &self,
        filter: F,
    ) -> Option<NodeId> {
        let res = self
            .nodes
            .borrow()
            .iter()
            .filter(filter)
            .min_by(|a, b| a.cmp_rsc_used(b))
            .map(|n| n.node_id());
        res
    }
}
