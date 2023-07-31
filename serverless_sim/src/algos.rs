use crate::{fn_dag::FnId, node::NodeId, sim_env::SimEnv, SPEED_SIMILAR_THRESHOLD};

impl SimEnv {
    pub fn algo_find_the_most_fast_node_for_fn(
        &self,
        parent_fns: &Vec<FnId>,
        child_fns: &Vec<FnId>,
    ) -> NodeId {
        let mut from_node_speeds = vec![];

        //计算节点到关联fn的传输时间，取最小的
        fn calc_node_2_rela_fn_transtime(
            env: &SimEnv,
            node: &Node,
            rela_fn: FnId,
            parent_fn_data: Option<f32>,
        ) -> f32 {
            let rela_fn_nodes = env
                .fn_2_nodes
                .get(&rela_fn)
                .expect("前驱fn一定已经被扩容了");
            // 对于每一个fn都找最近的，如果存在一样快的fn实例，选择负载更低的node
            let fastest_node: NodeId = *rela_fn_nodes
                .iter()
                .min_by(|&&a, &&b| {
                    let speed_a = env.node_ops().get_speed_btwn(a, node.node_id);
                    let speed_b = env.node_ops().get_speed_btwn(b, node.node_id);

                    if (speed_a - speed_b).abs() < SPEED_SIMILAR_THRESHOLD {
                        // 如果速度相差不大,比较资源
                        env.nodes[a].cmp_rsc(&env.nodes[b])
                    } else {
                        speed_a.partial_cmp(&speed_b).unwrap()
                    }
                })
                .expect("父fn至少有一个fn实例");
            if let Some(parent_data) = parent_fn_data {
                parent_data / env.node_ops().get_speed_btwn(fastest_node, node.node_id)
            } else {
                env.fns[rela_fn].out_put_size
                    / env.node_ops().get_speed_btwn(fastest_node, node.node_id)
            }
        }
        for node in &self.nodes {
            let mut speed = 0.0;
            for parent_fn in parent_fns {
                let parent_data = self.fns[*parent_fn].out_put_size;
                speed += calc_node_2_rela_fn_transtime(self, node, *parent_fn, Some(parent_data));
            }
            for child_fn in child_fns {
                speed += calc_node_2_rela_fn_transtime(self, node, *child_fn, None);
            }
            from_node_speeds.push((node.node_id, speed));
        }
        from_node_speeds.sort_by(|a, b| a.1.cmp(&b.1));

        // 取出排序完后最开始的，即最快的
        from_node_speeds.first().unwrap().0
    }

    ///找到有对应容器的，资源最空闲的节点
    pub fn algo_find_the_most_idle_node_for_fn(&self, fnid: FnId) -> NodeId {
        let fn_nodes = self.node_ops().get_fn_relate_nodes(&fnid).unwrap();
        // let mut node_id = *fn_nodes.iter().next().unwrap();

        // for fn_node in fn_nodes {
        //     // 选出资源占用最小的
        //     if self.nodes[*fn_node].cmp_rsc(&self.nodes[node_id]).is_lt() {
        //         node_id = *fn_node;
        //     }
        // }

        // node_id
        fn_nodes
            .iter()
            .min_by(|a, b| self.nodes[*a].cmp_rsc(&self.nodes[*b]))
            .unwrap()
    }

    pub fn algo_find_the_most_idle_node(&self) -> NodeId {
        self.nodes
            .iter()
            .min_by(|a, b| a.cmp_rsc(b))
            .unwrap()
            .node_id
    }
}
