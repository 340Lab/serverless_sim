use crate::{
    fn_dag::FnId,
    node::{Node, NodeId},
    sim_env::SimEnv,
    SPEED_SIMILAR_THRESHOLD,
};

pub trait ScaleExecutor {
    fn scale_down(&mut self, sim_env: &SimEnv, opt: ScaleOption);

    /// return success scale up cnt
    fn scale_up(&mut self, sim_env: &SimEnv, fnid: FnId, scale_cnt: usize) -> usize;
}

pub enum ScaleOption {
    /// scale cnt
    NoSpec(usize),
    /// fnid - scale cnt
    ForSpecFn(FnId, usize),
    /// nodeid - scale cnt
    ForSpecNode(NodeId, usize),
}

impl ScaleOption {
    fn scale_cnt(&self) -> usize {
        match self {
            ScaleOption::ForSpecFn(_, scale_cnt) => *scale_cnt,
            ScaleOption::ForSpecNode(_, scale_cnt) => *scale_cnt,
            ScaleOption::NoSpec(scale_cnt) => *scale_cnt,
        }
    }

    pub fn new() -> Self {
        ScaleOption::NoSpec(1)
    }

    pub fn for_spec_fn(mut self, spec_fn: FnId) -> Self {
        let scale_cnt = self.scale_cnt();
        ScaleOption::ForSpecFn(spec_fn, scale_cnt)
    }

    pub fn for_spec_node(mut self, spec_node: NodeId) -> Self {
        let scale_cnt = self.scale_cnt();
        ScaleOption::ForSpecNode(spec_node, scale_cnt)
    }

    pub fn with_scale_cnt(mut self, scale_cnt: usize) -> Self {
        assert!(scale_cnt > 0);
        match self {
            ScaleOption::NoSpec(_) => ScaleOption::NoSpec(scale_cnt),
            ScaleOption::ForSpecFn(fnid, _) => ScaleOption::ForSpecFn(fnid, scale_cnt),
            ScaleOption::ForSpecNode(nodeid, _) => ScaleOption::ForSpecNode(nodeid, scale_cnt),
        }
    }
}

pub struct DefaultScaleExecutor;

impl DefaultScaleExecutor {
    fn collect_idle_containers(&self, sim_env: &SimEnv) -> Vec<(NodeId, FnId)> {
        let mut idle_container_node_fn = Vec::new();

        for n in sim_env.nodes.borrow().iter() {
            for (fnid, fn_ct) in n.fn_containers.iter() {
                if fn_ct.is_idle() {
                    idle_container_node_fn.push((n.node_id(), *fnid));
                }
            }
        }

        idle_container_node_fn
    }

    fn scale_down_no_spec(&mut self, sim_env: &SimEnv, mut scale_cnt: usize) {
        let collect_idle_containers = self.collect_idle_containers(sim_env);
        if collect_idle_containers.len() < scale_cnt {
            log::warn!(
                "scale down has failed partly, target:{scale_cnt}, actual:{}",
                collect_idle_containers.len()
            );
            scale_cnt = collect_idle_containers.len();
        }

        for (nodeid, fnid) in collect_idle_containers[0..scale_cnt].iter() {
            sim_env.set_scale_down_result(*nodeid, *fnid);
        }
    }

    fn scale_down_for_fn(&mut self, sim_env: &SimEnv, fnid: FnId, mut scale_cnt: usize) {
        let mut collect_idle_containers = self.collect_idle_containers(sim_env);
        collect_idle_containers.retain(|(nodeid, fnid)| fnid == fnid);

        if collect_idle_containers.len() < scale_cnt {
            log::warn!(
                "scale down for spec fn {fnid} has failed partly, target:{scale_cnt}, actual:{}",
                collect_idle_containers.len()
            );
            scale_cnt = collect_idle_containers.len();
        }
        for (nodeid, fnid) in collect_idle_containers[0..scale_cnt].iter() {
            sim_env.set_scale_down_result(*fnid, *nodeid);
        }
    }

    fn scale_down_for_node(&mut self, sim_env: &SimEnv, nodeid: NodeId, mut scale_cnt: usize) {
        let mut collect_idle_containers = self.collect_idle_containers(sim_env);
        collect_idle_containers.retain(|(nodeid, fnid)| nodeid == nodeid);

        if collect_idle_containers.len() < scale_cnt {
            log::warn!(
                "scale down for spec node {nodeid} has failed partly, target:{scale_cnt}, actual:{}",collect_idle_containers.len()
            );
            scale_cnt = collect_idle_containers.len();
        }
        for (nodeid, fnid) in collect_idle_containers[0..scale_cnt].iter() {
            sim_env.set_scale_down_result(*fnid, *nodeid);
        }
    }

    fn scale_up_fn_to_nodes(&self,sim_env:&SimEnv,fnid:FnId,nodes:&[NodeId])->usize{
        let mut really_scale_cnt = 0;
        for &nodeid in nodes {
            if sim_env.node(nodeid).mem_enough_for_container(&sim_env.func(fnid)){
                sim_env.set_scale_up_result(fnid, nodeid);
                really_scale_cnt+=1;
            }else{
                break;
            }
        }
        really_scale_cnt
    }

    fn scale_up_to_most_resource_node(
        &mut self,
        sim_env: &SimEnv,
        fnid: FnId,
        mut scale_cnt: usize,
    ) -> usize {
        let mut nodes = sim_env.nodes.borrow_mut();
        let mut nodes_no_container: Vec<NodeId> = nodes
            .iter()
            .filter(|n| {
                !n.fn_containers.contains_key(&fnid)
                    // 有足够内存用于运行容器
                    && n.left_mem_for_place_container() > sim_env.func(fnid).container_mem()
                    && n.left_mem_for_place_container() > sim_env.func(fnid).cold_start_container_mem_use
            })
            .map(|n| n.node_id())
            .collect();

        drop(nodes);

        if nodes_no_container.len() < scale_cnt {
            log::warn!(
                "scale up to most resource node has failed partly, target:{scale_cnt}, actual:{}",
                nodes_no_container.len()
            );
            
            scale_cnt=nodes_no_container.len()
        } else {
            nodes_no_container.sort_by(|n1, n2| sim_env.node(*n1).cmp_rsc_used(&sim_env.node(*n2)));
        }

        self.scale_up_fn_to_nodes(sim_env,fnid,&nodes_no_container[0..scale_cnt])
    }

    fn scale_up_to_communicate_less_node(
        &mut self,
        sim_env: &SimEnv,
        fn_id: FnId,
        mut scale_cnt: usize,
    ) -> usize {
        let mut node_2_recv_time = vec![];

        //计算节点到关联fn的传输时间，取最小的
        fn calc_node_2_rela_fn_commu_time(
            env: &SimEnv,
            node: &Node,
            rela_fn: FnId,
            parent_fn_data: Option<f32>,
        ) -> f32 {
            let env_fn_2_nodes = env.fn_2_nodes.borrow();
            let rela_fn_nodes = env_fn_2_nodes
                .get(&rela_fn)
                .expect("前驱fn一定已经被扩容了");
            if rela_fn_nodes.len() == 0 {
                return 0.0;
            }
            // 对于每一个fn都找最近的，如果存在一样快的fn实例，选择负载更低的node
            let fastest_node: NodeId = *rela_fn_nodes
                .iter()
                .min_by(|&&a, &&b| {
                    assert!(a < env.node_cnt());
                    assert!(b < env.node_cnt());
                    let speed_a = env.node_get_speed_btwn(a, node.node_id());
                    let speed_b = env.node_get_speed_btwn(b, node.node_id());

                    if (speed_a - speed_b).abs() < SPEED_SIMILAR_THRESHOLD {
                        // 如果速度相差不大,比较资源
                        env.nodes.borrow()[a].cmp_rsc_used(&env.nodes.borrow()[b])
                    } else {
                        speed_a.partial_cmp(&speed_b).unwrap()
                    }
                })
                .expect("父fn至少有一个fn实例");
            if let Some(parent_data) = parent_fn_data {
                parent_data / env.node_get_speed_btwn(fastest_node, node.node_id())
            } else {
                env.fns.borrow()[rela_fn].out_put_size
                    / env.node_get_speed_btwn(fastest_node, node.node_id())
            }
        }

        let parent_fns = sim_env.func(fn_id).parent_fns(sim_env);

        for node in sim_env.nodes.borrow().iter().filter(|n| {
            // 该节点没有该fn的实例, 才需要被扩容对应fn
            !n.fn_containers.contains_key(&fn_id)&&
            // 有足够内存用于运行容器
             n.left_mem_for_place_container() > sim_env.func(fn_id).container_mem() && 
             n.left_mem_for_place_container() > sim_env.func(fn_id).cold_start_container_mem_use
        }) {
            let mut total_time = 0.0;
            for parent_fn in &parent_fns {
                let parent_data = sim_env.func(*parent_fn).out_put_size;
                total_time +=
                    calc_node_2_rela_fn_commu_time(sim_env, node, *parent_fn, Some(parent_data));
            }
            // for child_fn in child_fns {
            //     speed += calc_node_2_rela_fn_transtime(self, node, *child_fn, None);
            // }
            node_2_recv_time.push((node.node_id(), total_time));
        }
        node_2_recv_time.sort_by(|a, b| {
            let a = a.1;
            let b = b.1;
            std::cmp::PartialOrd::partial_cmp(&a, &b).unwrap()
        });

        if scale_cnt > node_2_recv_time.len() {
            log::warn!(
                "scale up to communicate less node has failed partly, target:{scale_cnt}, actual:{}", node_2_recv_time.len() 
            );
            // for (nodeid, _) in node_2_recv_time.iter() {
            //     sim_env.set_scale_up_result(fn_id, *nodeid);
            // }
            scale_cnt=node_2_recv_time.len()
        } else {
            // for (nodeid, _) in node_2_recv_time[0..scale_cnt].iter() {
            //     sim_env.set_scale_up_result(fn_id, *nodeid);
            // }
            // scale_cnt
        }

        self.scale_up_fn_to_nodes(sim_env, fn_id, &node_2_recv_time.iter().map(|p|{p.0}).collect::<Vec<_>>()[0..scale_cnt])
    }
}

impl ScaleExecutor for DefaultScaleExecutor {
    fn scale_down(&mut self, sim_env: &SimEnv, opt: ScaleOption) {
        match opt {
            ScaleOption::NoSpec(scale_cnt) => {
                self.scale_down_no_spec(sim_env, scale_cnt);
            }
            ScaleOption::ForSpecFn(fnid, scale_cnt) => {
                self.scale_down_for_fn(sim_env, fnid, scale_cnt);
            }
            ScaleOption::ForSpecNode(nodeid, scale_cnt) => {
                self.scale_down_for_node(sim_env, nodeid, scale_cnt);
            }
        }
    }

    /// return success scale up cnt
    fn scale_up(&mut self, sim_env: &SimEnv, fnid: FnId, scale_cnt: usize) -> usize {
        // ====================================================
        // dag开头，扩容fn到资源最多的节点
        // ====================================================
        if sim_env.func(fnid).parent_fns(sim_env).is_empty() {
            self.scale_up_to_most_resource_node(sim_env, fnid, scale_cnt)
        }
        // ====================================================
        // dag中间，扩容fn到通信最少的节点
        // ====================================================
        else {
            self.scale_up_to_communicate_less_node(sim_env, fnid, scale_cnt)
        }
    }
}
