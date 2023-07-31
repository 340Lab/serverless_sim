use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crate::{
    fn_dag::{FnContainer, FnId},
    sim_env::SimEnv,
    util,
};

pub type NodeId = usize;

pub struct NodeRscLimit {
    pub cpu: f32,
    pub mem: f32,
}

pub struct Node {
    pub node_id: NodeId,
    // #数据库容器
    // # databases

    // # #函数容器
    // # functions

    // # #serverless总控节点
    // # serverless_controller

    // #资源限制：cpu, mem
    pub rsc_limit: NodeRscLimit,

    pub fn_containers: HashMap<FnId, FnContainer>,

    pub cpu: f32,

    pub mem: f32,
}

impl Node {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            rsc_limit: NodeRscLimit {
                cpu: 1000.0,
                mem: 1000.0,
            },
            fn_containers: HashMap::new(),
            cpu: 0.0,
            mem: 0.0,
        }
    }

    pub fn cmp_rsc(&self, other: &Self) -> Ordering {
        const CPU_SCORE_WEIGHT: f32 = 0.5;
        const MEM_SCORE_WEIGHT: f32 = 0.5;
        if self.cpu * CPU_SCORE_WEIGHT + self.mem * MEM_SCORE_WEIGHT
            > other.cpu * CPU_SCORE_WEIGHT + other.mem * MEM_SCORE_WEIGHT
        {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

pub struct SimEnvNodeOps<'a> {
    pub env: &'a SimEnv,
}

impl SimEnvNodeOps<'_> {
    pub fn init_node_graph(&mut self) {
        fn _init_one_node(env: &mut SimEnv, node_id: NodeId) {
            let nodecnt: usize = env.nodes.len();
            let node = Node::new(node_id);
            // let node_i = nodecnt;
            env.nodes.push(node);

            let nodecnt: usize = env.nodes.len();
            println!("nodecnt {}", nodecnt);

            for i in 0..nodecnt - 1 {
                let randspeed = util::rand_f(8000.0, 10000.0);
                env.node_ops().set_speed_btwn(i, nodecnt - 1, randspeed);
            }
        }

        // # init nodes graph
        let dim = 10;
        self.env.node2node_graph = vec![vec![0.0; 10]; 10];
        for i in 0..dim {
            _init_one_node(&mut self.env, i);
        }
    }

    /// 设置节点间网速
    /// - speed: MB/s
    fn set_speed_btwn(&mut self, n1: usize, n2: usize, speed: f32) {
        assert!(n1 != n2);
        fn _set_speed_btwn(env: &mut SimEnv, nbig: usize, nsmall: usize, speed: f32) {
            env.node2node_graph[nbig][nsmall] = speed;
        }
        if n1 > n2 {
            _set_speed_btwn(&mut self.env, n1, n2, speed);
        } else {
            _set_speed_btwn(&mut self.env, n2, n1, speed);
        }
    }

    /// 获取节点间网速
    /// - speed: MB/s
    pub fn get_speed_btwn(&self, n1: NodeId, n2: NodeId) -> f32 {
        let _get_speed_btwn = |nbig: usize, nsmall: usize| self.env.node2node_graph[nbig][nsmall];
        if n1 > n2 {
            _get_speed_btwn(n1, n2)
        } else {
            _get_speed_btwn(n2, n1)
        }
    }

    pub fn get_fn_relate_nodes(&self, fnid: &FnId) -> Option<&HashSet<NodeId>> {
        let env = self.env;
        env.fn_2_nodes.get(fnid)
    }
}
