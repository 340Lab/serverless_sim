use std::{
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use crate::{
    fn_dag::{FnContainer, FnId, Func},
    request::ReqId,
    sim_env::SimEnv,
    util, NODE_CNT, NODE_LEFT_MEM_THRESHOLD, NODE_SCORE_CPU_WEIGHT, NODE_SCORE_MEM_WEIGHT,
};

pub type NodeId = usize;

pub struct NodeRscLimit {
    pub cpu: f32,
    pub mem: f32,
}

pub struct Node {
    node_id: NodeId,
    // #数据库容器
    // # databases

    // # #函数容器
    // # functions

    // # #serverless总控节点
    // # serverless_controller

    // #资源限制：cpu, mem
    pub rsc_limit: NodeRscLimit,

    pending_tasks: RefCell<BTreeSet<(ReqId, FnId)>>,

    pub fn_containers: RefCell<HashMap<FnId, FnContainer>>,

    // 使用了的cpu
    pub cpu: f32,

    // 使用了的内存
    pub mem: RefCell<f32>,

    pub last_frame_cpu: f32,

    pub frame_run_count: usize,
}

impl Node {
    pub fn mem(&self) -> f32 {
        *self.mem.borrow()
    }
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            rsc_limit: NodeRscLimit {
                cpu: 1000.0,
                mem: 8000.0,
            },
            fn_containers: HashMap::new().into(),
            cpu: 0.0,
            mem: 0.0.into(),
            last_frame_cpu: 0.0,
            frame_run_count: 0,
            pending_tasks: BTreeSet::new().into(),
        }
    }
    pub fn add_task(&self, req_id: ReqId, fn_id: FnId) {
        self.pending_tasks.borrow_mut().insert((req_id, fn_id));
    }
    pub fn left_mem(&self) -> f32 {
        self.rsc_limit.mem - self.mem()
    }
    pub fn left_mem_for_place_container(&self) -> f32 {
        self.rsc_limit.mem - self.mem() - NODE_LEFT_MEM_THRESHOLD
    }
    pub fn mem_enough_for_container(&self, func: &Func) -> bool {
        self.left_mem_for_place_container() > func.cold_start_container_mem_use
            && self.left_mem_for_place_container() > func.container_mem()
    }
    pub fn node_id(&self) -> NodeId {
        assert!(self.node_id < NODE_CNT);
        self.node_id
    }
    pub fn cmp_rsc_used(&self, other: &Self) -> Ordering {
        (self.cpu * NODE_SCORE_CPU_WEIGHT + self.mem() * NODE_SCORE_MEM_WEIGHT)
            .partial_cmp(&(other.cpu * NODE_SCORE_CPU_WEIGHT + other.mem() * NODE_SCORE_MEM_WEIGHT))
            .unwrap()
    }

    pub fn all_task_cnt(&self) -> usize {
        self.pending_task_cnt() + self.running_task_cnt()
    }

    pub fn pending_task_cnt(&self) -> usize {
        self.pending_tasks.borrow().len()
    }

    pub fn running_task_cnt(&self) -> usize {
        self.fn_containers
            .borrow()
            .iter()
            .map(|(_, c)| c.req_fn_state.len())
            .sum()
    }

    pub fn container_mut<'a>(&'a self, fnid: FnId) -> Option<RefMut<'a, FnContainer>> {
        let b = self.fn_containers.borrow_mut();
        if !b.contains_key(&fnid) {
            return None;
        }
        let res = RefMut::map(b, |map| {
            map.get_mut(&fnid)
                .unwrap_or_else(|| panic!("container {} not found", fnid))
        });
        Some(res)
        // .get_mut(&fnid)
    }
    pub fn container<'a>(&'a self, fnid: FnId) -> Option<Ref<'a, FnContainer>> {
        let b = self.fn_containers.borrow();
        if !b.contains_key(&fnid) {
            return None;
        }
        let res = Ref::map(b, |map| {
            map.get(&fnid)
                .unwrap_or_else(|| panic!("container {} not found", fnid))
        });
        Some(res)
        // .get_mut(&fnid)
    }
    // pub fn container<'a>(&'a self, fnid: FnId) -> Option<&'a FnContainer> {
    //     self.fn_containers.get(&fnid)
    // }

    pub fn try_load_spec_container(&self, fnid: FnId, env: &SimEnv) {
        if self.container(fnid).is_none() {
            // try cold start
            if self.mem_enough_for_container(&env.func(fnid)) {
                let fncon = FnContainer::new(fnid, self.node_id(), env);
                let con_mem_take = fncon.mem_take(env);
                self.fn_containers.borrow_mut().insert(fnid, fncon);
                // log::info!("expand fn: {fn_id} to node: {node_id}");
                // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
                let node_id = self.node_id();
                env.fn_2_nodes
                    .borrow_mut()
                    .entry(fnid)
                    .and_modify(|v| {
                        v.insert(node_id);
                    })
                    .or_insert_with(|| {
                        let mut set = HashSet::new();
                        set.insert(node_id);
                        set
                    });

                // will recalc next frame begin
                // but we need to add mem to node in this frame because it's new container
                *self.mem.borrow_mut() += con_mem_take;
                // self.nodes.borrow_mut()[node_id].mem +=
                //     self.func(fn_id).cold_start_container_mem_use;
            }
        }
    }

    pub fn load_container(&self, env: &SimEnv) {
        let mut removed_pending = vec![];
        for &(req_id, fnid) in self.pending_tasks.borrow().iter() {
            self.try_load_spec_container(fnid, env);

            if let Some(mut fncon) = self.container_mut(fnid) {
                // add to container
                fncon.req_fn_state.insert(
                    req_id,
                    env.fn_new_fn_running_state(&env.request(req_id), fnid),
                );
                removed_pending.push((req_id, fnid));
            }
        }
        for r in removed_pending {
            self.pending_tasks.borrow_mut().remove(&r);
        }
    }
}

impl SimEnv {
    pub fn node_init_node_graph(&self) {
        fn _init_one_node(env: &SimEnv, node_id: NodeId) {
            let node = Node::new(node_id);
            // let node_i = nodecnt;
            env.nodes.borrow_mut().push(node);

            let nodecnt: usize = env.nodes.borrow().len();

            for i in 0..nodecnt - 1 {
                let randspeed = env.env_rand_f(8000.0, 10000.0);
                env.node_set_speed_btwn(i, nodecnt - 1, randspeed);
            }
        }

        // # init nodes graph
        let dim = NODE_CNT;
        *self.node2node_connection_count.borrow_mut() = vec![vec![0; dim]; dim];
        *self.node2node_graph.borrow_mut() = vec![vec![0.0; dim]; dim];
        for i in 0..dim {
            _init_one_node(self, i);
        }

        log::info!("node speed graph: {:?}", self.node2node_graph.borrow());
    }

    /// 设置节点间网速
    /// - speed: MB/s
    fn node_set_speed_btwn(&self, n1: usize, n2: usize, speed: f32) {
        assert!(n1 != n2);
        fn _set_speed_btwn(env: &SimEnv, nbig: usize, nsmall: usize, speed: f32) {
            env.node2node_graph.borrow_mut()[nbig][nsmall] = speed;
        }
        if n1 > n2 {
            _set_speed_btwn(self, n1, n2, speed);
        } else {
            _set_speed_btwn(self, n2, n1, speed);
        }
    }

    /// 获取节点间网速
    /// - speed: MB/s
    pub fn node_get_speed_btwn(&self, n1: NodeId, n2: NodeId) -> f32 {
        let _get_speed_btwn =
            |nbig: usize, nsmall: usize| self.node2node_graph.borrow()[nbig][nsmall];
        if n1 > n2 {
            _get_speed_btwn(n1, n2)
        } else {
            _get_speed_btwn(n2, n1)
        }
    }

    //获取计算速度最慢的节点
    pub fn node_get_lowest(&self) -> NodeId {
        let nodes = self.nodes.borrow();
        let res = nodes
            .iter()
            .min_by(|x, y| x.cpu.partial_cmp(&y.cpu).unwrap())
            .unwrap();
        res.node_id
    }

    //获取最低带宽
    pub fn node_btw_get_lowest(&self) -> f32 {
        let mut low_btw = None;

        for i in 0..self.nodes.borrow().len() {
            for j in i + 1..self.nodes.borrow().len() {
                let btw = self.node_get_speed_btwn(i, j);
                if let Some(low_btw_) = low_btw.take() {
                    low_btw = Some(btw.min(low_btw_));
                } else {
                    low_btw = Some(btw);
                }
            }
        }

        low_btw.unwrap()
    }

    pub fn node_set_connection_count_between(&self, n1: NodeId, n2: NodeId, count: usize) {
        let _set_connection_count_between = |nbig: usize, nsmall: usize, count: usize| {
            self.node2node_connection_count.borrow_mut()[nbig][nsmall] = count;
        };
        if n1 > n2 {
            _set_connection_count_between(n1, n2, count);
        } else {
            _set_connection_count_between(n2, n1, count);
        }
    }

    pub fn node_get_connection_count_between(&self, n1: NodeId, n2: NodeId) -> usize {
        let _get_connection_count_between =
            |nbig: usize, nsmall: usize| self.node2node_connection_count.borrow()[nbig][nsmall];
        if n1 > n2 {
            _get_connection_count_between(n1, n2)
        } else {
            _get_connection_count_between(n2, n1)
        }
    }

    pub fn node_get_connection_count_between_by_offerd_graph(
        &self,
        n1: NodeId,
        n2: NodeId,
        offerd: &Vec<Vec<usize>>,
    ) -> usize {
        let _get_connection_count_between = |nbig: usize, nsmall: usize| offerd[nbig][nsmall];
        if n1 > n2 {
            _get_connection_count_between(n1, n2)
        } else {
            _get_connection_count_between(n2, n1)
        }
    }

    pub fn node_set_connection_count_between_by_offerd_graph(
        &self,
        n1: NodeId,
        n2: NodeId,
        count: usize,
        offerd: &mut Vec<Vec<usize>>,
    ) {
        let mut _set_connection_count_between = |nbig: usize, nsmall: usize, count: usize| {
            offerd[nbig][nsmall] = count;
        };
        if n1 > n2 {
            _set_connection_count_between(n1, n2, count);
        } else {
            _set_connection_count_between(n2, n1, count);
        }
    }

    pub fn node_cnt(&self) -> usize {
        self.nodes.borrow().len()
    }

    pub fn nodes<'a>(&'a self) -> Ref<'a, Vec<Node>> {
        self.nodes.borrow()
    }

    pub fn nodes_mut<'a>(&'a self) -> RefMut<'a, Vec<Node>> {
        self.nodes.borrow_mut()
    }

    pub fn node<'a>(&'a self, i: NodeId) -> Ref<'a, Node> {
        let b = self.nodes.borrow();

        Ref::map(b, |vec| &vec[i])
    }

    pub fn node_mut<'a>(&'a self, i: NodeId) -> RefMut<'a, Node> {
        let b = self.nodes.borrow_mut();

        RefMut::map(b, |vec| &mut vec[i])
    }
}
