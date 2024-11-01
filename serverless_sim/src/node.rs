use crate::cache::no_evict::NoEvict;
use crate::cache::InstanceCachePolicy;
use crate::config::Config;
use crate::with_env_sub::WithEnvHelp;
use crate::{
    fn_dag::{EnvFnExt, FnContainer, FnContainerState, FnId, Func},
    mechanism::SimEnvObserve,
    request::ReqId,
    sim_env::SimEnv,
    with_env_sub::WithEnvCore,
    NODE_CNT, NODE_LEFT_MEM_THRESHOLD, NODE_SCORE_CPU_WEIGHT, NODE_SCORE_MEM_WEIGHT,
};
use std::ptr::NonNull;
use std::{
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

pub type NodeId = usize;

#[derive(Clone)]
pub struct NodeRscLimit {
    // 节点cpu上限
    pub cpu: f32,
    // 节点mem上限
    pub mem: f32,
}

// #[derive(Clone)]
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

    // 待处理的任务
    pending_tasks: RefCell<BTreeSet<(ReqId, FnId)>>,

    // 节点上已有的函数容器
    pub fn_containers: RefCell<HashMap<FnId, FnContainer>>,

    // 使用了的cpu
    pub cpu: f32,

    // 使用了的内存
    // 具体函数使用内存在算法执行后才计算, 算法中需要使用last_frame_mem
    mem: RefCell<f32>,

    // 上一帧使用的cpu
    pub last_frame_cpu: f32,

    // 上一帧使用的mem
    pub last_frame_mem: f32,

    pub frame_run_count: usize,

    //缓存置换策略
    instance_cache_policy: RefCell<Box<dyn InstanceCachePolicy<FnId>>>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            node_id: self.node_id,
            rsc_limit: self.rsc_limit.clone(),
            fn_containers: self.fn_containers.clone(),
            pending_tasks: self.pending_tasks.clone(),
            cpu: self.cpu,
            mem: self.mem.clone(),
            last_frame_cpu: self.last_frame_cpu,
            last_frame_mem: self.last_frame_mem,
            frame_run_count: self.frame_run_count,

            // never used, clone is for SimEnvObserve
            instance_cache_policy: RefCell::new(Box::new(NoEvict::new())),
        }
    }
}

impl Node {
    // 具体函数使用内存在算法执行后才计算, 算法中需要使用last_frame_mem
    // 返回已使用的mem量
    pub fn unready_mem_mut<'a>(&'a self) -> RefMut<'a, f32> {
        self.mem.borrow_mut()
    }
    // 具体函数使用内存在算法执行后才计算, 算法中需要使用last_frame_mem
    pub fn unready_mem(&self) -> f32 {
        *self.mem.borrow()
    }
    fn new(node_id: NodeId, config: &Config) -> Self {
        Self {
            node_id,
            rsc_limit: NodeRscLimit {
                cpu: 1000.0,
                // cpu: 200.0,
                mem: 8000.0,
            },
            fn_containers: HashMap::new().into(),
            cpu: 0.0,
            mem: (0.0).into(),
            last_frame_cpu: 0.0,
            frame_run_count: 0,
            pending_tasks: BTreeSet::new().into(),
            last_frame_mem: 0.0,
            instance_cache_policy: RefCell::new(config.mech.new_instance_cache_policy()),
        }
    }

    // 增加任务
    pub fn add_task(&self, req_id: ReqId, fn_id: FnId) {
        self.pending_tasks.borrow_mut().insert((req_id, fn_id));
    }

    pub fn unready_left_mem(&self) -> f32 {
        self.rsc_limit.mem - self.unready_mem()
    }

    // 返回剩余的mem量
    pub fn left_mem(&self) -> f32 {
        self.rsc_limit.mem - self.last_frame_mem
    }

    // 返回剩余的可用于部署容器的mem量
    pub fn left_mem_for_place_container(&self) -> f32 {
        self.rsc_limit.mem - self.unready_mem() - NODE_LEFT_MEM_THRESHOLD
    }

    // 判断剩余的可用于部署容器的mem量是否足够部署特定函数的容器
    pub fn mem_enough_for_container(&self, func: &Func) -> bool {
        self.left_mem_for_place_container() > func.cold_start_container_mem_use
            && self.left_mem_for_place_container() > func.container_mem()
    }
    pub fn node_id(&self) -> NodeId {
        assert!(self.node_id < NODE_CNT);
        self.node_id
    }

    // 比较两个节点的资源使用情况
    // pub enum Ordering {
    //     Less,
    //     Equal,
    //     Greater,
    // }
    pub fn cmp_rsc_used(&self, other: &Self) -> Ordering {
        (self.cpu * NODE_SCORE_CPU_WEIGHT + self.unready_mem() * NODE_SCORE_MEM_WEIGHT)
            .partial_cmp(
                &(other.cpu * NODE_SCORE_CPU_WEIGHT + other.unready_mem() * NODE_SCORE_MEM_WEIGHT),
            )
            .unwrap()
    }

    // 返回节点上所有任务（待处理和正在运行）的总数
    pub fn all_task_cnt(&self) -> usize {
        self.pending_task_cnt() + self.running_task_cnt()
    }

    // 返回节点上待处理任务的数量
    pub fn pending_task_cnt(&self) -> usize {
        self.pending_tasks.borrow().len()
    }

    // 返回节点上正在运行的任务数量
    pub fn running_task_cnt(&self) -> usize {
        self.fn_containers
            .borrow()
            .iter()
            .map(|(_, c)| c.req_fn_state.len())
            .sum()
    }

    // 返回指定函数ID的容器的可变引用
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

    // 返回指定函数ID的容器的不可变引用
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

    pub fn try_unload_container(&self, fnid: FnId, env: &SimEnv, if_down: bool) {
        // log::info!("scale down fn {fnid} from node {}", self.node_id());
        // env.set_scale_down_result(fnid, self.node_id());

        let nodeid = self.node_id();
        let Some(cont) = self.fn_containers.borrow_mut().remove(&fnid) else {
            log::info!("try_unload_container not found {}", fnid);
            return;
        };

        //是主动缩容则要主动移除
        if if_down {
            assert!(self.instance_cache_policy.borrow_mut().remove_all(&fnid));
        }

        env.core
            .fn_2_nodes_mut()
            .get_mut(&fnid)
            .unwrap()
            .remove(&nodeid);
        match cont.state() {
            FnContainerState::Starting { .. } => {
                *self.mem.borrow_mut() -= env.func(fnid).cold_start_container_mem_use;
            }
            FnContainerState::Running => {
                *self.mem.borrow_mut() -= env.func(fnid).container_mem();
            }
        }
        // let fncon = self.fn_containers.borrow_mut().remove(&fnid).unwrap();
        // let con_mem_take = fncon.mem_take(env);
        // // log::info!("unload fn: {fn_id} from node: {node_id}");
        // // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
        // let node_id = self.node_id();
        // env.core.fn_2_nodes_mut().entry(fnid).and_modify(|v| {
        //     v.remove(&node_id);
        // });
        // // will recalc next frame begin
        // // but we need to add mem to node in this frame because it's new container
        // *self.mem.borrow_mut() -= con_mem_take;
        // self.nodes.borrow_mut()[node_id].mem +=
        //     self.func(fn_id).cold_start_container_mem_use;
    }

    pub fn try_load_container(&self, fnid: FnId, env: &SimEnv) {
        if self.container(fnid).is_some() {
            // log::info!("已经添加了{}", fnid);
            return;
        }

        let (old, flag) = unsafe {
            let node = NonNull::new_unchecked(self as *const Node as *mut Node);
            let (old, flag) = self.instance_cache_policy.borrow_mut().put(
                fnid,
                Box::new(move |to_replace| {
                    let node = node.as_ref();
                    // log::info!("节点{}要移除的容器{}", node.node_id, to_replace);
                    // for (_k, v) in node.fn_containers.borrow().iter() {
                    //     log::info!("{}", v.fn_id);
                    // }
                    node.container(*to_replace).unwrap().is_idle()
                }),
            );
            log::info!("old{:?}", old);
            (old, flag)
        };

        // 可以增加该容器
        if flag {
            // 1. 将old unload掉
            if old.is_some() {
                self.try_unload_container(old.unwrap(), env, false);
                log::info!("节点{}移除容器{}", self.node_id, old.unwrap());
            }
            // 2. load 当前fnid
            // try cold start
            // 首先从cache中寻找可用容器
            if self.mem_enough_for_container(&env.func(fnid)) {
                let fncon = FnContainer::new(fnid, self.node_id(), env);
                let con_mem_take = fncon.mem_take(env);
                self.fn_containers.borrow_mut().insert(fnid, fncon);
                let node_id = self.node_id();
                env.core
                    .fn_2_nodes_mut()
                    .entry(fnid)
                    .and_modify(|v| {
                        v.insert(node_id);
                    })
                    .or_insert_with(|| {
                        let mut set: HashSet<usize> = HashSet::new();
                        set.insert(node_id);
                        set
                    });

                // will recalc next frame begin
                // but we need to add mem to node in this frame because it's new container
                *self.mem.borrow_mut() += con_mem_take;
            } else {
                log::info!("内存不够，取消缓存标记{}", fnid);
                let mut node_cache = self.instance_cache_policy.borrow_mut();
                assert!(node_cache.remove_all(&fnid));
            }
        }
    }
    // 尝试加载节点上所有待处理任务的容器
    // 如果内存足够且容器不存在，则创建新容器，将任务状态添加到容器，并从待处理任务集合中移除
    pub fn load_container(&self, env: &SimEnv) {
        // 用于存储已移除的待处理任务
        let mut removed_pending = vec![];

        //let mut tasks = self.pending_tasks.borrow_mut().clone();

        // 遍历该节点上的所有待处理任务
        for &(req_id, fnid) in self.pending_tasks.borrow_mut().iter() {
            // 尝试加载函数容器
            self.try_load_container(fnid, env);

            if let Some(mut fncon) = self.container_mut(fnid) {
                // Maybe it's not the first time to load this container
                // So we need to warm it in the cache
                if fncon.req_fn_state.contains_key(&req_id) {
                    continue;
                }

                self.instance_cache_policy.borrow_mut().get(fnid).unwrap();
                // add to container

                assert!(fncon
                    .req_fn_state
                    .insert(
                        req_id,
                        env.fn_new_fn_running_state(&env.request(req_id), fnid)
                    )
                    .is_none());
                removed_pending.push((req_id, fnid));
            }
        }

        for (req_id, fnid) in removed_pending {
            self.pending_tasks.borrow_mut().remove(&(req_id, fnid));
        }
    }
}

impl SimEnv {
    // 初始化节点之间的图数据结构，包括节点之间的连接数计数和带宽图，并为每个节点设置随机速度
    pub fn node_init_node_graph(&self) {
        // 初始化一个节点
        fn _init_one_node(env: &SimEnv, node_id: NodeId) {
            let node = Node::new(node_id, env.help().config());

            // let node_i = nodecnt;
            env.core.nodes_mut().push(node);

            let nodecnt: usize = env.core.nodes().len();

            for i in 0..nodecnt - 1 {
                let randspeed = env.env_rand_f(8000.0, 10000.0);
                // 设置节点间网速
                env.node_set_speed_btwn(i, nodecnt - 1, randspeed);
            }
        }

        // 初始化节点图
        // # init nodes graph
        let dim = NODE_CNT;
        *self.core.node2node_connection_count_mut() = vec![vec![0; dim]; dim];
        *self.core.node2node_graph_mut() = vec![vec![0.0; dim]; dim];
        for i in 0..dim {
            _init_one_node(self, i);
        }

        log::info!("node bandwidth graph: {:?}", self.core.node2node_graph());
    }

    /// 设置节点间网速
    /// - speed: MB/s
    fn node_set_speed_btwn(&self, n1: usize, n2: usize, speed: f32) {
        assert!(n1 != n2);
        fn _set_speed_btwn(env: &SimEnv, nbig: usize, nsmall: usize, speed: f32) {
            env.core.node2node_graph_mut()[nbig][nsmall] = speed;
        }
        if n1 > n2 {
            _set_speed_btwn(self, n1, n2, speed);
        } else {
            _set_speed_btwn(self, n2, n1, speed);
        }
    }

    pub fn node_set_connection_count_between(&self, n1: NodeId, n2: NodeId, count: usize) {
        let _set_connection_count_between = |nbig: usize, nsmall: usize, count: usize| {
            self.core.node2node_connection_count_mut()[nbig][nsmall] = count;
        };
        if n1 > n2 {
            _set_connection_count_between(n1, n2, count);
        } else {
            _set_connection_count_between(n2, n1, count);
        }
    }

    pub fn node_get_connection_count_between(&self, n1: NodeId, n2: NodeId) -> usize {
        let _get_connection_count_between =
            |nbig: usize, nsmall: usize| self.core.node2node_connection_count()[nbig][nsmall];
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
}

impl EnvNodeExt for SimEnv {}
impl EnvNodeExt for SimEnvObserve {}
pub trait EnvNodeExt: WithEnvCore {
    // 返回节点数量
    fn node_cnt(&self) -> usize {
        self.core().nodes().len()
    }

    // 返回对节点列表的不可变引用
    fn nodes<'a>(&'a self) -> Ref<'a, Vec<Node>> {
        self.core().nodes()
    }

    // 返回对节点列表的可变引用
    fn nodes_mut<'a>(&'a self) -> RefMut<'a, Vec<Node>> {
        self.core().nodes_mut()
    }

    // 返回对指定节点ID的不可变引用
    fn node<'a>(&'a self, i: NodeId) -> Ref<'a, Node> {
        let b = self.nodes();

        Ref::map(b, |vec| &vec[i])
    }

    // 返回对指定节点ID的可变引用
    fn node_mut<'a>(&'a self, i: NodeId) -> RefMut<'a, Node> {
        let b = self.nodes_mut();

        RefMut::map(b, |vec| &mut vec[i])
    }
    /// 获取节点间网速
    /// - speed: MB/s
    fn node_get_speed_btwn(&self, n1: NodeId, n2: NodeId) -> f32 {
        let _get_speed_btwn =
            |nbig: usize, nsmall: usize| self.core().node2node_graph()[nbig][nsmall];
        if n1 > n2 {
            _get_speed_btwn(n1, n2)
        } else {
            _get_speed_btwn(n2, n1)
        }
    }

    //获取计算速度最慢的节点
    fn node_get_lowest(&self) -> NodeId {
        let nodes = self.core().nodes();
        let res = nodes
            .iter()
            .min_by(|x, y| x.cpu.partial_cmp(&y.cpu).unwrap())
            .unwrap();
        res.node_id
    }

    //获取最低带宽
    fn node_btw_get_lowest(&self) -> f32 {
        let mut low_btw = None;

        for i in 0..self.core().nodes().len() {
            for j in i + 1..self.core().nodes().len() {
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
}
