use std::{
    borrow::{Borrow, BorrowMut},
    cell::{Ref, RefCell, RefMut},
    clone,
    cmp::{Eq, Ordering},
    collections::{BTreeSet, HashMap, HashSet, LinkedList},
    fmt::Debug,
    hash::Hash,
    ptr::NonNull,
    rc::Rc,
};

use axum::http::header::SEC_WEBSOCKET_KEY;
use moka::sync::Cache;

use crate::{
    cache::lru::LRUCache,
    fn_dag::{FnContainer, FnContainerState, FnId, Func},
    request::ReqId,
    sim_env::{self, SimEnv},
    util, NODE_CNT, NODE_LEFT_MEM_THRESHOLD, NODE_SCORE_CPU_WEIGHT, NODE_SCORE_MEM_WEIGHT,
};

pub type NodeId = usize;

pub struct NodeRscLimit {
    // 节点cpu上限
    pub cpu: f32,
    // 节点mem上限
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

    //LRU置换策略
    pub lru: RefCell<LRUCache<FnId>>,
}

// // 双向链表节点
// pub struct ListNode<Payload> {
//     key: Option<Payload>, // None when dummy
//     // value: Option<FnContainer>,
//     prev: Option<Rc<RefCell<ListNode<Payload>>>>,
//     next: Option<Rc<RefCell<ListNode<Payload>>>>,
// }

// unsafe impl<Payload> Send for ListNode<Payload> {}
// unsafe impl<Payload> Sync for ListNode<Payload> {}

// impl<Payload> ListNode<Payload> {
//     fn new(key: Option<Payload>) -> Rc<RefCell<Self>> {
//         Rc::new(RefCell::new(ListNode {
//             key,
//             prev: None,
//             next: None,
//         }))
//     }
// }

// // LRU缓存结构
// pub struct LRUCache<Payload: Eq + Hash + Clone + Debug> {
//     capacity: usize,
//     cache: HashMap<Payload, Rc<RefCell<ListNode<Payload>>>>,
//     head: Rc<RefCell<ListNode<Payload>>>,
//     tail: Rc<RefCell<ListNode<Payload>>>,
//     // dummy: Rc<RefCell<ListNode<Payload>>>,
// }

// unsafe impl<Payload: Eq + Hash + Clone + Debug> Send for LRUCache<Payload> {}

// impl<Payload: Eq + Hash + Clone + Debug> LRUCache<Payload> {
//     pub fn new(capacity: usize) -> Self {
//         // let dummy = ListNode::new(None);
//         // dummy.borrow_mut().prev = Some(dummy.clone());
//         // dummy.borrow_mut().next = Some(dummy.clone());
//         let head = ListNode::new(None);
//         let tail = ListNode::new(None);
//         //let head_borrow_mut = head.borrow_mut();
//         //let tail_borrow_mut = tail.borrow_mut();
//         head.as_ref().borrow_mut().next = Some(tail.clone());
//         tail.as_ref().borrow_mut().prev = Some(head.clone());
//         LRUCache {
//             capacity,
//             cache: HashMap::new(),
//             head,
//             tail,
//             // dummy,
//         }
//     }
//     pub fn get(&mut self, key: Payload) -> Option<Payload> {
//         if let Some(rc_node) = self.cache.get(&key) {
//             let node = rc_node.clone();
//             //let value = Some(node.borrow().value.clone());
//             self.removeNode(node.clone());
//             self.moveToHead(node);
//             return Some(key);
//         }
//         None
//     }

//     // return Some(payload) if one is evcited
//     pub fn put(
//         &mut self,
//         key: Payload,
//         mut can_be_evict: impl FnMut(&Payload) -> bool,
//     ) -> (Option<Payload>, bool) {
//         if self.cache.contains_key(&key) {
//             let listnode = self.cache.get(&key).unwrap().clone();
//             //listnode.borrow_mut().value = Some(value);
//             self.removeNode(listnode.clone());
//             self.moveToHead(listnode);
//             return (None, true);
//             //找到了，id为None，put成功
//         }
//         let lsnode = ListNode::new(Some(key.clone()));
//         self.cache.insert(key.clone(), lsnode.clone());
//         self.moveToHead(lsnode.clone()); // 放在最上面
//         if self.cache.len() > self.capacity {
//             let mut back_node = self.tail.as_ref().borrow().prev.clone().unwrap();
//             while back_node.as_ref().borrow().key.is_some() {
//                 if can_be_evict(back_node.as_ref().borrow().key.as_ref().unwrap()) {
//                     // 取出并返回被淘汰节点的键（Payload），以便外部使用
//                     let key_to_remove = back_node.as_ref().borrow().key.clone().unwrap();
//                     self.cache.remove(&key_to_remove);
//                     self.removeNode(back_node);
//                     return (Some(key_to_remove), true);
//                     //找到要删除的，返回id，put成功
//                 } else {
//                     let next_back_node = back_node.as_ref().borrow().prev.clone().unwrap();
//                     back_node = next_back_node;
//                 }
//             }
//             self.removeNode(lsnode);
//             self.cache.remove(&key);
//             return (None, false);
//         }
//         (None, true)
//     }

//     fn moveToHead(&mut self, node: Rc<RefCell<ListNode<Payload>>>) {
//         let next = self.head.as_ref().borrow().next.clone();
//         node.as_ref().borrow_mut().prev = Some(self.head.clone());
//         node.as_ref().borrow_mut().next = next.clone();
//         self.head.as_ref().borrow_mut().next = Some(node.clone());
//         next.unwrap().as_ref().borrow_mut().prev = Some(node);
//     }

//     fn removeNode(&mut self, node: Rc<RefCell<ListNode<Payload>>>) {
//         let prev = node.as_ref().borrow().prev.clone().unwrap();
//         let next = node.as_ref().borrow().next.clone().unwrap();
//         prev.as_ref().borrow_mut().next = Some(next.clone());
//         next.as_ref().borrow_mut().prev = Some(prev);
//     }

//     #[cfg(test)]
//     fn cmp_list(&self, list: Vec<Payload>) {
//         assert_eq!(self.cache.len(), list.len());
//         let mut cur = self.head.as_ref().borrow().next.clone();
//         for i in &list {
//             if let Some(n) = cur {
//                 assert_eq!(i, n.as_ref().borrow().key.as_ref().unwrap());
//                 cur = n.as_ref().borrow().next.clone();
//             } else {
//                 panic!();
//             }
//         }
//         assert!(cur.unwrap().as_ref().borrow().key.is_none());
//     }

//     #[cfg(test)]
//     fn print_list(&self) {
//         let mut cur = Some(self.head.clone());
//         while let Some(n) = cur {
//             println!("{:?}", n.as_ref().borrow().key);
//             cur = n.as_ref().borrow().next.clone();
//         }
//     }
// }

// #[test]
// fn test_lru_cache() {
//     let mut cache = LRUCache::<usize>::new(3);

//     // 测试 put 方法，当缓存未满时
//     assert_eq!(cache.put(1, |_| true), (None, true));
//     assert_eq!(cache.put(2, |_| true), (None, true));
//     assert_eq!(cache.put(3, |_| true), (None, true));
//     cache.print_list();
//     cache.cmp_list(vec![3, 2, 1]);

//     // 测试 get 方法，对于已存在的键
//     assert_eq!(cache.get(2), Some(2));
//     cache.cmp_list(vec![2, 3, 1]);
//     cache.print_list();

//     // 再次 put 已存在的键，应该更新位置但不改变缓存大小
//     assert_eq!(cache.put(2, |_| true), (None, true));
//     cache.cmp_list(vec![2, 3, 1]);
//     cache.print_list();

//     // 当缓存满时，测试 put 方法，应该淘汰最久未使用的元素
//     assert_eq!(cache.put(4, |_| true), (Some(1), true));
//     cache.cmp_list(vec![4, 2, 3]);
//     cache.print_list();

//     // 确认 1 已经被移除，4 是最新加入的
//     assert_eq!(cache.get(1), None);
//     cache.print_list();
//     assert_eq!(cache.get(4), Some(4));
//     cache.cmp_list(vec![4, 2, 3]);
//     cache.print_list();

//     // 测试淘汰策略，如果淘汰函数返回false，则不淘汰
//     let mut can_be_evicted = false; // 初始设置为不淘汰任何元素
//     assert_eq!(cache.put(5, |_| can_be_evicted), (None, false)); // ，不会淘汰
//     cache.print_list();

//     can_be_evicted = true; // 设置为允许淘汰
//     assert_eq!(cache.put(6, |_| can_be_evicted), (Some(3), true)); // 2 应该被淘汰
//     cache.cmp_list(vec![6, 4, 2]);
//     cache.print_list();

//     // 确认 2 已经被移除
//     assert_eq!(cache.get(3), None);
//     cache.print_list();
// }

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
            last_frame_mem: 0.0,
            lru: RefCell::new(LRUCache::new(10)),
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

    pub fn try_unload_container(&self, fnid: FnId, env: &SimEnv) {
        log::info!("scale down fn {fnid} from node {}", self.node_id());
        // env.set_scale_down_result(fnid, self.node_id());

        // let mut lrucache = self.lru.borrow_mut();
        // let lrunode = lrucache.cache.get(&fnid).unwrap().clone();
        // lrucache.removeNode(lrunode);
        // lrucache.cache.remove(&fnid);

        let nodeid = self.node_id();
        let Some(cont) = self.fn_containers.borrow_mut().remove(&fnid) else {
            log::info!("try_unload_container not found {}", fnid);
            return;
        };

        let mut lrucache = self.lru.borrow_mut();
        assert!(lrucache.removeAll(&fnid));

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

    // 尝试在节点上加载指定函数ID的容器。如果内存足够且容器不存在，则创建新容器并更新节点状态
    // pub fn try_load_container(&mut self, fnid: FnId, env: &SimEnv) {
    //     if self.container(fnid).is_none() {
    //         // try cold start
    //         // 首先从lru_cache中寻找可用容器
    //         if let Some(cached_container) = self.lru.get(fnid) {
    //             // 缓存命中,deploy it to the node
    //             self.fn_containers.borrow_mut().insert(fnid);
    //             let node_id = self.node_id();
    //             env.core
    //                 .fn_2_nodes_mut()
    //                 .entry(fnid)
    //                 .and_modify(|v| {
    //                     v.insert(node_id);
    //                 })
    //                 .or_insert_with(|| {
    //                     let mut set = HashSet::new();
    //                     set.insert(node_id);
    //                     set
    //                 });
    //             // Update memory usage immediately as it's a reused container
    //             *self.mem.borrow_mut() += cached_container.unwrap().mem_take(env);
    //         } else {
    //             //缓存未命中，冷启动！
    //             if self.mem_enough_for_container(&env.func(fnid)) {
    //                 let fncon = FnContainer::new(fnid, self.node_id(), env);
    //                 let con_mem_take = fncon.mem_take(env);
    //                 self.fn_containers.borrow_mut().insert(fnid, fncon.clone());
    //                 // log::info!("expand fn: {fn_id} to node: {node_id}");
    //                 // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
    //                 let node_id = self.node_id();
    //                 env.core
    //                     .fn_2_nodes_mut()
    //                     .entry(fnid)
    //                     .and_modify(|v| {
    //                         v.insert(node_id);
    //                     })
    //                     .or_insert_with(|| {
    //                         let mut set = HashSet::new();
    //                         set.insert(node_id);
    //                         set
    //                     });

    //                 // will recalc next frame begin
    //                 // but we need to add mem to node in this frame because it's new container
    //                 *self.mem.borrow_mut() += con_mem_take;
    //                 // self.nodes.borrow_mut()[node_id].mem +=
    //                 //     self.func(fn_id).cold_start_container_mem_use;
    //                 // 增加新创建的容器到 lru_cache
    //                 &self.lru.put(fnid);
    //             }
    //         }
    //     }
    // }

    pub fn try_load_container(&self, fnid: FnId, env: &SimEnv) {
        if self.container(fnid).is_some() {
            //self.lru.borrow_mut().get(fnid);
            return;
        }

        let (old, flag) = self.lru.borrow_mut().put(fnid, |to_replace| {
            log::info!("节点{}要移除的容器{}", self.node_id, to_replace,);
            for (k, v) in self.fn_containers.borrow().iter() {
                log::info!("{}", v.fn_id);
            }
            self.container(*to_replace).unwrap().is_idle()
        });

        // 可以增加该容器
        if flag {
            // 1. 将old unload掉
            if old.is_some() {
                self.try_unload_container(old.unwrap(), env);
                log::info!("节点{}移除容器{}", self.node_id, old.unwrap());
            }
            // 2. load 当前fnid
            // try cold start
            // 首先从lru_cache中寻找可用容器
            if self.mem_enough_for_container(&env.func(fnid)) {
                //let fncon = FnContainer::new(fnid, self.node_id(), env);
                let fncon = FnContainer::new(fnid, self.node_id(), env);
                let con_mem_take = fncon.mem_take(env);
                self.fn_containers.borrow_mut().insert(fnid, fncon);
                log::info!("节点{}添加容器{}", self.node_id, fnid);
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
                // self.nodes.borrow_mut()[node_id].mem +=
                //     self.func(fn_id).cold_start_container_mem_use;
                // 增加新创建的容器到 lru_cache
            } else {
                // let mut lrucache = self.lru.borrow_mut();
                // let lrunode = lrucache.cache.get(&fnid).unwrap().clone();
                // lrucache.removeNode(lrunode);
                // lrucache.cache.remove(&fnid);
                let mut lrucache = self.lru.borrow_mut();
                assert!(lrucache.removeAll(&fnid));
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
                //6.14新增任务
                self.lru.borrow_mut().get(fnid);
                // add to container
                fncon.req_fn_state.insert(
                    req_id,
                    env.fn_new_fn_running_state(&env.request(req_id), fnid),
                );
                removed_pending.push((req_id, fnid));
            }
        }

        // let mut abcd: HashMap<usize, usize> = HashMap::new();

        // for &(req_id, fnid) in self.pending_tasks.borrow().iter() {
        //     abcd.insert(req_id.clone(), fnid.clone());
        // }

        // for (req_id, fnid) in abcd.iter() {
        //     // 尝试加载函数容器
        //     self.try_load_container(*fnid, env);

        //     if let Some(mut fncon) = self.container_mut(*fnid) {
        //         // add to container
        //         fncon.req_fn_state.insert(
        //             *req_id,
        //             env.fn_new_fn_running_state(&env.request(*req_id), *fnid),
        //         );
        //         removed_pending.push((req_id, fnid));
        //     }
        // }

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
            let node = Node::new(node_id);
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

    /// 获取节点间网速
    /// - speed: MB/s
    pub fn node_get_speed_btwn(&self, n1: NodeId, n2: NodeId) -> f32 {
        let _get_speed_btwn =
            |nbig: usize, nsmall: usize| self.core.node2node_graph()[nbig][nsmall];
        if n1 > n2 {
            _get_speed_btwn(n1, n2)
        } else {
            _get_speed_btwn(n2, n1)
        }
    }

    //获取计算速度最慢的节点
    pub fn node_get_lowest(&self) -> NodeId {
        let nodes = self.core.nodes();
        let res = nodes
            .iter()
            .min_by(|x, y| x.cpu.partial_cmp(&y.cpu).unwrap())
            .unwrap();
        res.node_id
    }

    //获取最低带宽
    pub fn node_btw_get_lowest(&self) -> f32 {
        let mut low_btw = None;

        for i in 0..self.core.nodes().len() {
            for j in i + 1..self.core.nodes().len() {
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

    // 返回节点数量
    pub fn node_cnt(&self) -> usize {
        self.core.nodes().len()
    }

    // 返回对节点列表的不可变引用
    pub fn nodes<'a>(&'a self) -> Ref<'a, Vec<Node>> {
        self.core.nodes()
    }

    // 返回对节点列表的可变引用
    pub fn nodes_mut<'a>(&'a self) -> RefMut<'a, Vec<Node>> {
        self.core.nodes_mut()
    }

    // 返回对指定节点ID的不可变引用
    pub fn node<'a>(&'a self, i: NodeId) -> Ref<'a, Node> {
        let b = self.nodes();

        Ref::map(b, |vec| &vec[i])
    }

    // 返回对指定节点ID的可变引用
    pub fn node_mut<'a>(&'a self, i: NodeId) -> RefMut<'a, Node> {
        let b = self.nodes_mut();

        RefMut::map(b, |vec| &mut vec[i])
    }
}
