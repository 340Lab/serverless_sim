// use std::{
//     cell::{Ref, RefMut},
//     collections::{HashMap, HashSet, VecDeque},
// };

// use daggy::{
//     petgraph::visit::{Topo, Visitable},
//     Dag, NodeIndex, Walker,
// };
// use enum_as_inner::EnumAsInner;

// use crate::{
//     node::{Node, NodeId},
//     request::{ReqId, Request},
//     sim_env::SimEnv,
//     util, CONTAINER_BASIC_MEM,
// };

// pub type FnId = usize;

// pub type DagId = usize;

// pub type FnDagInner = Dag<FnId, f32>;

// pub struct FnDAG {
//     pub dag_i: DagId,
//     pub begin_fn_g_i: NodeIndex,
//     pub dag_inner: FnDagInner,
// }

// impl FnDAG {
//     fn new(begin_fn: FnId, dag_i: DagId, env: &SimEnv) -> Self {
//         let mut dag = Dag::new();
//         let begin = dag.add_node(begin_fn);
//         env.func_mut(begin_fn)
//             .setup_after_insert_into_dag(dag_i, begin);

//         Self {
//             dag_i,
//             begin_fn_g_i: begin,
//             dag_inner: dag,
//         }
//     }

//     pub fn instance_single_fn(dag_i: DagId, env: &SimEnv) -> FnDAG {
//         let begin_fn: FnId = env.fn_gen_rand_fn();
//         let dag = FnDAG::new(begin_fn, dag_i, env);
//         dag
//     }

//     pub fn instance_map_reduce(dag_i: DagId, env: &SimEnv, map_cnt: usize) -> FnDAG {
//         let begin_fn = env.fn_gen_rand_fn();
//         let mut dag = FnDAG::new(begin_fn, dag_i, env);

//         let end_fn = env.fn_gen_rand_fn();
//         let end_g_i = dag.dag_inner.add_node(end_fn);
//         env.func_mut(end_fn)
//             .setup_after_insert_into_dag(dag_i, end_g_i);

//         for _i in 0..map_cnt {
//             let next = env.fn_gen_rand_fn();
//             let (_, next_i) = dag.dag_inner.add_child(
//                 dag.begin_fn_g_i,
//                 env.fns.borrow()[begin_fn].out_put_size,
//                 next,
//             );
//             env.func_mut(next)
//                 .setup_after_insert_into_dag(dag_i, next_i);

//             dag.dag_inner
//                 .add_edge(next_i, end_g_i, env.func(next).out_put_size)
//                 .unwrap();
//         }

//         dag
//     }

//     pub fn instance_single_fn(dag_i: DagId, env: &SimEnv, map_cnt: usize) -> FnDAG {}
//     // #[allow(dead_code)]
//     // pub fn begin_fn(&self) -> FnId {
//     //     self.dag[self.begin_fn_g_i]
//     // }

//     pub fn new_dag_walker(&self) -> Topo<NodeIndex, <FnDagInner as Visitable>::Map> {
//         Topo::new(&self.dag_inner)
//     }

//     pub fn contains_fn(&self, env: &SimEnv, fnid: FnId) -> bool {
//         let gi = env.func(fnid).graph_i;
//         self.dag_inner.node_weight(gi).is_some() && self.dag_inner[gi] == fnid
//     }
// }

// pub struct Func {
//     pub fn_id: FnId,

//     pub dag_id: DagId,

//     pub graph_i: NodeIndex,

//     // #  #运算量/s 一个普通请求处理函数请求的运算量为1，
//     pub cpu: f32, // 1
//     // # 平均时间占用内存资源 mb
//     pub mem: f32, // = 300
//     // // # 依赖的数据库-整个过程中数据传输量
//     // databases_2_throughput={}
//     // # 输出数据量 mb
//     pub out_put_size: f32, //=100,

//     // 当前函数有实例的节点
//     pub nodes: HashSet<usize>,

//     // frame count of cold start
//     pub cold_start_time: usize,

//     pub cold_start_container_mem_use: f32,

//     pub cold_start_container_cpu_use: f32,
// }

// impl Func {
//     pub fn parent_fns(&self, env: &SimEnv) -> Vec<FnId> {
//         let dag = env.dag_inner(self.dag_id);
//         let ps = dag.parents(self.graph_i);
//         ps.iter(&dag).map(|(_edge, graph_i)| dag[graph_i]).collect()
//     }

//     pub fn setup_after_insert_into_dag(&mut self, dag_i: DagId, graph_i: NodeIndex) {
//         self.dag_id = dag_i;
//         self.graph_i = graph_i;
//     }

//     pub fn container_mem(&self) -> f32 {
//         CONTAINER_BASIC_MEM
//     }
// }

// #[derive(EnumAsInner)]
// pub enum FnContainerState {
//     Starting { left_frame: usize },
//     Running,
// }

// pub struct FnContainer {
//     pub node_id: NodeId,
//     pub fn_id: FnId,
//     pub req_fn_state: HashMap<ReqId, RunningTask>,
//     pub born_frame: usize,
//     pub used_times: usize,
//     pub this_frame_used: bool,
//     pub recent_frames_done_cnt: VecDeque<usize>,
//     pub recent_frames_working_cnt: VecDeque<usize>,

//     /// cpu 利用率
//     /// 实际用的计算量/分配到的cpu计算量
//     cpu_use_rate: f32,

//     state: FnContainerState,
// }

// const WORKING_CNT_WINDOW: usize = 20;

// impl FnContainer {
//     pub fn mem_take(&self, env: &SimEnv) -> f32 {
//         match self.state() {
//             FnContainerState::Starting { .. } => env.func(self.fn_id).cold_start_container_mem_use,
//             FnContainerState::Running => env.func(self.fn_id).container_mem(),
//         }
//     }

//     pub fn recent_handle_speed(&self) -> f32 {
//         if self.recent_frames_done_cnt.len() == 0 {
//             return 0.0;
//         }
//         (self
//             .recent_frames_done_cnt
//             .iter()
//             .map(|v| *v)
//             .sum::<usize>() as f32)
//             / (self.recent_frames_done_cnt.len() as f32)
//     }
//     pub fn busyness(&self) -> f32 {
//         if self.recent_frames_working_cnt.len() == 0 {
//             return 0.0;
//         }
//         let mut weight = 1;
//         self.recent_frames_working_cnt
//             .iter()
//             .map(|v| {
//                 let v = (*v * weight) as f32;
//                 // 越接近当前权重越大
//                 weight += 1;
//                 v
//             })
//             .sum::<f32>()
//             / (self.recent_frames_working_cnt.len() as f32)
//     }

//     pub fn recent_frame_is_idle(&self, mut frame_cnt: usize) -> bool {
//         for working_cnt in self.recent_frames_working_cnt.iter().rev() {
//             if *working_cnt > 0 {
//                 return false;
//             }
//             frame_cnt -= 1;
//             if frame_cnt == 0 {
//                 break;
//             }
//         }
//         true
//     }

//     pub fn record_this_frame(&mut self, sim_env: &SimEnv, done_cnt: usize, working_cnt: usize) {
//         // log::info!(
//         //     "container record at frame: {} done cnt:{done_cnt} working cnt:{working_cnt}",
//         //     sim_env.current_frame()
//         // );
//         self.recent_frames_done_cnt.push_back(done_cnt);
//         while self.recent_frames_done_cnt.len() > *sim_env.each_fn_watch_window.borrow() {
//             self.recent_frames_done_cnt.pop_front();
//         }
//         self.recent_frames_working_cnt.push_back(working_cnt);
//         while self.recent_frames_working_cnt.len() > WORKING_CNT_WINDOW {
//             self.recent_frames_working_cnt.pop_front();
//         }
//     }

//     pub fn new(fn_id: FnId, node_id: NodeId, sim_env: &SimEnv) -> Self {
//         Self {
//             node_id,
//             fn_id,
//             req_fn_state: HashMap::default(),
//             born_frame: sim_env.current_frame(),
//             used_times: 0,
//             this_frame_used: false,
//             cpu_use_rate: 0.0,
//             state: FnContainerState::Starting {
//                 left_frame: sim_env.func(fn_id).cold_start_time,
//             },
//             recent_frames_done_cnt: VecDeque::new(),
//             recent_frames_working_cnt: VecDeque::new(),
//         }
//     }

//     pub fn starting_left_frame_move_on(&mut self) {
//         match self.state {
//             FnContainerState::Starting { ref mut left_frame } => {
//                 *left_frame -= 1;
//                 if *left_frame == 0 {
//                     drop(left_frame);
//                     self.state = FnContainerState::Running;
//                 }
//             }
//             _ => {
//                 panic!("not starting")
//             }
//         }
//     }

//     pub fn container_basic_mem(&self, env: &SimEnv) -> f32 {
//         match self.state {
//             FnContainerState::Starting { .. } => env.func(self.fn_id).cold_start_container_mem_use,
//             FnContainerState::Running => CONTAINER_BASIC_MEM,
//         }
//     }

//     // pub fn calc_mem_used(&self, env: &SimEnv) -> f32 {
//     //     match self.state {
//     //         FnContainerState::Starting { .. } => env.func(self.fn_id).cold_start_container_mem_use,
//     //         FnContainerState::Running => {
//     //             CONTAINER_BASIC_MEM + env.func(self.fn_id).mem * self.req_fn_state.len() as f32
//     //         }
//     //     }
//     // }

//     pub fn use_freq(&self, env: &SimEnv) -> f32 {
//         if env.current_frame() - self.born_frame == 0 {
//             return 0.0;
//         }
//         (self.used_times as f32) / ((env.current_frame() - self.born_frame) as f32)
//     }

//     pub fn cpu_use_rate(&self) -> f32 {
//         self.cpu_use_rate
//     }

//     pub fn set_cpu_use_rate(&mut self, alloced: f32, used: f32) {
//         if alloced < 0.00001 {
//             panic!("alloced cpu is too small");
//             // self.cpu_use_rate = 0.0;
//         }
//         self.cpu_use_rate = used / alloced;
//     }

//     pub fn state_mut(&mut self) -> &mut FnContainerState {
//         &mut self.state
//     }

//     pub fn state(&self) -> &FnContainerState {
//         &self.state
//     }

//     pub fn is_idle(&self) -> bool {
//         match self.state {
//             FnContainerState::Running => self.req_fn_state.len() == 0,
//             FnContainerState::Starting { .. } => false,
//         }
//     }
// }

// pub struct RunningTask {
//     /// nodeid - (need,recv)
//     pub data_recv: HashMap<NodeId, (f32, f32)>,

//     /// 剩余计算量
//     pub left_calc: f32,
// }

// impl RunningTask {
//     pub fn data_recv_done(&self) -> bool {
//         let mut done = true;
//         for (_, (need, recv)) in self.data_recv.iter() {
//             if *need > *recv {
//                 done = false;
//                 break;
//             }
//         }
//         done
//     }

//     pub fn compute_done(&self) -> bool {
//         self.left_calc <= 0.0
//     }
// }

// impl SimEnv {
//     fn fn_gen_rand_fn(&self) -> FnId {
//         let id = self.fn_alloc_fn_id();
//         let (cpu, out_put_size) = if self.config.fntype_cpu() {
//             (self.env_rand_f(10.0, 100.0), self.env_rand_f(0.1, 20.0))
//         } else if self.config.fntype_data() {
//             (self.env_rand_f(10.0, 100.0), self.env_rand_f(30.0, 100.0))
//         } else {
//             panic!("not support fntype");
//         };
//         self.fns.borrow_mut().push(Func {
//             fn_id: id,
//             cpu,
//             mem: self.env_rand_f(100.0, 1000.0),
//             out_put_size,
//             nodes: HashSet::new(),
//             cold_start_container_mem_use: self.env_rand_f(100.0, 500.0),
//             cold_start_container_cpu_use: self.env_rand_f(0.1, 50.0),
//             cold_start_time: self.env_rand_i(5, 10),
//             dag_id: 0,
//             graph_i: (0).into(),
//         });
//         id
//     }

//     pub fn fn_gen_fn_dags(&self) {
//         let env = self;
//         // for _ in 0..10 {
//         //     let dag_i = env.dags.borrow().len();
//         //     let dag = FnDAG::instance_map_reduce(dag_i, env, util::rand_i(2, 10));
//         //     env.dags.borrow_mut().push(dag);
//         // }
//         if self.config.dag_type_dag() {
//             for _ in 0..6 {
//                 let mapcnt = env.env_rand_i(2, 5); //2-4
//                 let dag_i = env.dags.borrow().len();
//                 let dag = FnDAG::instance_map_reduce(dag_i, env, mapcnt);
//                 log::info!("dag {} {:?}", dag.dag_i, dag.dag_inner);

//                 env.dags.borrow_mut().push(dag);
//             }
//         } else if self.config.dag_type_single() {
//             for _ in 0..10 {
//                 let dag_i = env.dags.borrow().len();
//                 let dag = FnDAG::instance_single_fn(dag_i, env);
//                 env.dags.borrow_mut().push(dag);
//             }
//         } else {
//             panic!("not support dag type {}", self.config.dag_type);
//         }
//     }

//     fn fn_alloc_fn_id(&self) -> usize {
//         let env = self;
//         let ret = *env.fn_next_id.borrow();
//         *env.fn_next_id.borrow_mut() += 1;
//         ret
//     }

//     // pub fn fn_is_fn_dag_begin(&self, dag_i: DagId, fn_i: FnId) -> bool {
//     //     let dags = self.dags.borrow();
//     //     let dag = &dags[dag_i];
//     //     dag.dag[dag.begin_fn_g_i] == fn_i
//     // }

//     pub fn fn_new_fn_running_state(&self, req: &Request, fnid: FnId) -> RunningTask {
//         let env = self;

//         let total_calc: f32 = env.func(fnid).cpu;
//         let fngi = env.func(fnid).graph_i;
//         let mut need_node_data: HashMap<NodeId, f32> = HashMap::new();
//         let dag_i = req.dag_i;
//         let env_dags = env.dags.borrow();
//         let dag = &env_dags[dag_i];
//         for (_, pgi) in dag.dag_inner.parents(fngi).iter(&dag.dag_inner) {
//             let p: FnId = dag.dag_inner[pgi];
//             let node = req.get_fn_node(p).unwrap();
//             need_node_data
//                 .entry(node)
//                 .and_modify(|v| {
//                     *v += env.fns.borrow()[p].out_put_size;
//                 })
//                 .or_insert(env.fns.borrow()[p].out_put_size);
//         }
//         RunningTask {
//             data_recv: need_node_data
//                 .iter()
//                 .map(|(node_id, data)| (*node_id, (*data, 0.0)))
//                 .collect(),

//             left_calc: total_calc,
//         }
//     }

//     pub fn func<'a>(&'a self, i: FnId) -> Ref<'a, Func> {
//         let b = self.fns.borrow();

//         Ref::map(b, |vec| &vec[i])
//     }

//     pub fn func_mut<'a>(&'a self, i: FnId) -> RefMut<'a, Func> {
//         let fns = self.fns.borrow_mut();

//         RefMut::map(fns, |fns| &mut fns[i])
//     }

//     pub fn dag_inner<'a>(&'a self, i: usize) -> Ref<'a, FnDagInner> {
//         let b = self.dags.borrow();

//         Ref::map(b, |vec| &vec[i].dag_inner)
//     }

//     pub fn dag<'a>(&'a self, i: usize) -> Ref<'a, FnDAG> {
//         let b = self.dags.borrow();

//         Ref::map(b, |vec| &vec[i])
//     }

//     pub fn fn_container_cnt(&self, fnid: FnId) -> usize {
//         let map = self.fn_2_nodes.borrow();
//         map.get(&fnid).map_or_else(|| 0, |nodes| nodes.len())
//     }

//     pub fn fn_containers_for_each<F: FnMut(&FnContainer)>(&self, fnid: FnId, mut f: F) {
//         let map = self.fn_2_nodes.borrow();
//         if let Some(nodes) = map.get(&fnid) {
//             for node in nodes.iter() {
//                 let node = self.node(*node);
//                 f(&node.container(fnid).unwrap());
//             }
//         }
//     }

//     // pub fn fn_running_containers_nodes(&self, fnid: FnId) -> HashSet<NodeId> {
//     //     let mut nodes = HashSet::<NodeId>::new();
//     //     self.fn_containers_for_each(fnid, |c| {
//     //         if c.state().is_running() {
//     //             nodes.insert(c.node_id);
//     //         }
//     //     });
//     //     nodes
//     // }
// }
