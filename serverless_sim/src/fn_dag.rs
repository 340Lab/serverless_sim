use std::{
    cell::{Ref, RefMut},
    collections::{HashMap, HashSet},
};

use daggy::{
    petgraph::visit::{Topo, Visitable},
    Dag, NodeIndex, Walker,
};

use crate::{
    node::NodeId,
    request::{ReqId, Request},
    sim_env::SimEnv,
    util, CONTAINER_BASIC_MEM,
};

pub type FnId = usize;

pub type DagId = usize;

pub type FnDagInner = Dag<FnId, f32>;

pub struct FnDAG {
    pub dag_i: DagId,
    pub begin_fn_g_i: NodeIndex,
    pub dag: FnDagInner,
}

impl FnDAG {
    fn new(begin_fn: FnId, dag_i: DagId, env: &SimEnv) -> Self {
        let mut dag = Dag::new();
        let begin = dag.add_node(begin_fn);
        env.func_mut(begin_fn)
            .setup_after_insert_into_dag(dag_i, begin);

        Self {
            dag_i,
            begin_fn_g_i: begin,
            dag,
        }
    }

    pub fn instance_single_fn(dag_i: DagId, env: &SimEnv) -> FnDAG {
        let begin_fn: FnId = env.fn_gen_rand_fn();
        let dag = FnDAG::new(begin_fn, dag_i, env);
        dag
    }

    pub fn instance_map_reduce(dag_i: DagId, env: &SimEnv, map_cnt: usize) -> FnDAG {
        let begin_fn = env.fn_gen_rand_fn();
        let mut dag = FnDAG::new(begin_fn, dag_i, env);

        let end_fn = env.fn_gen_rand_fn();
        let end_g_i = dag.dag.add_node(end_fn);
        env.func_mut(end_fn)
            .setup_after_insert_into_dag(dag_i, end_g_i);

        for _i in 0..map_cnt {
            let next = env.fn_gen_rand_fn();
            let (_, next_i) = dag.dag.add_child(
                dag.begin_fn_g_i,
                env.fns.borrow()[begin_fn].out_put_size,
                next,
            );
            env.func_mut(next)
                .setup_after_insert_into_dag(dag_i, next_i);

            dag.dag
                .add_edge(next_i, end_g_i, env.func(next).out_put_size)
                .unwrap();
        }

        dag
    }

    pub fn begin_fn(&self) -> FnId {
        self.dag[self.begin_fn_g_i]
    }

    pub fn new_dag_walker(&self) -> Topo<NodeIndex, <FnDagInner as Visitable>::Map> {
        Topo::new(&self.dag)
    }
}

pub struct Func {
    pub fn_id: FnId,

    pub dag_id: DagId,

    pub graph_i: NodeIndex,

    // #  #运算量/s 一个普通请求处理函数请求的运算量为1，
    pub cpu: f32, // 1
    // # 平均时间占用内存资源 mb
    pub mem: f32, // = 300
    // // # 依赖的数据库-整个过程中数据传输量
    // databases_2_throughput={}
    // # 输出数据量 mb
    pub out_put_size: f32, //=100,

    // 当前函数有实例的节点
    pub nodes: HashSet<usize>,

    // frame count of cold start
    pub cold_start_time: usize,

    pub cold_start_container_mem_use: f32,

    pub cold_start_container_cpu_use: f32,
}

impl Func {
    pub fn parent_fns(&self, env: &SimEnv) -> Vec<FnId> {
        let dag = env.dag_inner(self.dag_id);
        let ps = dag.parents(self.graph_i);
        ps.iter(&dag).map(|(_edge, graph_i)| dag[graph_i]).collect()
    }

    pub fn setup_after_insert_into_dag(&mut self, dag_i: DagId, graph_i: NodeIndex) {
        self.dag_id = dag_i;
        self.graph_i = graph_i;
    }

    pub fn container_mem(&self) -> f32 {
        CONTAINER_BASIC_MEM
    }
}

pub enum FnContainerState {
    Starting { left_frame: usize },
    Running,
}

pub struct FnContainer {
    pub fn_id: FnId,
    pub req_fn_state: HashMap<ReqId, FnRunningState>,
    pub born_frame: usize,
    pub used_times: usize,
    pub this_frame_used: bool,

    /// cpu 利用率
    /// 实际用的计算量/分配到的cpu计算量
    cpu_use_rate: f32,

    state: FnContainerState,
}

impl FnContainer {
    pub fn new(fn_id: FnId, sim_env: &SimEnv) -> Self {
        Self {
            fn_id,
            req_fn_state: HashMap::default(),
            born_frame: sim_env.current_frame(),
            used_times: 0,
            this_frame_used: false,
            cpu_use_rate: 0.0,
            state: FnContainerState::Starting {
                left_frame: sim_env.func(fn_id).cold_start_time,
            },
        }
    }

    pub fn starting_left_frame_move_on(&mut self) {
        match self.state {
            FnContainerState::Starting { ref mut left_frame } => {
                *left_frame -= 1;
                if *left_frame == 0 {
                    drop(left_frame);
                    self.state = FnContainerState::Running;
                }
            }
            _ => {
                panic!("not starting")
            }
        }
    }

    pub fn calc_mem_used(&self, env: &SimEnv) -> f32 {
        match self.state {
            FnContainerState::Starting { .. } => env.func(self.fn_id).cold_start_container_mem_use,
            FnContainerState::Running => {
                CONTAINER_BASIC_MEM + env.func(self.fn_id).mem * self.req_fn_state.len() as f32
            }
        }
    }

    pub fn use_freq(&self, env: &SimEnv) -> f32 {
        if env.current_frame() - self.born_frame == 0 {
            return 0.0;
        }
        self.used_times as f32 / (env.current_frame() - self.born_frame) as f32
    }

    pub fn cpu_use_rate(&self) -> f32 {
        self.cpu_use_rate
    }

    pub fn set_cpu_use_rate(&mut self, alloced: f32, used: f32) {
        if alloced < 0.00001 {
            panic!("alloced cpu is too small");
            self.cpu_use_rate = 0.0;
        }
        self.cpu_use_rate = used / alloced;
    }

    pub fn state_mut(&mut self) -> &mut FnContainerState {
        &mut self.state
    }

    pub fn state(&self) -> &FnContainerState {
        &self.state
    }

    pub fn is_idle(&self) -> bool {
        match self.state {
            FnContainerState::Running => self.req_fn_state.len() == 0,
            FnContainerState::Starting { .. } => false,
        }
    }
}

pub struct FnRunningState {
    /// nodeid - (need,recv)
    pub data_recv: HashMap<NodeId, (f32, f32)>,

    /// 剩余计算量
    pub left_calc: f32,
}

impl FnRunningState {
    pub fn data_recv_done(&mut self) -> bool {
        let mut done = true;
        for (_, (need, recv)) in self.data_recv.iter_mut() {
            if *need > *recv {
                done = false;
                break;
            }
        }
        done
    }

    pub fn compute_done(&self) -> bool {
        self.left_calc <= 0.0
    }
}

impl SimEnv {
    fn fn_gen_rand_fn(&self) -> FnId {
        let id = self.fn_alloc_fn_id();
        self.fns.borrow_mut().push(Func {
            fn_id: id,
            cpu: util::rand_f(0.3, 100.0),
            mem: util::rand_f(100.0, 1000.0),
            out_put_size: util::rand_f(0.1, 20.0),
            nodes: HashSet::new(),
            cold_start_container_mem_use: util::rand_f(100.0, 500.0),
            cold_start_container_cpu_use: util::rand_f(0.1, 50.0),
            cold_start_time: util::rand_i(1, 10),
            dag_id: 0,
            graph_i: 0.into(),
        });
        id
    }

    pub fn fn_gen_fn_dags(&self) {
        let env = self;
        for _ in 0..50 {
            let dag_i = env.dags.borrow().len();
            let dag = FnDAG::instance_map_reduce(dag_i, env, util::rand_i(2, 10));
            env.dags.borrow_mut().push(dag);
        }

        for _ in 0..50 {
            let dag_i = env.dags.borrow().len();
            let dag = FnDAG::instance_single_fn(dag_i, env);
            env.dags.borrow_mut().push(dag);
        }
    }

    fn fn_alloc_fn_id(&self) -> usize {
        let env = self;
        let ret = *env.fn_next_id.borrow();
        *env.fn_next_id.borrow_mut() += 1;
        ret
    }

    pub fn fn_is_fn_dag_begin(&self, dag_i: DagId, fn_i: FnId) -> bool {
        let dags = self.dags.borrow();
        let dag = &dags[dag_i];
        dag.dag[dag.begin_fn_g_i] == fn_i
    }

    pub fn fn_new_fn_running_state(&self, req: &Request, fnid: FnId) -> FnRunningState {
        let env = self;

        let total_calc: f32 = env.func(fnid).cpu;
        let fngi = env.func(fnid).graph_i;
        let mut need_node_data: HashMap<NodeId, f32> = HashMap::new();
        let dag_i = req.dag_i;
        let env_dags = env.dags.borrow();
        let dag = &env_dags[dag_i];
        for (_, pgi) in dag.dag.parents(fngi).iter(&dag.dag) {
            let p: FnId = dag.dag[pgi];
            let node = req.get_fn_node(p).unwrap();
            need_node_data
                .entry(node)
                .and_modify(|v| {
                    *v += env.fns.borrow()[p].out_put_size;
                })
                .or_insert(env.fns.borrow()[p].out_put_size);
        }
        FnRunningState {
            data_recv: need_node_data
                .iter()
                .map(|(node_id, data)| (*node_id, (*data, 0.0)))
                .collect(),

            left_calc: total_calc,
        }
    }

    pub fn func<'a>(&'a self, i: FnId) -> Ref<'a, Func> {
        let b = self.fns.borrow();

        Ref::map(b, |vec| &vec[i])
    }

    pub fn func_mut<'a>(&'a self, i: FnId) -> RefMut<'a, Func> {
        let fns = self.fns.borrow_mut();

        RefMut::map(fns, |fns| &mut fns[i])
    }

    pub fn dag_inner<'a>(&'a self, i: usize) -> Ref<'a, FnDagInner> {
        let b = self.dags.borrow();

        Ref::map(b, |vec| &vec[i].dag)
    }

    pub fn dag<'a>(&'a self, i: usize) -> Ref<'a, FnDAG> {
        let b = self.dags.borrow();

        Ref::map(b, |vec| &vec[i])
    }
}
