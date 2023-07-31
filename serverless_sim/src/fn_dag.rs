use std::collections::{HashMap, HashSet};

use daggy::{Dag, NodeIndex, Walker};

use crate::{
    node::NodeId,
    request::{ReqId, Request},
    sim_env::SimEnv,
    util,
};

pub type FnId = usize;

pub type DagId = usize;

pub type FnDagInner = Dag<FnId, f32>;

pub struct FnDAG {
    pub begin: NodeIndex,
    pub dag: FnDagInner,
}

impl FnDAG {
    pub fn new(begin_fn: FnId) -> Self {
        let mut dag = Dag::new();
        let begin = dag.add_node(begin_fn);
        Self { begin, dag }
    }

    pub fn instance_single_fn(env: &mut SimEnv) -> FnDAG {
        let begin_fn: FnId = env.fn_ops().gen_rand_fn();
        let mut dag = FnDAG::new(begin_fn);
        dag
    }

    pub fn instance_map_reduce(env: &mut SimEnv, map_cnt: usize) -> FnDAG {
        let begin_fn = env.fn_ops().gen_rand_fn();
        let mut dag = FnDAG::new(begin_fn);
        let end_fn = env.fn_ops().gen_rand_fn();
        for i in 0..map_cnt {
            let mut next = env.fn_ops().gen_rand_fn();
            let (_, next_i) = dag
                .dag
                .add_child(dag.begin, env.fns[begin_fn].out_put_size, next);
            dag.dag
                .add_child(next_i, env.fns[next].out_put_size, end_fn);
        }

        dag
    }

    pub fn get_parent_fns(&self, fn_i: NodeIndex) -> Vec<FnId> {
        self.dag
            .parents(fn_i)
            .iter(&self.dag)
            .map(|(parent_i, _)| self.dag[*parent_i])
            .collect()
    }
}

pub struct Func {
    pub unique_i: usize,
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
}

pub struct FnContainer {
    pub fn_id: FnId,
    pub req_fn_state: HashMap<ReqId, FnRunningState>,
}

impl FnContainer {
    pub fn new(fn_id: FnId) -> Self {
        Self {
            fn_id,
            req_fn_state: HashMap::default(),
        }
    }
    pub fn start_for_req(&mut self, req: ReqId, begin_state: FnRunningState) {
        self.req_fn_state.insert(req, begin_state);
    }
}

pub struct FnRunningState {
    /// nodeid - (need,recv)
    pub data_recv: HashMap<NodeId, (f32, f32)>,

    /// 剩余计算量
    pub left_calc: f32,
}

impl FnRunningState {
    pub fn data_recv_done(&self) -> bool {
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

pub struct SimEnvFnOps<'a> {
    pub env: &'a mut SimEnv,
}

impl SimEnvFnOps<'_> {
    fn gen_rand_fn(&mut self) -> FnId {
        let env = self.env;
        let id = self.alloc_fn_id();
        env.fns.push(Func {
            unique_i: id,
            cpu: util::rand_f(0.3, 10.0),
            mem: util::rand_f(100.0, 1000.0),
            out_put_size: util::rand_f(0.1, 20.0),
            nodes: HashSet::new(),
        });
        id
    }

    pub fn gen_fn_dags(&mut self) {
        let env = self.env;
        for _ in 0..50 {
            let dag = FnDAG::instance_map_reduce(env, util::rand_i(2, 10));
            env.dags.push(dag);
        }

        for _ in 0..50 {
            let dag = FnDAG::instance_single_fn(env);
            env.dags.push(dag);
        }
    }

    fn alloc_fn_id(&mut self) -> usize {
        let env = self.env;
        let ret = env.fn_next_id;
        env.fn_next_id += 1;
        ret
    }

    pub fn get_fn_dag_mut(&mut self, dag_i: DagId) -> &mut FnDAG {
        &mut self.env.dags[dag_i]
    }

    pub fn is_fn_dag_begin(&self, dag_i: DagId, fn_i: FnId) -> bool {
        let dag = self.env.dags[dag_i];
        dag.dag[dag.begin] == fn_i
    }

    pub fn new_fn_running_state(
        &self,
        req: &Request,
        fngi: NodeIndex,
        fnid: FnId,
    ) -> FnRunningState {
        let env = self.env;

        let total_calc: f32 = env.fns[fnid].cpu;
        let need_node_data: HashMap<NodeId, f32> = HashMap::new();
        let dag_i = req.dag_i;

        let dag = env.dags[dag_i];
        for (_, pgi) in dag.dag.parents(fngi).iter(&dag.dag) {
            let p: FnId = dag.dag[pgi];
            let node = req.get_fn_node(p).unwrap();
            need_node_data
                .entry(node)
                .and_modify(|v| {
                    *v += env.fns[p].out_put_size;
                })
                .or_insert(env.fns[p].out_put_size);
        }
        FnRunningState {
            data_recv: need_node_data
                .iter()
                .map(|(node_id, data)| (*node_id, (*data, 0.0)))
                .collect(),
            total_calc,
            left_calc: 0.0,
        }
    }
}
