use std::collections::{HashMap, HashSet};

use daggy::{
    petgraph::visit::{Topo, Visitable},
    Dag, NodeIndex,
};

use crate::{
    fn_dag::{DagId, FnDagInner, FnId, Func},
    node::NodeId,
    sim_env::SimEnv,
    util, REQUEST_GEN_FRAME_INTERVAL,
};

pub type ReqId = usize;

// pub struct RequestPlan {
//     /// 请求id
//     pub req_id: ReqId,

//     /// 对应请求处理的应用
//     pub dag_i: usize,

//     /// 第一次walk是用来扩容，后面的walk是用来扩容
//     pub fn_dag_walker: Topo<NodeIndex, <Dag<Func, f32> as Visitable>::Map>,

//     /// 函数节点被调度到的机器节点
//     pub fn_node: HashMap<FnId, NodeId>,
// }

pub struct Request {
    /// 请求id
    pub req_id: ReqId,

    /// 对应请求处理的应用
    pub dag_i: DagId,

    /// 函数节点被调度到的机器节点
    pub fn_node: HashMap<FnId, NodeId>,

    ///完成执行的函数节点
    pub done_fns: HashSet<FnId>,

    pub cur_frame_done: HashSet<FnId>,

    pub begin_frame: usize,

    pub end_frame: usize,

    fn_dag_walker: Topo<NodeIndex, <FnDagInner as Visitable>::Map>,

    current_fn: Option<(FnId, NodeIndex)>,
}

impl Request {
    // pub fn new_from_plan(env: &SimEnv, plan: RequestPlan) -> Self {
    //     Self {
    //         /// 请求id
    //         req_id: plan.req_id,
    //         dag_i: plan.dag_i,
    //         fn_node: plan.fn_node,
    //         done_fns: HashSet::new(),
    //     }
    // }
    pub fn new(env: &mut SimEnv, dag_i: DagId, begin_frame: usize) -> Self {
        let mut new = Self {
            req_id: env.request_ops().alloc_req_id(),
            dag_i,
            fn_node: HashMap::new(),
            done_fns: HashSet::new(),
            fn_dag_walker: Topo::new(&env.dags[dag_i].dag),
            current_fn: None,
            begin_frame,
            end_frame: 0,
        };
        new.topo_walk_dag(&env.dags[dag_i].dag);
        new
    }
    pub fn get_fn_node(&self, fnid: FnId) -> Option<NodeId> {
        self.fn_node.get(&fnid).map(|v| *v)
    }

    pub fn topo_walk_dag(&mut self, g: &FnDagInner) -> Option<(FnId, NodeIndex)> {
        let res = self.fn_dag_walker.next(g);
        let res = res.map(|i| (g[i], i));
        self.current_fn = res;
        res
    }

    pub fn dag_current_fn(&self) -> Option<(FnId, NodeIndex)> {
        self.current_fn
    }

    pub fn fn_done(&mut self, fnid: FnId, current_frame: usize) {
        self.done_fns.insert(fnid);
        self.cur_frame_done.insert(fnid);
        if self.done_fns.len() == self.fn_node.len() {
            self.end_frame = current_frame;
        }
    }

    pub fn is_done(&self) -> bool {
        self.done_fns.len() == self.fn_node.len()
    }
}

pub struct SimEnvRequestOps<'a> {
    env: &'a mut SimEnv,
}

impl SimEnvRequestOps<'_> {
    pub fn alloc_req_id(&mut self) -> ReqId {
        let env = self.env;
        let ret = env.req_next_id;
        env.req_next_id += 1;
        ret
    }

    pub fn sim_gen_requests(&mut self) {
        let env = self.env;
        if env.current_frame % REQUEST_GEN_FRAME_INTERVAL == 0 {
            let req_cnt = util::rand_i(2, env.dags.len());

            for _ in 0..req_cnt {
                let dag_i = util::rand_i(0, env.dags.len() - 1);
                let request = Request::new(env, dag_i, env.current_frame);
                let req_id = request.req_id;
                env.requests.insert(req_id, request);
            }
        }
    }
}
