use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::{
    fn_dag::{FnContainer, FnDAG, FnId, Func, SimEnvFnOps},
    node::{Node, NodeId, SimEnvNodeOps},
    request::{ReqId, Request, SimEnvRequestOps},
    SPEED_SIMILAR_THRESHOLD,
};

pub struct SimEnv {
    pub nodes: Vec<Node>,

    // 节点间网速图
    pub node2node_graph: Vec<Vec<f32>>,

    // databases=[]

    // # dag应用
    pub dags: Vec<FnDAG>,

    pub fn_next_id: FnId,

    pub fn_2_nodes: HashMap<FnId, HashSet<NodeId>>,

    pub fns: Vec<Func>,

    pub current_frame: usize,

    pub requests: BTreeMap<ReqId, Request>,

    pub req_next_id: ReqId,
}

impl SimEnv {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            node2node_graph: Vec::new(),
            dags: Vec::new(),
            fn_next_id: 0,
            current_frame: 0,
            fn_2_nodes: HashMap::new(),
            fns: Vec::new(),
            req_next_id: 0,
            requests: BTreeMap::new(),
        }
    }

    pub fn node_ops(&mut self) -> SimEnvNodeOps {
        SimEnvNodeOps { env: self }
    }

    pub fn fn_ops(&mut self) -> SimEnvFnOps {
        SimEnvFnOps { env: self }
    }

    pub fn request_ops(&mut self) -> SimEnvRequestOps {
        SimEnvRequestOps { env: self }
    }

    pub fn init(&mut self) {
        self.node_ops().init_node_graph();
        // # # init databases
        // # databases_cnt=5
        // # for i in range(databases_cnt):
        // #     db=DataBase()
        // #     # bind a database to node
        // #     while True:
        // #         rand_node_i=random.randint(0,dim-1)
        // #         if self.nodes[rand_node_i].database==None:
        // #             self.nodes[rand_node_i].database=db
        // #             db.node=self.nodes[rand_node_i]
        // #             break
        // #     self.databases.append(db)

        // # init dags
        self.fn_ops().gen_fn_dags();
    }

    // pub fn find_the_most_idle_node(&self) -> NodeId {
    //     self.nodes
    //         .iter()
    //         .min_by(|a, b| a.cmp_rsc(b))
    //         .unwrap()
    //         .node_id
    // }

    // pub fn schedule_req_plan_after_expand(
    //     &mut self,
    //     cur_fn: FnId,
    //     expand_node: NodeId,
    //     mut req_plan: RequestPlan,
    // ) {
    //     //决定fn调度到哪个node
    //     req_plan.fn_node.insert(cur_fn, expand_node);
    // }
    /// 继续确定当前请求应该放到哪些节点上
    // pub fn scale_and_schedule(&mut self, action: Action, mut req_plan: RequestPlan) {
    //     if let Some(next) = req_plan.fn_dag_walker.next(&self.dags[req_plan.dag_i].dag) {
    //         let fn_to_plan: FnId = self.dags[req_plan.dag_i].dag[next];
    //         match action {
    //             Action::ExpandGreedy => {
    //                 let expand_node = self.expand_greedy(req_plan.dag_i, next);

    //                 self.schedule_req_plan_after_expand(fn_to_plan, expand_node, req_plan);
    //             }
    //             // Action::ExpandRandom => {
    //             //     // 随机选择一个节点,随机选择一个函数放置或者扩容
    //             //     self.schedule_req_plan_after_expand(fn_to_plan, expand_node, req_plan);
    //             // }
    //             // Action::ShrinkRandom => {
    //             //     // 随机选择一个节点,随机选择一个函数实例缩容
    //             // }
    //             Action::ShrinkRuleBased => {}
    //             Action::DoNothing => {}
    //         };
    //         self.scheduling_request = Some(req_plan)
    //     } else {
    //         // # 说明已经完成了
    //         let running = RunningRequest::new_from_plan(self, req_plan);
    //         self.executing_requsts.push(running);
    //     }
    // }

    // fn exe_fn_one_step(&mut self, fn_node_id: NodeId, fn_id: FnId) {
    //     let fn_node = &self.nodes[fn_node_id];

    //     for c in &fn_node.fn_containers {
    //         if c.fn_id == fn_id {}
    //     }
    // }

    // pub fn for_each_parent_fn<F: Fn(FnId)>(&self, dag_i: DagId, child_gnode: NodeIndex, cb: F) {
    //     let parents = self.dags[dag_i].dag.parents(child_gnode);
    //     for (_edge_i, node_i) in parents.iter(&self.dags[cur_dag].dag) {
    //         let fn_id: FnId = self.dags[dag_i].dag[node_i];
    //         cb(fn_id);
    //     }
    // }

    // pub fn request_fn_prev_done(&self, req: &RunningRequest, fn_g_node: NodeIndex) -> bool {
    //     let cur_dag = req.dag_i;
    //     let parents = self.dags[cur_dag].dag.parents(fn_g_node);
    //     let mut parents_all_done = true;
    //     for (_edge_i, node_i) in parents.iter(&self.dags[cur_dag].dag) {
    //         let fn_id: FnId = self.dags[cur_dag].dag[node_i];
    //         if req.done_fns.get(&fn_id).is_none() {
    //             parents_all_done = false;
    //             break;
    //         }
    //     }
    //     parents_all_done
    // }

    // /// 请求下一个fn节点是否在运行（是否被放到container上）
    // pub fn req_fn_running(&self, running: &RunningRequest, fn_id: FnId) -> bool {
    //     let fn_node: NodeId = *running.fn_node.get(&fn_id).unwrap();
    //     let fn_node = &self.nodes[fn_node];
    //     fn_node
    //         .fn_containers
    //         .get(&fn_id)
    //         .unwrap()
    //         .req_fn_state
    //         .contains_key(&running.req_id)
    // }

    // pub fn req_fn_start_run(
    //     &self,
    //     running: &RunningRequest,
    //     fn_id: FnId,
    //     need_node_data: HashMap<NodeId, f32>,
    // ) {
    //     let fn_node: NodeId = *running.fn_node.get(&fn_id).unwrap();
    //     let fn_node = &self.nodes[fn_node];
    //     fn_node
    //         .fn_containers
    //         .get(&fn_id)
    //         .unwrap()
    //         .req_fn_state
    //         .insert(
    //             running.req_id,
    //             FnRunningState::new(self.fns[fn_id].cpu, need_node_data),
    //         );
    // }

    // pub fn exe_sim(&mut self) {
    //     // 遍历每一个请求，将每个请求，当前可以执行，但是未分配到fn container执行的fn分配到fn container
    //     for running in &self.executing_requsts {
    //         let mut walker = Topo::new(&self.dags[running.dag_i].dag);
    //         while let Some(gnode_i) = walker.next(&self.dags[running.dag_i].dag) {
    //             if self.request_fn_prev_done(running, gnode_i) {
    //                 let cur_fn: FnId = self.dags[running.dag_i].dag[gnode_i];
    //                 if !self.req_fn_running(running, cur_fn) {
    //                     let mut need_node_data = HashMap::new();
    //                     self.for_each_parent_fn(running.dag_i, gnode_i, |parent_fnid| {
    //                         let parent_fn_node: NodeId =
    //                             *running.fn_node.get(&parent_fnid).unwrap();
    //                         need_node_data
    //                             .entry(parent_fn_node)
    //                             .and_modify(|data| {
    //                                 *data += self.fns[parent_fnid].out_put_size;
    //                             })
    //                             .or_insert(self.fns[parent_fnid].out_put_size);
    //                     });
    //                     self.req_fn_start_run(running, cur_fn, need_node_data);
    //                 }
    //             }
    //         }
    //     }
    // }

    pub fn step(&mut self, action: Action) {
        //没有正在调度的请求了，分配一个正在调度的请求
        self.request_ops().sim_gen_requests();

        self.scale();

        self.schedule_fn();
    }
}
