use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
};

use crate::{
    actions::Action,
    fn_dag::{FnDAG, FnId, Func},
    node::{Node, NodeId},
    parse_arg,
    request::{ReqId, Request},
    sim_scale_executor::DefaultScaleExecutor,
    sim_scale_from_zero::{
        DirectlyScaleFromZero, LazyScaleFromZero, ScaleFromZero, ScaleFromZeroImpl,
        ScaleFromZeroType,
    },
    sim_scaler::{ScaleArg, Scaler, ScalerImpl, ScalerType},
    sim_scaler_ai::AIScaler,
    sim_scaler_hpa::HpaScaler,
};

pub struct SimEnv {
    pub nodes: RefCell<Vec<Node>>,

    // 节点间网速图
    pub node2node_graph: RefCell<Vec<Vec<f32>>>,

    // databases=[]

    // # dag应用
    pub dags: RefCell<Vec<FnDAG>>,

    pub fn_next_id: RefCell<FnId>,

    pub fn_2_nodes: RefCell<HashMap<FnId, HashSet<NodeId>>>,

    pub fns: RefCell<Vec<Func>>,

    pub current_frame: RefCell<usize>,

    pub requests: RefCell<BTreeMap<ReqId, Request>>,

    pub done_requests: RefCell<Vec<Request>>,

    pub req_next_id: RefCell<ReqId>,

    pub cost: RefCell<f32>,

    pub scale_executor: RefCell<DefaultScaleExecutor>,

    pub scaler: RefCell<ScalerImpl>,
}

impl SimEnv {
    pub fn new() -> Self {
        let args = parse_arg::get_arg();
        let newenv = Self {
            nodes: RefCell::new(Vec::new()),
            node2node_graph: RefCell::new(Vec::new()),
            dags: RefCell::new(Vec::new()),
            fn_next_id: RefCell::new(0),
            current_frame: RefCell::new(0),
            fn_2_nodes: RefCell::new(HashMap::new()),
            fns: RefCell::new(Vec::new()),
            req_next_id: RefCell::new(0),
            requests: RefCell::new(BTreeMap::new()),
            done_requests: RefCell::new(Vec::new()),
            cost: RefCell::new(0.00000001),
            scale_executor: RefCell::new(DefaultScaleExecutor),
            scaler: RefCell::new(match args.scaler {
                ScalerType::AiScaler => AIScaler.into(),
                ScalerType::HpaScaler => HpaScaler::new().into(),
            }),
        };

        newenv.init();
        newenv
    }

    fn init(&self) {
        self.node_init_node_graph();
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
        self.fn_gen_fn_dags();
    }

    pub fn current_frame(&self) -> usize {
        *self.current_frame.borrow()
    }

    // pub fn find_the_most_idle_node(&self) -> NodeId {
    //     self.nodes
    //         .iter()
    //         .min_by(|a, b| a.cmp_rsc(b))
    //         .unwrap()
    //         .node_id
    // }

    // pub fn schedule_req_plan_after_expand(
    //     &self,
    //     cur_fn: FnId,
    //     expand_node: NodeId,
    //     mut req_plan: RequestPlan,
    // ) {
    //     //决定fn调度到哪个node
    //     req_plan.fn_node.insert(cur_fn, expand_node);
    // }
    /// 继续确定当前请求应该放到哪些节点上
    // pub fn scale_and_schedule(&self, action: Action, mut req_plan: RequestPlan) {
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

    // fn exe_fn_one_step(&self, fn_node_id: NodeId, fn_id: FnId) {
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

    // pub fn exe_sim(&self) {
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

    pub fn step(&self, action: Action) -> (f32, String) {
        self.on_frame_begin();

        //没有正在调度的请求了，分配一个正在调度的请求
        self.req_sim_gen_requests();

        match parse_arg::get_arg().scale_from_zero {
            ScaleFromZeroType::LazyScaleFromZero => LazyScaleFromZero.scale_some(self),
            ScaleFromZeroType::DirectlyScaleFromZero => LazyScaleFromZero.scale_some(self),
        }

        self.schedule_fn();

        match parse_arg::get_arg().scaler {
            ScalerType::AiScaler => self
                .scaler
                .borrow_mut()
                .scale(self, ScaleArg::AIScaler(action)),
            ScalerType::HpaScaler => self.scaler.borrow_mut().scale(self, ScaleArg::HPAScaler),
        }

        let ret = (self.score(), self.state_str());

        log::info!("score: {} frame:{}", ret.0, self.current_frame());

        self.on_frame_end();

        ret
    }

    fn on_frame_begin(&self) {
        for n in self.nodes.borrow_mut().iter_mut() {
            n.cpu = 0.0;
            n.mem = n
                .fn_containers
                .iter()
                .map(|(_, c)| c.calc_mem_used(self))
                .sum();

            //有些变为运行状态 内存占用变大很正常
            assert!(
                n.mem <= n.rsc_limit.mem,
                "mem {} > limit {}",
                n.mem,
                n.rsc_limit.mem
            );
        }
    }

    fn on_frame_end(&self) {
        for (_req_i, req) in self.requests.borrow_mut().iter_mut() {
            req.cur_frame_done.clear();
        }

        for n in self.nodes.borrow_mut().iter_mut() {
            for (_, c) in n.fn_containers.iter_mut() {
                if c.this_frame_used {
                    c.this_frame_used = false;
                    c.used_times += 1;
                }
            }
            let mut cost = self.cost.borrow_mut();
            *cost += n.cpu * 0.00001 + n.mem * 0.00001;
        }
        // 自增 frame
        let mut cur_frame = self.current_frame.borrow_mut();
        *cur_frame += 1;
    }
}
