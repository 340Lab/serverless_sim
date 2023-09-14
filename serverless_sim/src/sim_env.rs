use std::{
    cell::RefCell,
    collections::{ BTreeMap, HashMap, HashSet },
    time::{ SystemTime, UNIX_EPOCH, Duration },
};

use crate::{
    actions::{ Action, AdjustEachFnWatchWindow, RawAction, EFActionWrapper },
    fn_dag::{ FnDAG, FnId, Func },
    metric::Records,
    node::{ Node, NodeId },
    // parse_arg,
    request::{ ReqId, Request },
    sim_scale_executor::DefaultScaleExecutor,
    sim_scale_from_zero::{ LazyScaleFromZero, ScaleFromZero },
    sim_scaler::{ ScaleArg, Scaler, ScalerImpl, ScalerType },
    sim_scaler_hpa::HpaScaler,
    sim_ef::{ EFState, EFScalerImpl, self },
    network::Config,
    sim_schedule::SchedulerImpl,
    sim_ef_faas_flow::FaasFlowScheduler,
    sim_ef_lass::LassEFScaler,
    sim_ef_fnsche::FnScheScaler,
};

pub struct SimEnv {
    pub recent_use_time: Duration,

    pub config: Config,

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

    pub metric_record: RefCell<Records>,

    pub each_fn_watch_window: RefCell<usize>,

    pub aief_state: Option<RefCell<EFState>>,

    pub spec_scheduler: RefCell<Option<SchedulerImpl>>,

    pub spec_ef_scaler: RefCell<Option<EFScalerImpl>>,
}

impl SimEnv {
    pub fn new(config: Config) -> Self {
        let scaler_type = config.scaler_type();
        let start = SystemTime::now();
        let recent_use_time = start.duration_since(UNIX_EPOCH).unwrap();

        // let args = parse_arg::get_arg();
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
            scaler: RefCell::new(match scaler_type {
                // ScalerType::AiScaler => AIScaler::new().into(),
                ScalerType::HpaScaler => HpaScaler::new().into(),
                // ScalerType::LassScaler => LassScaler::new().into(),
                ScalerType::AiEFScaler => HpaScaler::new().into(),
            }),
            metric_record: Records::new(config.str()).into(),
            each_fn_watch_window: (20).into(),
            aief_state: if scaler_type.is_ai_ef_scaler() {
                Some(RefCell::new(EFState::new()))
            } else {
                None
            },
            recent_use_time,
            spec_scheduler: sim_ef::prepare_spec_scheduler(&config).into(),
            spec_ef_scaler: sim_ef::prepare_spec_scaler(&config).into(),
            config,
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

    // return scores, next_batch_state
    // pub fn step_batch(&mut self, raw_actions: Vec<Vec<f32>>) -> (Vec<f32>, String) {
    //     let start = SystemTime::now();
    //     self.recent_use_time = start.duration_since(UNIX_EPOCH).unwrap();

    //     let mut state = String::new();
    //     if self.config.scaler_type().is_aief_scaler() {
    //         self.step_aief_batch(raw_actions)
    //     } else {
    //         panic!("not support")
    //     }
    // }

    pub fn avoid_gc(&mut self) {
        let start = SystemTime::now();
        self.recent_use_time = start.duration_since(UNIX_EPOCH).unwrap();
    }

    pub fn step(&mut self, raw_action: u32) -> (f32, String) {
        // update to current time
        self.avoid_gc();
        if self.config.scaler_type().is_aief_scaler() {
            // panic!("not support")
            self.step_ef(EFActionWrapper::Int(raw_action))
        } else {
            self.step_common(
                (raw_action / 3).try_into().unwrap(),
                (raw_action % 3).try_into().unwrap()
            )
        }
    }

    fn step_common(
        &self,
        action: Action,
        adjust_watch_window: AdjustEachFnWatchWindow
    ) -> (f32, String) {
        self.on_frame_begin();

        //没有正在调度的请求了，分配一个正在调度的请求
        self.req_sim_gen_requests();

        match self.config.scaler_type() {
            ScalerType::HpaScaler => {
                // match parse_arg::get_arg().scale_from_zero {
                //     ScaleFromZeroType::LazyScaleFromZero => LazyScaleFromZero.scale_some(self),
                //     ScaleFromZeroType::DirectlyScaleFromZero => ,
                // }
                LazyScaleFromZero.scale_some(self);
            }
            // ScalerType::AiScaler => {
            //     match adjust_watch_window {
            //         AdjustEachFnWatchWindow::Up => {
            //             if *self.each_fn_watch_window.borrow_mut() < 255 {
            //                 *self.each_fn_watch_window.borrow_mut() += 1;
            //             }
            //         }
            //         AdjustEachFnWatchWindow::Down => {
            //             if *self.each_fn_watch_window.borrow_mut() > 4 {
            //                 *self.each_fn_watch_window.borrow_mut() -= 1;
            //             }
            //         }
            //         AdjustEachFnWatchWindow::Keep => {}
            //     }
            //     self.scaler.borrow_mut().scale(self, ScaleArg::AIScaler(action));
            // }
            // ScalerType::LassScaler =>
            //     self.scaler
            //         .borrow_mut()
            //         .scale(
            //             self,
            //             ScaleArg::LassScaler(Action::AllowAll(crate::actions::AdjustThres::Keep))
            //         ),
            ScalerType::AiEFScaler => {}
        }

        self.schedule_fn();

        match self.config.scaler_type() {
            // ScalerType::AiScaler => {}
            ScalerType::HpaScaler => self.scaler.borrow_mut().scale(self, ScaleArg::HPAScaler),
            // ScalerType::LassScaler => {}
            ScalerType::AiEFScaler => {
                panic!();
            }
        }

        self.metric_record.borrow_mut().add_frame(self);

        let ret = (self.score(), "[]".to_string());

        log::info!("score: {} frame:{}", ret.0, self.current_frame());

        self.on_frame_end();

        ret
    }

    pub fn on_frame_begin(&self) {
        for n in self.nodes.borrow_mut().iter_mut() {
            n.last_frame_cpu = n.cpu;
            n.cpu = 0.0;
            n.mem = n.fn_containers
                .iter()
                .map(|(_, c)| c.container_basic_mem(self))
                .sum();

            //有些变为运行状态 内存占用变大很正常
            assert!(n.mem <= n.rsc_limit.mem, "mem {} > limit {}", n.mem, n.rsc_limit.mem);
        }
    }

    pub fn on_frame_end(&self) {
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
