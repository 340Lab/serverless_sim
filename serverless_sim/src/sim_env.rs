use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use daggy::petgraph;
use rand_pcg::Pcg64;
use rand_seeder::Seeder;

use crate::{
    actions::ESActionWrapper,
    config::Config,
    es::{self, ESScaler, ESState},
    fn_dag::{FnDAG, FnId, Func},
    metric::{OneFrameMetric, Records},
    node::{Node, NodeId},
    // parse_arg,
    request::{ReqId, Request},
    scale_executor::DefaultScaleExecutor,
    scale_preloader::{least_task::LeastTaskPreLoader, ScalePreLoader},
    schedule::Scheduler,
};

pub struct SimEnv {
    pub recent_use_time: Duration,

    pub rander: RefCell<Pcg64>,

    pub config: Config,

    pub nodes: RefCell<Vec<Node>>,

    // 节点间网速图
    pub node2node_graph: RefCell<Vec<Vec<f32>>>,
    // 节点间网速图
    pub node2node_connection_count: RefCell<Vec<Vec<usize>>>,

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

    pub scale_preloader: RefCell<Box<dyn ScalePreLoader>>,

    pub metric: RefCell<OneFrameMetric>,

    pub metric_record: RefCell<Records>,

    pub each_fn_watch_window: RefCell<usize>,

    pub ef_state: RefCell<ESState>,

    pub spec_scheduler: RefCell<Option<Box<dyn Scheduler + Send>>>,

    pub spec_ef_scaler: RefCell<Option<Box<dyn ESScaler + Send>>>,

    // end time - tasks
    pub timers: RefCell<HashMap<usize, Vec<Box<dyn FnMut(&SimEnv) + Send>>>>,

    pub fn_must_scale_up: RefCell<HashSet<FnId>>,
    // pub distance2hpa: RefCell<usize>,

    // pub hpa_action: RefCell<usize>,
}

impl SimEnv {
    pub fn new(config: Config) -> Self {
        let start = SystemTime::now();
        let recent_use_time = start.duration_since(UNIX_EPOCH).unwrap();

        // let args = parse_arg::get_arg();
        let newenv = Self {
            nodes: RefCell::new(Vec::new()),
            node2node_graph: RefCell::new(Vec::new()),
            node2node_connection_count: RefCell::new(Vec::new()),
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
            metric_record: Records::new(config.str()).into(),
            each_fn_watch_window: (20).into(),
            ef_state: RefCell::new(ESState::new()),
            recent_use_time,
            spec_scheduler: es::prepare_spec_scheduler(&config).into(),
            spec_ef_scaler: es::prepare_spec_scaler(&config).into(),
            rander: RefCell::new(Seeder::from(&*config.rand_seed).make_rng()),

            config,
            timers: HashMap::new().into(),
            fn_must_scale_up: HashSet::new().into(),
            // distance2hpa: (0).into(),
            // hpa_action: (0).into(),
            metric: OneFrameMetric::new().into(),
            scale_preloader: RefCell::new(Box::new(LeastTaskPreLoader::new())),
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
        self.step_es(ESActionWrapper::Int(raw_action))
    }

    pub fn on_frame_begin(&self) {
        for n in self.nodes.borrow_mut().iter_mut() {
            n.last_frame_cpu = n.cpu;
            n.cpu = 0.0;
            *n.mem.borrow_mut() = n
                .fn_containers
                .borrow()
                .iter()
                .map(|(_, c)| c.container_basic_mem(self))
                .sum();

            //有些变为运行状态 内存占用变大很正常
            assert!(
                n.mem() <= n.rsc_limit.mem,
                "mem {} > limit {}",
                n.mem(),
                n.rsc_limit.mem
            );
        }
        // metric
        self.metric.borrow_mut().on_frame_begin();

        // timer
        if let Some(timers) = self.timers.borrow_mut().remove(&self.current_frame()) {
            for mut timer in timers {
                timer(self);
            }
        }

        // *self.distance2hpa.borrow_mut() = 0;
    }

    pub fn on_frame_end(&self) {
        for (_req_i, req) in self.requests.borrow_mut().iter_mut() {
            req.cur_frame_done.clear();
        }

        for n in self.nodes.borrow_mut().iter_mut() {
            for (_, c) in n.fn_containers.borrow_mut().iter_mut() {
                if c.this_frame_used {
                    c.this_frame_used = false;
                    c.used_times += 1;
                }
            }
            let mut cost = self.cost.borrow_mut();
            *cost += n.cpu * 0.00001 + n.mem() * 0.00001;
        }
        // 自增 frame
        let mut cur_frame = self.current_frame.borrow_mut();
        *cur_frame += 1;
    }
}
