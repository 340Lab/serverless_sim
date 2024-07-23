use std::{
    cell::{Ref, RefCell, RefMut},
    collections::{BTreeMap, HashMap, HashSet},
    process::Command,
    str,
    sync::mpsc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand_pcg::Pcg64;
use rand_seeder::Seeder;

use crate::{
    actions::ESActionWrapper,
    config::Config,
    fn_dag::{DagId, FnDAG, FnId, Func},
    mechanism::ConfigNewMec,
    mechanism_thread::{self, MechScheduleOnce},
    metric::{MechMetric, OneFrameMetric, Recorder, Records},
    node::{Node, NodeId},
    request::{ReqId, Request},
    scale::{down_exec::DefaultScaleDownExec, num::ScaleNum, up_exec::ScaleUpExec},
    sim_run::Scheduler,
    with_env_sub::WithEnvHelp,
    CONTAINER_BASIC_MEM,
};

// 定义 call_python_script 函数
pub fn call_python_script(arg: &str, rng: f32) -> f64 {
    // 将 f32 转换为 String 以传递给 Python 脚本
    let rng_str = format!("{}", rng);
    // linux use python3
    // windows use python
    #[cfg(target_os = "windows")]
    let mut python = Command::new("python");
    #[cfg(not(target_os = "windows"))]
    let mut python = Command::new("python3");
    let output = python
        .arg("src/real-world-emulation/RealWorldAppEmulation.py")
        .arg(arg)
        .arg(rng_str)
        .output()
        .expect("Failed to execute Python script");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if output.status.success() {
        stdout
            .trim()
            .parse::<f64>()
            .expect("Failed to parse Python script output")
    } else {
        panic!(
            "Python script error:\nStandard Output: {}\nStandard Error: {}",
            stdout, stderr
        );
    }
}

impl WithEnvHelp for SimEnv {
    fn help(&self) -> &SimEnvHelperState {
        &self.help
    }
}

pub struct SimEnvHelperState {
    config: Config,
    req_next_id: RefCell<ReqId>,
    fn_next_id: RefCell<FnId>,
    cost: RefCell<f32>,
    metric: RefCell<OneFrameMetric>,
    metric_record: RefCell<Option<Recorder>>,
    mech_metric: RefCell<MechMetric>,
    dag_call_frequency: RefCell<BTreeMap<DagId, (f64, f64)>>,
    pub dag_accumulate_call_frequency: RefCell<BTreeMap<DagId, f64>>,
    // key: frame_idx  value: exe_time
    algo_exc_time: RefCell<HashMap<usize, usize>>,
}

impl Clone for SimEnvHelperState {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            req_next_id: self.req_next_id.clone(),
            fn_next_id: self.fn_next_id.clone(),
            cost: self.cost.clone(),
            metric: self.metric.clone(),
            metric_record: RefCell::new(None),
            dag_call_frequency: BTreeMap::new().into(),
            mech_metric: self.mech_metric.clone(),
            algo_exc_time: self.algo_exc_time.clone(),
            dag_accumulate_call_frequency: BTreeMap::new().into(),
        }
    }
}

impl SimEnvHelperState {
    pub fn fn_next_id(&self) -> FnId {
        let ret = *self.fn_next_id.borrow_mut();
        *self.fn_next_id.borrow_mut() += 1;
        ret
    }
    pub fn req_next_id(&self) -> ReqId {
        let ret = *self.req_next_id.borrow_mut();
        *self.req_next_id.borrow_mut() += 1;
        ret
    }
    pub fn config<'a>(&'a self) -> &'a Config {
        &self.config
    }
    pub fn cost<'a>(&'a self) -> Ref<'a, f32> {
        self.cost.borrow()
    }
    pub fn metric<'a>(&'a self) -> Ref<'a, OneFrameMetric> {
        self.metric.borrow()
    }
    pub fn metric_record<'a>(&'a self) -> Ref<'a, Option<Recorder>> {
        self.metric_record.borrow()
    }

    pub fn cost_mut<'a>(&'a self) -> RefMut<'a, f32> {
        self.cost.borrow_mut()
    }
    pub fn metric_mut<'a>(&'a self) -> RefMut<'a, OneFrameMetric> {
        self.metric.borrow_mut()
    }
    pub fn metric_record_mut<'a>(&'a self) -> RefMut<'a, Option<Recorder>> {
        self.metric_record.borrow_mut()
    }
    pub fn mech_metric<'a>(&'a self) -> Ref<'a, MechMetric> {
        self.mech_metric.borrow()
    }
    pub fn mech_metric_mut<'a>(&'a self) -> RefMut<'a, MechMetric> {
        self.mech_metric.borrow_mut()
    }
    pub fn fn_call_frequency<'a>(&'a self) -> Ref<'a, BTreeMap<DagId, (f64, f64)>> {
        self.dag_call_frequency.borrow()
    }
    pub fn fn_call_frequency_mut<'a>(&'a self) -> RefMut<'a, BTreeMap<DagId, (f64, f64)>> {
        self.dag_call_frequency.borrow_mut()
    }
    pub fn algo_exc_time<'a>(&'a self) -> Ref<'a, HashMap<usize, usize>> {
        self.algo_exc_time.borrow()
    }
    pub fn algo_exc_time_mut<'a>(&'a self) -> RefMut<'a, HashMap<usize, usize>> {
        self.algo_exc_time.borrow_mut()
    }
    pub fn avg_algo_exc_time(&self) -> f64 {
        let sum = self.algo_exc_time.borrow().values().sum::<usize>();
        let count = self.algo_exc_time.borrow().len();
        (sum as f64) / (count as f64)
    }
}

pub struct SimEnvCoreState {
    fn_2_nodes: RefCell<HashMap<FnId, HashSet<NodeId>>>,
    dags: RefCell<Vec<FnDAG>>,
    fns: RefCell<Vec<Func>>,
    // 节点间网速图
    node2node_graph: RefCell<Vec<Vec<f32>>>,
    node2node_connection_count: RefCell<Vec<Vec<usize>>>,
    nodes: RefCell<Vec<Node>>,
    current_frame: RefCell<usize>,
    requests: RefCell<BTreeMap<ReqId, Request>>,
    done_requests: RefCell<Vec<Request>>,
}

impl Clone for SimEnvCoreState {
    fn clone(&self) -> Self {
        Self {
            fn_2_nodes: RefCell::new(self.fn_2_nodes.borrow().clone()),
            dags: RefCell::new(self.dags.borrow().clone()),
            fns: RefCell::new(self.fns.borrow().clone()),
            node2node_graph: RefCell::new(self.node2node_graph.borrow().clone()),
            node2node_connection_count: RefCell::new(
                self.node2node_connection_count.borrow().clone(),
            ),
            nodes: RefCell::new(self.nodes.borrow().clone()),
            current_frame: RefCell::new(*self.current_frame.borrow()),
            requests: RefCell::new(self.requests.borrow().clone()),
            done_requests: RefCell::new(self.done_requests.borrow().clone()),
        }
    }
}

impl SimEnvCoreState {
    pub fn dags<'a>(&'a self) -> Ref<'a, Vec<FnDAG>> {
        self.dags.borrow()
    }
    pub fn dags_mut<'a>(&'a self) -> RefMut<'a, Vec<FnDAG>> {
        self.dags.borrow_mut()
    }
    pub fn fns<'a>(&'a self) -> Ref<'a, Vec<Func>> {
        self.fns.borrow()
    }
    pub fn fns_mut<'a>(&'a self) -> RefMut<'a, Vec<Func>> {
        self.fns.borrow_mut()
    }
    pub fn node2node_graph<'a>(&'a self) -> Ref<'a, Vec<Vec<f32>>> {
        self.node2node_graph.borrow()
    }
    pub fn node2node_graph_mut<'a>(&'a self) -> RefMut<'a, Vec<Vec<f32>>> {
        self.node2node_graph.borrow_mut()
    }

    pub fn fn_2_nodes<'a>(&'a self) -> Ref<'a, HashMap<FnId, HashSet<NodeId>>> {
        self.fn_2_nodes.borrow()
    }
    pub fn node2node_connection_count<'a>(&'a self) -> Ref<'a, Vec<Vec<usize>>> {
        self.node2node_connection_count.borrow()
    }
    pub fn nodes<'a>(&'a self) -> Ref<'a, Vec<Node>> {
        self.nodes.borrow()
    }
    pub fn current_frame<'a>(&'a self) -> usize {
        *self.current_frame.borrow()
    }
    pub fn requests<'a>(&'a self) -> Ref<'a, BTreeMap<ReqId, Request>> {
        self.requests.borrow()
    }
    pub fn done_requests<'a>(&'a self) -> Ref<'a, Vec<Request>> {
        self.done_requests.borrow()
    }

    pub fn fn_2_nodes_mut<'a>(&'a self) -> RefMut<'a, HashMap<FnId, HashSet<NodeId>>> {
        self.fn_2_nodes.borrow_mut()
    }
    pub fn node2node_connection_count_mut<'a>(&'a self) -> RefMut<'a, Vec<Vec<usize>>> {
        self.node2node_connection_count.borrow_mut()
    }
    pub fn nodes_mut<'a>(&'a self) -> RefMut<'a, Vec<Node>> {
        self.nodes.borrow_mut()
    }
    pub fn current_frame_mut<'a>(&'a self) -> RefMut<'a, usize> {
        self.current_frame.borrow_mut()
    }
    pub fn requests_mut<'a>(&'a self) -> RefMut<'a, BTreeMap<ReqId, Request>> {
        self.requests.borrow_mut()
    }
    pub fn done_requests_mut<'a>(&'a self) -> RefMut<'a, Vec<Request>> {
        self.done_requests.borrow_mut()
    }
}

pub struct SimEnvMechanisms {
    scale_executor: RefCell<DefaultScaleDownExec>,
    scale_up_exec: RefCell<Box<dyn ScaleUpExec>>,
    spec_scheduler: RefCell<Option<Box<dyn Scheduler + Send>>>,
    spec_scale_num: RefCell<Option<Box<dyn ScaleNum + Send>>>,
}
impl SimEnvMechanisms {
    pub fn scale_executor<'a>(&'a self) -> Ref<'a, DefaultScaleDownExec> {
        self.scale_executor.borrow()
    }
    pub fn scale_up_exec<'a>(&'a self) -> Ref<'a, Box<dyn ScaleUpExec>> {
        self.scale_up_exec.borrow()
    }
    pub fn spec_scheduler<'a>(&'a self) -> Ref<'a, Option<Box<dyn Scheduler + Send>>> {
        self.spec_scheduler.borrow()
    }
    pub fn spec_scale_num<'a>(&'a self) -> Ref<'a, Option<Box<dyn ScaleNum + Send>>> {
        self.spec_scale_num.borrow()
    }

    pub fn scale_executor_mut<'a>(&'a self) -> RefMut<'a, DefaultScaleDownExec> {
        self.scale_executor.borrow_mut()
    }
    pub fn scale_up_exec_mut<'a>(&'a self) -> RefMut<'a, Box<dyn ScaleUpExec>> {
        self.scale_up_exec.borrow_mut()
    }
    pub fn spec_scheduler_mut<'a>(&'a self) -> RefMut<'a, Option<Box<dyn Scheduler + Send>>> {
        self.spec_scheduler.borrow_mut()
    }
    pub fn spec_scale_num_mut<'a>(&'a self) -> RefMut<'a, Option<Box<dyn ScaleNum + Send>>> {
        self.spec_scale_num.borrow_mut()
    }
}

impl SimEnvMechanisms {}

pub struct SimEnv {
    pub recent_use_time: Duration,
    pub rander: RefCell<Pcg64>,
    // end time - tasks
    pub timers: RefCell<HashMap<usize, Vec<Box<dyn FnMut(&SimEnv) + Send>>>>,

    pub help: SimEnvHelperState,
    pub core: SimEnvCoreState,
    // pub mechanisms: SimEnvMechanisms,
    // pub new_mech: MechanismImpl,
    pub master_mech_not_running: bool,
    pub mech_caller: mpsc::Sender<MechScheduleOnce>,
}

impl SimEnv {
    // 构造函数，接收一个 Config 参数，用于初始化模拟环境的各项属性
    pub fn new(config: Config) -> Self {
        let start = SystemTime::now();
        let recent_use_time = start.duration_since(UNIX_EPOCH).unwrap();

        // let args = parse_arg::get_arg();
        let newenv = Self {
            help: SimEnvHelperState {
                // nodes: vec![Node::new(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)],
                req_next_id: RefCell::new(0),
                fn_next_id: RefCell::new(0),
                cost: RefCell::new(0.00000001),
                metric: RefCell::new(OneFrameMetric::new()),
                metric_record: RefCell::new(Some(Recorder::new(config.str()))),
                config: config.clone(),
                mech_metric: RefCell::new(MechMetric::new()),
                dag_call_frequency: RefCell::new(BTreeMap::new()),
                algo_exc_time: RefCell::new(HashMap::new()),
                dag_accumulate_call_frequency: RefCell::new(BTreeMap::new()),
            },
            core: SimEnvCoreState {
                node2node_graph: RefCell::new(Vec::new()),
                dags: RefCell::new(Vec::new()),
                nodes: RefCell::new(Vec::new()),
                node2node_connection_count: RefCell::new(Vec::new()),
                requests: RefCell::new(BTreeMap::new()),
                done_requests: RefCell::new(Vec::new()),
                current_frame: RefCell::new(0),
                fn_2_nodes: RefCell::new(HashMap::new()),
                fns: RefCell::new(Vec::new()),
            },
            // mechanisms: SimEnvMechanisms {
            //     scale_executor: RefCell::new(DefaultScaleDownExec),
            //     scale_up_exec: RefCell::new(Box::new(LeastTaskScaleUpExec::new())),
            //     spec_scheduler: RefCell::new(sche::prepare_spec_scheduler(&config)),
            //     spec_scale_num: RefCell::new(new_scale_num(&config)),
            // },
            // new_mech: ,
            master_mech_not_running: true,
            recent_use_time,
            rander: RefCell::new(Seeder::from(&*config.rand_seed).make_rng()),
            timers: HashMap::new().into(),
            mech_caller: mechanism_thread::spawn(config.new_mec().unwrap()),
        };

        // 为模拟环境创建所有的dag、node、func
        newenv.init();
        newenv
    }
    pub fn reset(&mut self) {
        let config = self.help.config.clone();
        *self = SimEnv::new(config);
    }
    // 初始化方法，进一步设置仿真环境的状态
    fn init(&self) {
        // 创建 NODE_CNT 个节点并初始化网速图和连接图
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

        // 创建 DAG 实例，并将其加入到 dags 列表中
        self.fn_gen_fn_dags(self);

        let cache_req_freq = format!("cache/{}", self.help.config.no_mech_str());
        if std::fs::metadata(&cache_req_freq).is_err() {
            //为每个dag生成调用频率和CV
            for dag in self.core.dags().iter() {
                let rng = self.env_rand_f(0.0, 1.0);
                let avg_freq = call_python_script("IAT", rng);
                let cv = call_python_script("CV", rng);
                self.help
                    .fn_call_frequency_mut()
                    .insert(dag.dag_i, (avg_freq, cv));
                log::info!(
                    "gen cv:{}, freq:{} for app:{} by rng{}",
                    cv,
                    avg_freq,
                    dag.dag_i,
                    rng
                );
            }
            // mkdir, allow failure
            let _ = std::fs::create_dir("cache");
            // write to file
            let mut file = std::fs::File::create(cache_req_freq).unwrap();
            serde_json::to_writer(&mut file, &*self.help.fn_call_frequency()).unwrap();
        } else {
            // read frome file
            let mut file = std::fs::File::open(cache_req_freq).unwrap();
            let freq: BTreeMap<DagId, (f64, f64)> = serde_json::from_reader(&mut file).unwrap();
            *self.help.fn_call_frequency_mut() = freq;
        }

        log::info!("env init done");
    }

    // 获取当前模拟帧数
    pub fn current_frame(&self) -> usize {
        *self.core.current_frame.borrow()
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

    // 更新最近使用时间，以避免模拟环境被 gc 被清理
    pub fn avoid_gc(&mut self) {
        let start = SystemTime::now();
        self.recent_use_time = start.duration_since(UNIX_EPOCH).unwrap();
    }

    // 根据给定的 raw_action，执行仿真环境的一个时间步，返回 score 和 state
    pub fn step(&mut self, raw_action: u32) -> (f32, String) {
        // update to current time
        self.avoid_gc();
        self.step_es(ESActionWrapper::Int(raw_action), None, None, None, None)
    }

    // 在模拟一帧开始时调用，更新节点状态、清空已完成请求、重置性能指标等
    pub fn on_frame_begin(&self) {
        // 遍历每个节点，更新状态
        for n in self.core.nodes_mut().iter_mut() {
            // 将当前帧的 CPU 使用量保存为上一帧的 CPU 使用量
            n.last_frame_cpu = n.cpu;
            n.last_frame_mem = n.unready_mem();
            // 将当前帧的 CPU 使用量重置为0.0
            n.cpu = 0.0;

            // 更新节点的内存使用量,重新计算
            *n.unready_mem_mut() = n
                .fn_containers
                .borrow()
                .iter()
                .map(|(_, c)| c.container_basic_mem(self))
                .sum();

            // 对节点上的每个容器的mem_use和last_frame_mem重设
            for (_, c) in n.fn_containers.borrow_mut().iter_mut() {
                c.last_frame_mem = c.mem_use;
                c.mem_use = CONTAINER_BASIC_MEM;
            }

            //有些变为运行状态 内存占用变大很正常
            assert!(
                n.unready_mem() <= n.rsc_limit.mem,
                "mem {} > limit {}",
                n.unready_mem(),
                n.rsc_limit.mem
            );
        }
        // metric，将这一帧已完成的请求数清空
        self.help.metric.borrow_mut().on_frame_begin();

        // timer
        if let Some(timers) = self.timers.borrow_mut().remove(&self.current_frame()) {
            for mut timer in timers {
                timer(self);
            }
        }

        // *self.distance2hpa.borrow_mut() = 0;
    }

    // 在模拟一帧结束时调用，更新节点成本和本帧使用过的容器的使用次数，增加帧数
    pub fn on_frame_end(&self) {
        // 遍历环境中的每个请求，清空该请求的当前帧已完成函数表
        for (_req_i, req) in self.core.requests_mut().iter_mut() {
            req.cur_frame_done.clear();
        }

        // 遍历环境中的每个节点
        for n in self.core.nodes_mut().iter_mut() {
            // 遍历节点上的每个容器
            for (_, c) in n.fn_containers.borrow_mut().iter_mut() {
                // 更新容器的使用情况
                if c.this_frame_used {
                    c.this_frame_used = false;
                    c.used_times += 1;
                }
            }
            // 更新模拟环境的总成本
            let mut cost = self.help.cost_mut();
            *cost += n.cpu * 0.00001 + n.unready_mem() * 0.00001;
        }

        // 将这一帧的数据记录到表中
        self.help
            .metric_record_mut()
            .as_mut()
            .unwrap()
            .add_frame(self);

        // 自增 frame
        let mut cur_frame = self.core.current_frame.borrow_mut();

        log::info!("frame done: {}", *cur_frame);
        *cur_frame += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::call_python_script;

    #[test]
    fn test_python_res_consistency() {
        for i in 0..20 {
            let ran = 0.001 * (i as f32);
            let avg_freq = call_python_script("IAT", ran);
            let cv = call_python_script("CV", ran);

            let avg_freq2 = call_python_script("IAT", ran);
            let cv2 = call_python_script("CV", ran);

            assert!(avg_freq - avg_freq2 < 0.0001);
            assert!(cv - cv2 < 0.0001);
        }
    }
}
