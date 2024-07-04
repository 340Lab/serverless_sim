use crate::fn_dag::EnvFnExt;
use crate::node::EnvNodeExt;
use crate::score::EnvMetricExt;
use crate::{
    config::Config,
    fn_dag::FnId,
    mechanism_conf::ModuleMechConf,
    sim_env::SimEnv,
    util::Window,
};
use chrono;
use serde::{ Deserialize, Serialize };
use serde_json::Value;
use std::{ collections::{ BTreeMap, HashMap }, fs::{ self, File }, io::{ Read, Write } };

// #[derive(Serialize, Deserialize)]
// pub struct ReqFrame {
//     // reqid
//     r: ReqId,
//     // dagid
//     d: DagId,
//     // new in
//     n: bool,
// }

// #[derive(Serialize, Deserialize)]
// pub struct NodeFrame {
//     // node id
//     n: NodeId,
//     // cpu
//     c: f32,
//     // mem
//     m: f32,
// }

// #[derive(Serialize, Deserialize)]
// pub struct RecordOneFrame {
//     frame: usize,
//     running_reqs: Vec<Value>,
//     nodes: Vec<Value>,
//     req_done_time_avg: f32,
//     req_done_time_std: f32,
//     req_done_time_avg_90p: f32,
//     cost: f32,
//     score:f32,
// }
#[derive(Clone)]
pub struct MechMetric {
    // 函数-窗口，窗口中记录了该函数在 窗口长度 中被请求但还未被调度的次数
    fn_recent_req_cnt_window: HashMap<FnId, Window>,

    // 函数-未被调度的数量
    fn_unsche_req_cnt: HashMap<FnId, usize>,

    // 节点-任务数量
    node_task_new_cnt: HashMap<FnId, usize>,
}

impl MechMetric {
    pub fn new() -> Self {
        Self {
            fn_recent_req_cnt_window: HashMap::new(),
            fn_unsche_req_cnt: HashMap::new(),
            node_task_new_cnt: HashMap::new(),
        }
    }

    // FIX 滑动窗口有问题，需要重新设计
    // 新请求生成之后
    pub fn on_new_req_generated(&mut self, env: &SimEnv) {
        // 清空数据表
        self.fn_unsche_req_cnt.clear();
        self.node_task_new_cnt.clear();

        // 记录这一帧中每个函数的未被调度的请求数量
        let mut fn_count = HashMap::new();

        // 遍历所有函数
        for func in env.core.fns().iter() {
            // MARK 对每个函数都进行统计
            fn_count.entry(func.fn_id).or_insert(0);
        }

        // 遍历每个请求
        for (_, req) in env.core.requests().iter() {
            // DAG的迭代器
            let mut walker = env.dag(req.dag_i).new_dag_walker();

            // 遍历DAG的每个节点
            while let Some(fngid) = walker.next(&env.dag(req.dag_i).dag_inner) {
                // 获取函数节点id
                let fnid = env.dag_inner(req.dag_i)[fngid];

                // 若该函数没有被调度到节点上
                if req.get_fn_node(fnid) == None {
                    // 如果fnid在fn_unsche_req_cnt中存在，则将其值加一，否则插入一个新值1
                    self.fn_unsche_req_cnt
                        .entry(fnid)
                        .and_modify(|v| {
                            *v += 1;
                        })
                        .or_insert(1);
                }

                // 如果请求的已完成函数列表中不包含fnid，则计数+1
                if !req.done_fns.contains_key(&fnid) {
                    fn_count.entry(fnid).and_modify(|v| {
                        *v += 1;
                    });
                }
            }
        }

        // 遍历所有函数，将其未完成的函数请求数量记录到窗口中
        for (fnid, count) in fn_count.iter() {
            // 如果目前对该函数没有记录，则先创建一个窗口
            if !self.fn_recent_req_cnt_window.contains_key(fnid) {
                self.fn_recent_req_cnt_window.insert(*fnid, Window::new(10));
            }
            // 更新记录
            self.fn_recent_req_cnt_window
                .entry(*fnid)
                .and_modify(|window| window.push(*count as f32));
        }

        // 遍历模拟环境中的每个节点
        for n in env.nodes().iter() {
            // 将节点ID和该节点的所有任务插入到node_task_new_cnt中
            self.node_task_new_cnt.insert(n.node_id(), n.all_task_cnt());
        }
    }

    pub fn fn_recent_req_cnt(&self, fnid: FnId) -> f32 {
        self.fn_recent_req_cnt_window
            .get(&fnid)
            .map(|v| v.avg())
            .unwrap_or(0.0)
    }
    pub fn fn_unsche_req_cnt(&self, fnid: FnId) -> usize {
        *self.fn_unsche_req_cnt.get(&fnid).unwrap_or(&0)
    }

    pub fn add_node_task_new_cnt(&mut self, nodeid: FnId) {
        self.node_task_new_cnt
            .entry(nodeid)
            .and_modify(|v| {
                *v += 1;
            })
            .or_insert(1);
    }
    pub fn node_task_new_cnt(&self, nodeid: FnId) -> usize {
        *self.node_task_new_cnt.get(&nodeid).unwrap_or(&0)
    }
}

#[derive(Clone)]
pub struct OneFrameMetric {
    // pub frame: usize,
    done_request_count: usize,
}

impl OneFrameMetric {
    pub fn new() -> Self {
        Self {
            // frame: 0,
            done_request_count: 0,
        }
    }
    pub fn on_frame_begin(&mut self) {
        // self.frame += 1;
        self.done_request_count = 0;
    }
    pub fn add_done_request(&mut self) {
        self.done_request_count += 1;
    }
    // pub fn done_request_count(&self) -> usize {
    //     self.done_request_count
    // }
}

#[derive(Serialize, Deserialize)]
pub struct Records {
    pub record_name: String,
    // 0 frame,
    // 1 running_reqs,
    // 2 nodes,
    // 3 req_done_time_avg,
    // 4 req_done_time_std,
    // 5 req_done_time_avg_90p,
    // 6 cost
    pub frames: Vec<Vec<serde_json::Value>>,
}

const FRAME_IDX_FRAME: usize = 0;                           // 帧数
const FRAME_IDX_RUNNING_REQS: usize = 1;                    // 请求数量
const FRAME_IDX_NODES: usize = 2;                           // 节点的状态：cpu、mem
const FRAME_IDX_REQ_DONE_TIME_AVG: usize = 3;               // 请求的平均完成时间
const FRAME_IDX_REQ_DONE_TIME_STD: usize = 4;               // 请求的完成时间的标准差
const FRAME_IDX_REQ_DONE_TIME_AVG_90P: usize = 5;           // 请求的90%完成时间
const FRAME_IDX_COST: usize = 6;                            // 成本
const FRAME_IDX_SCORE: usize = 7;                           // 得分（强化学习用）
const FRAME_IDX_DONE_REQ_COUNT: usize = 8;                  // 已完成请求数量
const FRAME_IDX_REQ_WAIT_SCHE_TIME: usize = 9;              // 等待调度的时间
const FRAME_IDX_REQ_WAIT_COLDSTART_TIME: usize = 10;        // 冷启动的时间
const FRAME_IDX_REQ_DATA_RECV_TIME: usize = 11;             // 数据接收时间
const FRAME_IDX_REQ_EXE_TIME: usize = 12;                   // 请求的执行时间
const FRAME_IDX_ALGO_EXE_TIME: usize = 13;                  // 算法执行时间
const FRAME_IDX_FNCONTAINER_COUNT: usize = 14;              // 总的容器数量

// the last + 1
const FRAME_LEN: usize = 15;

impl Records {
    pub fn new(mut key: String) -> Self {
        // let args = parse_arg::get_arg();
        key = key.replace(":", "_");
        key = key.replace(",", ".");
        key = key.replace("\"", "");
        let record_name = format!(
            "{}.{}",
            key,
            // match args.scale_from_zero {
            //     ScaleFromZeroType::LazyScaleFromZero => "lazy_scale_from_zero",
            //     ScaleFromZeroType::DirectlyScaleFromZero => "directly_scale_from_zero",
            // },
            chrono::offset::Utc::now().format("UTC_%Y_%m_%d_%H_%M_%S")
        );
        Self {
            record_name,
            frames: Vec::new(),
        }
    }

    // 将模拟环境中的一帧数据添加到记录中
    pub fn add_frame(&mut self, sim_env: &SimEnv) {
        let mut frame = vec![Value::Null; FRAME_LEN];
        frame[FRAME_IDX_FRAME] = sim_env.core.current_frame().into();
        frame[FRAME_IDX_RUNNING_REQS] = sim_env.core
            .requests()
            .iter()
            .map(|(reqid, req)| {
                serde_json::json!({
                    "r": *reqid,
                    "d": req.dag_i,
                    "n": (req.begin_frame == sim_env.current_frame()),
                })
            })
            .collect::<Vec<_>>()
            .into();
        frame[FRAME_IDX_NODES] = sim_env.core
            .nodes()
            .iter()
            .map(|node| {
                serde_json::json!( {
                    "n": node.node_id(),
                    "c": node.cpu,
                    "m": node.unready_mem(),
                })
            })
            .collect::<Vec<_>>()
            .into();
        frame[FRAME_IDX_REQ_DONE_TIME_AVG] = sim_env.req_done_time_avg().into();
        frame[FRAME_IDX_REQ_DONE_TIME_STD] = sim_env.req_done_time_std().into();
        frame[FRAME_IDX_REQ_DONE_TIME_AVG_90P] = sim_env.req_done_time_avg_90p().into();
        frame[FRAME_IDX_COST] = sim_env.cost_each_req().into();
        frame[FRAME_IDX_SCORE] = sim_env.score().into();
        frame[FRAME_IDX_DONE_REQ_COUNT] = sim_env.help.metric().done_request_count.into();
        frame[FRAME_IDX_REQ_WAIT_SCHE_TIME] = sim_env.req_wait_sche_time_avg().into();
        frame[FRAME_IDX_REQ_WAIT_COLDSTART_TIME] = sim_env.req_wait_coldstart_time_avg().into();
        frame[FRAME_IDX_REQ_DATA_RECV_TIME] = sim_env.req_data_recv_time_avg().into();
        frame[FRAME_IDX_REQ_EXE_TIME] = sim_env.req_exe_time_avg().into();
        frame[FRAME_IDX_ALGO_EXE_TIME] = sim_env.help.avg_algo_exc_time().into();
        frame[FRAME_IDX_FNCONTAINER_COUNT] = sim_env.core
            .nodes()
            .iter()
            .map(|n| n.fn_containers.borrow().len())
            .sum::<usize>()
            .into();

        self.frames.push(frame);
    }

    pub fn flush(&self, env: &SimEnv) {
        if env.help.config().no_log {
            log::info!("no metric record, skip flush");
            return;
        }
        if self.frames.len() > 9 {
            fs::create_dir_all("records").unwrap();

            log::info!("flush to target key: {}", self.record_name);
            let mut file = File::create(format!("records/{}.json", self.record_name)).unwrap();
            file.write_all(serde_json::to_string(self).unwrap().as_bytes()).unwrap();

            // 计算几个关键指标，输出到对应seed的文件中
        } else {
            log::info!("frame is too small, skip flush");
        }
    }
}

#[derive(Clone)]
struct RecordFile {
    file_name: String,
    config: Config,
    time_str: String,
}
impl RecordFile {
    fn concat_name_without_time(&self) -> String {
        self.file_name.split("UTC").next().unwrap().to_owned()
    }
    fn new(file_name: String) -> Option<Self> {
        if let None = file_name.find("UTC") {
            return None;
        }
        let mut utciter = file_name.split("UTC");
        let _front_utc = utciter.next().unwrap().to_owned();
        let back_utc = utciter.next().unwrap().to_owned();

        let _idx = 0;
        let config = Config {
            rand_seed: "".to_owned(),
            request_freq: "".to_owned(),
            dag_type: "".to_owned(),
            cold_start: "".to_owned(),
            fn_type: "".to_owned(),
            // app_types: vec![],
            no_log: false,

            mech: ModuleMechConf::new().0,
            total_frame: 1000,
        };

        Some(Self {
            file_name,
            config,
            time_str: back_utc,
        })
    }
}

pub fn group_records_by_seed() {
    // 过滤掉已经有记录的seed文件，对剩下的文件进行扫描

    // 找出group文件，将所有已有的seed信息读取出来
    // seed->(config->record_file)
    let mut seed_files: HashMap<String, BTreeMap<String, RecordFile>> = HashMap::new();

    let direntries = fs::read_dir("./records").unwrap();
    for entry in direntries {
        let entry = entry.unwrap();
        let file_name = entry.file_name().into_string().unwrap();
        if file_name.ends_with(".json") {
            if let Some(record_file) = RecordFile::new(file_name) {
                log::info!("read one record file: {}", record_file.file_name);
                let different_confs = seed_files
                    .entry(record_file.config.rand_seed.clone())
                    .or_insert_with(|| {
                        let map = BTreeMap::new();
                        map
                    });
                different_confs
                    .entry(record_file.concat_name_without_time())
                    .and_modify(|v| {
                        if record_file.time_str > v.time_str {
                            *v = record_file.clone();
                        }
                    })
                    .or_insert(record_file);
            }
            // .insert(record_file.config.str(), record_file);
        }
    }

    let mut seeds_metrics_cache: HashMap<String, Vec<Vec<Value>>> = HashMap::new();

    // flush seed infos
    for (seed, recordfiles) in seed_files {
        // let mut config_metrics = vec![];
        let mut count = recordfiles.len();
        if !seeds_metrics_cache.contains_key(&seed) {
            seeds_metrics_cache.insert(
                seed.clone(),
                get_seed_metrics(&seed).map_or(vec![], |v| v)
            );
        }
        let config_metrics = seeds_metrics_cache.get_mut(&seed).unwrap();
        for (configstr, f) in recordfiles.iter().rev() {
            let mut read_data = || {
                let mut records: Records = serde_json
                    ::from_str(&fs::read_to_string(format!("records/{}", f.file_name)).unwrap())
                    .unwrap();
                count -= 1;
                log::info!("deserialed one record file: {}, left:{}", f.file_name, count);
                if records.frames.len() < 999 {
                    return None;
                }
                let (
                    cost_per_req,
                    time_per_req,
                    score,
                    coldstart_time_per_req,
                    waitsche_time_per_req,
                    datarecv_time_per_req,
                    exe_time_per_req,
                ) = {
                    let last_frame = records.frames.iter_mut().rev().next().unwrap();
                    let cost_per_req = last_frame[FRAME_IDX_COST].take();
                    let time_per_req = last_frame[FRAME_IDX_REQ_DONE_TIME_AVG].take();
                    let score = last_frame[FRAME_IDX_SCORE].clone();
                    let coldstart_time_per_req =
                        last_frame[FRAME_IDX_REQ_WAIT_COLDSTART_TIME].clone();
                    let waitsche_time_per_req = last_frame[FRAME_IDX_REQ_WAIT_SCHE_TIME].clone();
                    let datarecv_time_per_req = last_frame[FRAME_IDX_REQ_DATA_RECV_TIME].clone();
                    let exe_time_per_req = last_frame[FRAME_IDX_REQ_EXE_TIME].clone();
                    // drop(last_frame);
                    (
                        cost_per_req,
                        time_per_req,
                        score,
                        coldstart_time_per_req,
                        waitsche_time_per_req,
                        datarecv_time_per_req,
                        exe_time_per_req,
                    )
                };

                // let req_done = last_frame[FRAME_IDX_DONE_REQ_COUNT].take();
                let rps =
                    records.frames
                        .iter()
                        .map(|f| f[FRAME_IDX_DONE_REQ_COUNT].as_f64().unwrap())
                        .sum::<f64>() / (records.frames.len() as f64);

                let one_config_info: Vec<Value> = vec![
                    configstr.clone().into(),
                    cost_per_req,
                    time_per_req,
                    score,
                    rps.into(),
                    coldstart_time_per_req,
                    waitsche_time_per_req,
                    datarecv_time_per_req,
                    exe_time_per_req,
                    f.time_str.clone().into()
                ];
                Some(one_config_info)
            };
            if
                let Some(update) = config_metrics
                    .iter_mut()
                    .filter(|config| config[0].as_str().unwrap() == &*configstr)
                    .next()
            {
                if update[9].as_str().unwrap() < f.time_str.as_str() {
                    if let Some(one_config_info) = read_data() {
                        *update = one_config_info;
                    }
                }
            } else {
                if let Some(one_config_info) = read_data() {
                    config_metrics.push(one_config_info);
                }
            }
        }
        let mut file = File::options()
            .append(false)
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(format!("records/seed_{}.json", seed))
            .unwrap();
        file.write_all(serde_json::to_string(&config_metrics).unwrap().as_bytes()).unwrap();
    }
}

/// used sync io operation, use spawn_blocking
/// return: seed->[[configstr, cost, time, score]...]
pub fn get_seeds_metrics<'a>(
    seeds: impl Iterator<Item = &'a String>
) -> HashMap<String, Vec<Vec<Value>>> {
    let mut seeds_metrics = HashMap::new();
    for seed in seeds {
        if let Some(metrics) = get_seed_metrics(seed) {
            seeds_metrics.insert(seed.clone(), metrics);
        }
    }
    seeds_metrics
}

/// return: [[configstr, cost, time, score]...]
pub fn get_seed_metrics(seed: &String) -> Option<Vec<Vec<Value>>> {
    let mut seed_metrics = vec![];
    if
        let Ok(mut file) = File::options()
            .read(true)
            .write(true)
            .append(false)
            .open(format!("records/seed_{}.json", seed))
    {
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        if content.len() == 0 {
            return None;
        }
        let records: Vec<Vec<Value>> = serde_json::from_str(&content).unwrap_or_else(|_| {
            panic!("deserial failed: {}", content);
        });
        for record in records {
            seed_metrics.push(record);
        }
        Some(seed_metrics)
    } else {
        None
    }
}
