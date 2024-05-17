use crate::apis::{
    ApiHandler, GetEnvIdResp, GetNetworkTopoReq, GetNetworkTopoResp, ResetReq, ResetResp, StepReq,
    StepResp,
};
use crate::{
    apis,
    config::Config,
    metric::{self, Records},
    sim_env::SimEnv,
};
use async_trait::async_trait;
use axum::{http::StatusCode, routing::post, Json, Router};
use moka::sync::Cache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::format;
use std::{
    cmp::min,
    collections::HashMap,
    fs::{self, File},
    io::Read,
    sync::Arc,
};

pub async fn start() {
    // build our application with a route
    // 定义了应用程序的路由，将不同的HTTP请求映射到相应的处理函数上
    let mut app = Router::new()
        .route("/collect_seed_metrics", post(collect_seed_metrics))
        .route("/get_seeds_metrics", post(get_seeds_metrics))
        .route("/history_list", post(history_list))
        .route("/history", post(history))
        .route("/meteic", post(metric));
    app = apis::add_routers(app);
    // run our app with hyper, listening globally on port 3000
    // run it with hyper on localhost:3000

    // 绑定全局的3000端口，并通过.serve()启动应用程序，从而监听和处理HTTP请求
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
// async fn root() -> &'static str {
//     "Hello, World!"
// }

// 用于定义全局静态变量，这些变量在首次访问时才进行初始化，且初始化过程只执行一次。
lazy_static! {
    /// This is an example for using doc comment attributes
    pub static ref SIM_ENVS: RwLock<HashMap<String,Mutex<SimEnv>>> = RwLock::new(HashMap::new());
    static ref HISTORY_CACHE: Cache<String,Arc<Records>> = Cache::new(100);
    static ref COLLECT_SEED_METRICS_LOCK :tokio::sync::Mutex<()>= tokio::sync::Mutex::new(());
}

pub struct ApiHandlerImpl;

#[async_trait]
impl ApiHandler for ApiHandlerImpl {
    async fn handle_get_network_topo(&self, req: GetNetworkTopoReq) -> GetNetworkTopoResp {
        let env_ids_response = self.handle_get_env_id().await;
        if let GetEnvIdResp::Exist { env_id } = env_ids_response {
            if let Some(first_env_id) = env_id.first() {
                let sim_envs =  SIM_ENVS.read() ;
                return match sim_envs.get(first_env_id) {
                    Some(env_mutex) => {
                        let env = env_mutex.lock();
                        let node_count = env.node_cnt();
                        let mut topo = Vec::with_capacity(node_count);
                        for i in 0..node_count {
                            let mut row = Vec::with_capacity(node_count);
                            for j in 0..node_count {
                                let speed = if i == j {
                                    0.0
                                } else {
                                    f64::from(env.node_get_speed_btwn(i, j))
                                };
                                row.push(speed);
                            }
                            topo.push(row);
                        }
                        GetNetworkTopoResp::Exist { topo }
                    }
                    None => GetNetworkTopoResp::NotFound {
                        msg: "Environment not found".to_string(),
                    },
                };
            }
        }
        GetNetworkTopoResp::NotFound {
            msg: "No valid environment IDs available".to_string(),
        }
    }

    async fn handle_get_env_id(&self) -> GetEnvIdResp {
        let read_guard = SIM_ENVS.read();
        let env_ids: Vec<String> = read_guard.keys().cloned().collect();
        if env_ids.is_empty() {
            GetEnvIdResp::NotFound {
                msg: "No environments found".to_string(),
            }
        } else {
            GetEnvIdResp::Exist { env_id: env_ids }
        }
    }

    async fn handle_reset(&self, req: ResetReq) -> ResetResp {
        log::info!("Reset sim env");
        // 将req.config反序列化为Config类型的数据，并使用match表达式处理反序列化结果
        match serde_json::from_value::<Config>(req.config) {
            Ok(config) => {
                // 获取配置的标识键，并尝试获取或创建该SimEnv实例
                let key = config.str();
                {
                    // 获取全局SIM_ENVS的写入锁
                    let sim_envs = SIM_ENVS.read();

                    // 如果找到了已有的模拟环境实例，则获取其独占锁，以便更新模拟环境
                    if let Some(sim_env) = sim_envs.get(&key) {
                        let mut sim_env = sim_env.lock();
                        // 调用模拟环境的帮助方法来记录指标，并刷新记录
                        sim_env.help.metric_record().flush(&sim_env);
                        // 用新的配置创建一个新的模拟环境实例
                        *sim_env = SimEnv::new(config);
                    } 
                    else {
                        // 释放读锁
                        drop(sim_envs);
                        // 获取写锁
                        let mut sim_envs = SIM_ENVS.write();
                        // 向模拟环境映射中插入一个新的模拟环境实例
                        sim_envs.insert(key.clone(), SimEnv::new(config).into());
                    }
                }
                ResetResp::Success { env_id: key }
            }
            Err(e) => ResetResp::InvalidConfig {
                msg: format!("Invalid config: {}", e),
            },
        }
    }

    async fn handle_step(&self, StepReq { env_id, action }: StepReq) -> StepResp {
        let key = env_id;
        // log::info!("Step sim env");

        // 获取全局SIM_ENVS的读取锁
        let sim_envs = SIM_ENVS.read();

        // 尝试获取指定env_id对应的SimEnv实例
        if let Some(sim_env) = sim_envs.get(&key) {
            // 尝试获取该SimEnv实例的独占锁
            let mut sim_env = sim_env.lock();

            // 调用SimEnv实例的step方法
            let (score, state) = tokio::task::block_in_place(|| sim_env.step(action as u32));

            // insert your application logic here
            // 根据步进操作的结果，返回StepResp::Success，其中包含得分、状态和停止标志，停止标志基于当前帧是否大于1000
            StepResp::Success {
                score: score as f64,
                state,
                stop: sim_env.current_frame() > 1000,
                info: "".to_owned(),
            }
        } else {
            let msg = format!("Sim env {key} not found, create new one");
            log::warn!("{}", msg);
            StepResp::EnvNotFound { msg }
        }
    }
}

// async fn history() -> (StatusCode, Json<()>) {
//     log::info!("Get history");
//     let paths = fs::read_dir("./").unwrap();

//     for path in paths {
//         println!("Name: {}", path.unwrap().path().display())
//     }

//     (StatusCode::OK, Json(()))
// }

async fn metric(Json(payload): Json<HistoryReq>) -> (StatusCode, Json<MetricResp>) {
    if HISTORY_CACHE.get(&payload.name).is_none() {
        let mut history = File::open(format!("./records/{}", &payload.name)).unwrap();
        let mut history_str = String::new();
        history.read_to_string(&mut history_str).unwrap();
        // log::info!("Get history {}", history_str);
        // let mut reader = BufReader::new(history);
        let history: Records = serde_json::from_str(&*history_str).unwrap();
        HISTORY_CACHE.insert(payload.name.clone(), Arc::new(history));
    }
    let history = HISTORY_CACHE.get(&payload.name).unwrap().clone();

    let fcnt = history.frames.len();
    let get_value_of_frame = |fi: usize, vi: usize| -> Value {
        if fi >= fcnt {
            return (0).into();
        }
        return history.frames[fi][vi].clone();
    };
    let get_avg_of_value = |vi: usize| -> f64 {
        if history.frames.len() == 0 {
            return 0.0;
        }
        history
            .frames
            .iter()
            .map(|f| {
                if vi < f.len() {
                    return f[vi].as_f64().unwrap();
                }
                0.0
            })
            .sum::<f64>()
            / (history.frames.len() as f64)
    };
    // 0 frame,
    // 1 running_reqs,
    // 2 nodes,
    // 3 req_done_time_avg,
    // 4 req_done_time_std,
    // 5 req_done_time_avg_90p,
    // 6 cost
    (
        StatusCode::OK,
        Json(MetricResp {
            cost_stable: get_value_of_frame(1997, 0),
            cost_avg: get_avg_of_value(0).into(),
            req_time_stable: get_value_of_frame(1997, 3),
            req_time_avg: get_avg_of_value(3).into(),
            cost_perform_stable: get_value_of_frame(1997, 6),
            cost_perform_avg: get_avg_of_value(6).into(),
            score: get_value_of_frame(1997, 7),
        }),
    )
}
#[derive(Serialize)]
struct MetricResp {
    cost_stable: Value,
    cost_avg: Value,
    req_time_stable: Value,
    req_time_avg: Value,
    cost_perform_stable: Value,
    cost_perform_avg: Value,
    score: Value,
}

async fn history(Json(payload): Json<HistoryReq>) -> (StatusCode, Json<HistoryResp>) {
    if HISTORY_CACHE.get(&payload.name).is_none() {
        let mut history = File::open(format!("./records/{}", &payload.name)).unwrap();
        let mut history_str = String::new();
        history.read_to_string(&mut history_str).unwrap();
        // log::info!("Get history {}", history_str);
        // let mut reader = BufReader::new(history);
        let history: Records = serde_json::from_str(&*history_str).unwrap();
        HISTORY_CACHE.insert(payload.name.clone(), Arc::new(history));
    }
    let history = HISTORY_CACHE.get(&payload.name).unwrap().clone();
    let begin = min(payload.begin, history.frames.len() - 1);
    let end = min(payload.end, history.frames.len() - 1);
    (
        StatusCode::OK,
        Json(HistoryResp {
            frames: history.frames[begin..end].to_vec(),
            begin,
            end,
            total: history.frames.len(),
        }),
    )
}

#[derive(Deserialize)]
struct HistoryReq {
    name: String,
    begin: usize,
    end: usize,
}

#[derive(Serialize)]
struct HistoryResp {
    frames: Vec<Vec<Value>>,
    begin: usize,
    end: usize,
    total: usize,
}

async fn history_list() -> (StatusCode, Json<HistoryListResp>) {
    log::info!("Get history list");

    let mut resp = HistoryListResp { list: vec![] };

    if let Ok(paths) = fs::read_dir("./records") {
        for path in paths {
            resp.list
                .push(path.unwrap().file_name().into_string().unwrap());
        }
    };

    (StatusCode::OK, Json(resp))
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct HistoryListResp {
    list: Vec<String>,
}

// async fn step_batch(Json(payload): Json<StepBatchReq>) -> (StatusCode, Json<StepBatchResp>) {
//     let key = payload.config.str();
//     // log::info!("Step sim env");

//     let mut resp = StepBatchResp {
//         stop: false,
//         info: format!("unreset for {}", key.clone()),
//         scores: Vec::new(),
//         next_state: "{{{invalid".to_owned(),
//     };
//     {
//         let sim_envs = SIM_ENVS.read().unwrap();
//         if let Some(sim_env) = sim_envs.get(&key) {
//             let mut sim_env = sim_env.lock().unwrap();
//             let (scores, next_state) = sim_env.step_batch(payload.actions);

//             // insert your application logic here
//             resp = StepBatchResp {
//                 stop: sim_env.current_frame() > 1000,
//                 info: "".to_owned(),
//                 scores,
//                 next_state,
//             };
//         }
//     }

//     (StatusCode::OK, Json(resp))
// }

async fn get_seeds_metrics(
    Json(payload): Json<Vec<String>>,
) -> (StatusCode, Json<HashMap<String, Vec<Vec<Value>>>>) {
    let res = tokio::task::spawn_blocking(move || metric::get_seeds_metrics(payload.iter()))
        .await
        .unwrap();
    (StatusCode::OK, Json(res))
}

// 执行种子度量数据的收集操作
async fn collect_seed_metrics() -> (StatusCode, Json<()>) {
    // 获取COLLECT_SEED_METRICS_LOCK的互斥锁，确保在一个时刻只有一个任务可以执行收集种子度量的操作
    let _hold = COLLECT_SEED_METRICS_LOCK.lock().await;
    // 启动一个新的阻塞任务，该任务会调用metric::group_records_by_seed函数来处理种子度量的数据收集工作
    tokio::task::spawn_blocking(metric::group_records_by_seed)
        .await
        .unwrap();
    (StatusCode::OK, ().into())
}

// async fn step_float(
//     // this argument tells axum to parse the request body
//     // as JSON into a `CreateUser` type
//     Json(payload): Json<StepFloatReq>
// ) -> (StatusCode, Json<StepResp>) {
//     let key = payload.config.str();
//     // log::info!("Step sim env");

//     let mut resp = StepResp {
//         score: 0.0,
//         state: "invalid{{{".to_owned(),
//         stop: false,
//         info: format!("unreset for {}", key.clone()),
//     };
//     {
//         let sim_envs = SIM_ENVS.read().unwrap();
//         if let Some(sim_env) = sim_envs.get(&key) {
//             let mut sim_env = sim_env.lock().unwrap();
//             let (score, state) = sim_env.step_ef(ESActionWrapper::Float(payload.action));

//             // insert your application logic here
//             resp = StepResp {
//                 score,
//                 state,
//                 stop: sim_env.current_frame() > 297,
//                 info: "".to_owned(),
//             };
//         } else {
//             log::warn!("Sim env {key} not found, create new one");
//         }
//     }
//     // let sim_env = SIM_ENV.lock().unwrap();

//     // this will be converted into a JSON response
//     // with a status code of `201 Created`
//     (StatusCode::OK, Json(resp))
// }
