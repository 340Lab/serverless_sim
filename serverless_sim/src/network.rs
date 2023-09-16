use axum::{ http::StatusCode, routing::post, Json, Router };
use moka::sync::Cache;
use serde::{ Deserialize, Serialize };
use serde_json::Value;
use std::{
    cmp::{ min },
    fs::{ self, File },
    io::{ Read },
    sync::{ Arc, Mutex, RwLock },
    collections::HashMap,
};

use crate::{ metric::Records, sim_env::SimEnv, config::Config };

pub async fn start() {
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        // .route("/", get(root))
        // `POST /users` goes to `create_user`
        .route("/step", post(step))
        // .route("/step_float", post(step_float))
        // .route("/step_batch", post(step_batch))
        .route("/reset", post(reset))
        // .route("/state_score", post(state_score))
        .route("/history_list", post(history_list))
        .route("/history", post(history))
        .route("/meteic", post(metric));

    // run our app with hyper, listening globally on port 3000
    // run it with hyper on localhost:3000
    axum::Server
        ::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service()).await
        .unwrap();
}

// basic handler that responds with a static string
// async fn root() -> &'static str {
//     "Hello, World!"
// }

lazy_static! {
    /// This is an example for using doc comment attributes
    pub static ref SIM_ENVS: RwLock<HashMap<String,Mutex<SimEnv>>> = RwLock::new(HashMap::new());
    static ref HISTORY_CACHE: Cache<String,Arc<Records>> = Cache::new(100);
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
        history.frames
            .iter()
            .map(|f| {
                if vi < f.len() {
                    return f[vi].as_f64().unwrap();
                }
                0.0
            })
            .sum::<f64>() / (history.frames.len() as f64)
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
    let paths = fs::read_dir("./records").unwrap();

    let mut resp = HistoryListResp { list: vec![] };

    for path in paths {
        resp.list.push(path.unwrap().file_name().into_string().unwrap());
    }

    (StatusCode::OK, Json(resp))
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct HistoryListResp {
    list: Vec<String>,
}

async fn reset(Json(payload): Json<Config>) -> (StatusCode, ()) {
    log::info!("Reset sim env");
    payload.check_valid();
    let key = payload.str();
    {
        let sim_envs = SIM_ENVS.read().unwrap();
        if let Some(sim_env) = sim_envs.get(&key) {
            let mut sim_env = sim_env.lock().unwrap();
            sim_env.metric_record.borrow().flush();
            *sim_env = SimEnv::new(payload);
        } else {
            drop(sim_envs);
            let mut sim_envs = SIM_ENVS.write().unwrap();
            sim_envs.insert(key, SimEnv::new(payload).into());
        }
    }

    // sim_env.metric_record.borrow().flush();
    // *sim_env = SimEnv::new();

    (StatusCode::OK, ())
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

async fn step(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<StepReq>
) -> (StatusCode, Json<StepResp>) {
    let key = payload.config.str();
    // log::info!("Step sim env");

    let mut resp = StepResp {
        score: 0.0,
        state: "invalid{{{".to_owned(),
        stop: false,
        info: format!("unreset for {}", key.clone()),
    };
    let mut step = || {
        let sim_envs = SIM_ENVS.read().unwrap();
        if let Some(sim_env) = sim_envs.get(&key) {
            let mut sim_env = sim_env.lock().unwrap();
            let (score, state) = sim_env.step(payload.action);

            // insert your application logic here
            resp = StepResp {
                score,
                state,
                stop: sim_env.current_frame() > 1000,
                info: "".to_owned(),
            };
            true
        } else {
            log::warn!("Sim env {key} not found, create new one");
            false
        }
    };
    if !step() {
        let mut sim_envs = SIM_ENVS.write().unwrap();
        sim_envs.insert(key.clone(), SimEnv::new(payload.config.clone()).into());
        step();
    }

    // let sim_env = SIM_ENV.lock().unwrap();

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::OK, Json(resp))
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

#[derive(Deserialize)]
pub struct StepFloatReq {
    pub config: Config,
    pub action: f32,
}

#[derive(Deserialize)]
pub struct StepReq {
    pub config: Config,
    pub action: u32,
}

// the output to our `create_user` handler
#[derive(Serialize)]
pub struct StepResp {
    pub score: f32,
    pub state: String,
    pub stop: bool,
    pub info: String,
}

// the input to our `create_user` handler
// #[derive(Deserialize)]
// pub struct StepBatchReq {
//     pub config: Config,
//     pub actions: Vec<Vec<f32>>,
// }

// // the output to our `create_user` handler
// #[derive(Serialize)]
// pub struct StepBatchResp {
//     pub scores: Vec<f32>,
//     pub next_state: String,
//     pub stop: bool,
//     pub info: String,
// }

// async fn state_score() -> (StatusCode, Json<StateScoreResp>) {
//     let sim_env = SIM_ENV.lock().unwrap();

//     // this will be converted into a JSON response
//     // with a status code of `201 Created`
//     (
//         StatusCode::OK,
//         Json(StateScoreResp {
//             state: sim_env.state(),
//             score: sim_env.score(),
//         }),
//     )
// }

// #[derive(Serialize)]
// struct StateScoreResp {
//     score: f32,
//     state: State,
// }
