use std::{ env::set_var, time::Duration };

mod actions;
mod algos;
mod fn_dag;
mod metric;
mod network;
mod node;
mod output;
// mod parse_arg;
mod request;
mod sim_env;
mod sim_scale_executor;
mod sim_scale_from_zero;
mod sim_scaler;
// mod sim_scaler_ai;
mod sim_scaler_hpa;
// mod sim_scaler_lass;
mod sim_schedule;
mod sim_score;
mod sim_state;
mod sim_ef;
mod util;
mod sim_ef_state;
mod env_gc;
mod operation;
mod sim_ef_faas_flow;
mod sim_ef_ai;
mod sim_ef_lass;
mod sim_ef_fnsche;
mod sim_ef_hpa;
mod scale_down_policy;

#[macro_use]
extern crate lazy_static;

#[tokio::main]
async fn main() {
    set_var("RUST_LOG", "debug,error,warn,info");
    set_var("RUST_BACKTRACE", "1");
    env_logger::init();
    std::thread::sleep(Duration::from_secs(1));
    output::print_logo();
    env_gc::start_gc();
    // parse_arg::parse_arg();
    network::start().await;
}

const SPEED_SIMILAR_THRESHOLD: f32 = 0.1;

const REQUEST_GEN_FRAME_INTERVAL: usize = 30;

const NODE_SCORE_CPU_WEIGHT: f32 = 0.5;

const NODE_SCORE_MEM_WEIGHT: f32 = 0.5;

const NODE_CNT: usize = 30;

const CONTAINER_BASIC_MEM: f32 = 199.0;

const NODE_LEFT_MEM_THRESHOLD: f32 = 2500.0;
