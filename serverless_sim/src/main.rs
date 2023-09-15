use std::{ env::set_var, time::Duration };

mod actions;
mod algos;
mod fn_dag;
mod metric;
mod network;
mod node;
mod output;
mod request;
mod sim_env;
mod scale_executor;
mod scaler;
mod schedule;
mod score;
mod state;
mod es;
mod util;
// mod es_state;
mod env_gc;
mod es_faas_flow;
mod es_ai;
mod es_lass;
mod es_fnsche;
mod es_hpa;
mod scale_down_policy;
mod config;

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
