use std::env::set_var;

mod actions;
mod algos;
mod fn_dag;
mod network;
mod node;
mod output;
mod parse_arg;
mod request;
mod sim_env;
mod sim_scale_executor;
mod sim_scale_from_zero;
mod sim_scaler;
mod sim_scaler_ai;
mod sim_scaler_hpa;
mod sim_schedule;
mod sim_score;
mod sim_state;
mod util;

#[macro_use]
extern crate lazy_static;

#[tokio::main]
async fn main() {
    set_var("RUST_LOG", "debug,error,warn,info");
    set_var("RUST_BACKTRACE", "1");
    env_logger::init();
    output::print_logo();
    parse_arg::parse_arg();
    network::start().await;
}

const SPEED_SIMILAR_THRESHOLD: f32 = 0.1;

const REQUEST_GEN_FRAME_INTERVAL: usize = 100;

const NODE_SCORE_CPU_WEIGHT: f32 = 0.5;

const NODE_SCORE_MEM_WEIGHT: f32 = 0.5;

const NODE_CNT: usize = 30;

const CONTAINER_BASIC_MEM: f32 = 199.0;

const NODE_LEFT_MEM_THRESHOLD: f32 = 2500.0;
