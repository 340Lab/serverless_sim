use std::{env::set_var, time::Duration};

mod actions;
mod algos;
mod apis;
mod config;
mod env_gc;
mod es;
mod fn_dag;
mod metric;
mod network;
mod node;
mod output;
mod request;
mod scale;
mod sche;
mod score;
mod sim_env;
mod sim_run;
mod sim_timer;
mod state;
mod util;

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

const REQUEST_GEN_FRAME_INTERVAL: usize = 10;

const NODE_SCORE_CPU_WEIGHT: f32 = 0.5;

const NODE_SCORE_MEM_WEIGHT: f32 = 0.5;

const NODE_CNT: usize = 30;

const CONTAINER_BASIC_MEM: f32 = 199.0;

const NODE_LEFT_MEM_THRESHOLD: f32 = 2500.0;
