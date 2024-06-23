mod actions;
mod algos;
mod apis;
mod cache;
mod config;
mod env_gc;
mod fn_dag;
mod mechanism;
mod mechanism_conf;
mod mechanism_thread;
mod metric;
mod network;
mod node;
mod output;
mod request;
mod scale;
mod sche;
mod score;
mod sim_env;
mod sim_events;
mod sim_loop;
mod sim_run;
mod sim_timer;
mod state;
mod util;
mod with_env_sub;

use mechanism_conf::ModuleMechConf;

use std::{env::set_var, time::Duration};

#[macro_use]
extern crate lazy_static;

#[tokio::main]
async fn main() {
    set_var("RUST_LOG", "debug,error,warn,info");
    // 遇到 panic 时自动打印回溯信息
    set_var("RUST_BACKTRACE", "1");
    // 指定要记录的日志级别
    // 读取之前设置的 RUST_LOG 环境变量, 初始化 env_logger 日志记录器
    env_logger::init();
    std::thread::sleep(Duration::from_secs(1));
    output::print_logo();
    // 启动垃圾回收（Garbage Collection, GC）机制
    env_gc::start_gc();
    ModuleMechConf::new().export_module_file();
    // parse_arg::parse_arg();
    network::start().await;
}

const REQUEST_GEN_FRAME_INTERVAL: usize = 10;

const NODE_SCORE_CPU_WEIGHT: f32 = 0.5;

const NODE_SCORE_MEM_WEIGHT: f32 = 0.5;

const NODE_CNT: usize = 10;

const CONTAINER_BASIC_MEM: f32 = 199.0;

const NODE_LEFT_MEM_THRESHOLD: f32 = 2500.0;
