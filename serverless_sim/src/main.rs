use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    hash::Hash,
    rc::Rc,
};

use daggy::{
    petgraph::visit::{Topo, Visitable},
    Dag, NodeIndex, Parents, Walker,
};

mod actions;
mod algos;
mod fn_dag;
mod network;
mod node;
mod request;
mod sim_env;
mod sim_scaler;
mod sim_schedule;
mod sim_score;
mod sim_state;
mod util;

#[tokio::main]
async fn main() {
    network::start().await;
}

const SPEED_SIMILAR_THRESHOLD: f32 = 0.1;

const REQUEST_GEN_FRAME_INTERVAL: usize = 20;

const NODE_SCORE_CPU_WEIGHT: f32 = 0.5;

const NODE_SCORE_MEM_WEIGHT: f32 = 0.5;
