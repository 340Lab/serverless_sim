use bp_balance::BpBalanceScheduler;

use crate::{ config::Config, sim_run::Scheduler };

use self::{
    consistenthash::ConsistentHashScheduler, // rule_based::{RuleBasedScheduler, ScheduleRule},
    // time_aware::TimeScheduler,
    faasflow::FaasFlowScheduler,
    fnsche::FnScheScheduler,
    greedy::GreedyScheduler,
    pass::PassScheduler,
    pos::PosScheduler,
    random::RandomScheduler,
    hash::HashScheduler,
    rotate::RotateScheduler,
    ensure_scheduler::EnsureScheduler,
    load_least::LoadLeastScheduler,
    priority::PriorityScheduler
};

pub mod consistenthash;
pub mod faasflow;
pub mod fnsche;
pub mod greedy;
pub mod pass;
pub mod pos;
pub mod random;
pub mod bp_balance;
pub mod hash;
pub mod rotate;
pub mod ensure_scheduler;
pub mod load_least;
pub mod priority;


// pub mod rule_based;
// pub mod time_aware;

pub fn prepare_spec_scheduler(config: &Config) -> Option<Box<dyn Scheduler + Send>> {
    let es = &config.mech;
    // let (scale_num_name, scale_num_attr) = es.scale_num_conf();
    // let (scale_up_exec_name, scale_up_exec_attr) = es.scale_up_exec_conf();
    // let (scale_down_exec_name, scale_down_exec_attr) = es.scale_down_exec_conf();
    let (sche_name, sche_attr) = es.sche_conf();
    match &*sche_name {
        "faasflow" => {
            return Some(Box::new(FaasFlowScheduler::new()));
        }
        "pass" => {
            return Some(Box::new(PassScheduler::new()));
        }
        "pos" => {
            return Some(Box::new(PosScheduler::new(&sche_attr)));
        }
        "fnsche" => {
            return Some(Box::new(FnScheScheduler::new()));
        }
        "random" => {
            return Some(Box::new(RandomScheduler::new()));
        }
        "greedy" => {
            return Some(Box::new(GreedyScheduler::new()));
        }
        "bp_balance" => {
            return Some(Box::new(BpBalanceScheduler::new()));
        }
        "consistenthash" => {
            return Some(Box::new(ConsistentHashScheduler::new()));
        }
        "hash" => {
            return Some(Box::new(HashScheduler::new()));
        }
        "rotate" => {
            return Some(Box::new(RotateScheduler::new()));
        }
        "ensure_scheduler"=>{
            return Some(Box::new(EnsureScheduler::new()));
        }
        "load_least" => {
            return Some(Box::new(LoadLeastScheduler::new()));
        }
        "priority" => {
            return Some(Box::new(PriorityScheduler::new(&sche_attr)));
        }
        _ => {
            return None;
        }
    }
}
