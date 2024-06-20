use crate::{config::Config, sim_run::Scheduler};

use self::{
    consistenthash::ConsistentHashScheduler, // rule_based::{RuleBasedScheduler, ScheduleRule},
                                             // time_aware::TimeScheduler,
    faasflow::FaasFlowScheduler,
    fnsche::FnScheScheduler,
    greedy::GreedyScheduler,
    pass::PassScheduler,
    pos::PosScheduler,
    random::RandomScheduler,
};

pub mod consistenthash;
pub mod faasflow;
pub mod fnsche;
pub mod greedy;
pub mod pass;
pub mod pos;
pub mod random;
// pub mod rule_based;
// pub mod time_aware;

pub fn prepare_spec_scheduler(config: &Config) -> Option<Box<dyn Scheduler + Send>> {
    let es = &config.mech;
    // let (scale_num_name, scale_num_attr) = es.scale_num_conf();
    // let (scale_up_exec_name, scale_up_exec_attr) = es.scale_up_exec_conf();
    // let (scale_down_exec_name, scale_down_exec_attr) = es.scale_down_exec_conf();
    let (sche_name, _sche_attr) = es.sche_conf();
    match &*sche_name {
        "faasflow" => {
            return Some(Box::new(FaasFlowScheduler::new()));
        }
        "pass" => {
            return Some(Box::new(PassScheduler::new()));
        }
        "pos" => {
            return Some(Box::new(PosScheduler::new()));
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
        "consistenthash" => {
            return Some(Box::new(ConsistentHashScheduler::new()));
        }
        _ => {
            return None;
        }
    }
}
