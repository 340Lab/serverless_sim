use crate::{config::Config, sim_run::Scheduler};

use self::{
    faasflow::FaasFlowScheduler, fnsche::FnScheScheduler, greedy::GreedyScheduler, pass::PassScheduler, pos::PosScheduler, random::RandomScheduler
    // rule_based::{RuleBasedScheduler, ScheduleRule},
    // time_aware::TimeScheduler,
};

pub mod faasflow;
pub mod fnsche;
pub mod pass;
pub mod pos;
pub mod random;
pub mod greedy;
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
        _ => {
            return None;
        }
    }
    // if config.es.sche_faas_flow() {
    //     return Some(Box::new(FaasFlowScheduler::new()));
    // }
    // else if config.es.sche_gofs() {
    //     return Some(Box::new(RuleBasedScheduler {
    //         rule: ScheduleRule::GOFS,
    //     }));
    // }
    // else if config.es.sche_load_least() {
    //     return Some(Box::new(RuleBasedScheduler {
    //         rule: ScheduleRule::LeastLoad,
    //     }));
    // }
    // else if config.es.sche_random() {
    //     return Some(Box::new(RuleBasedScheduler {
    //         rule: ScheduleRule::Random,
    //     }));
    // }
    // else if config.es.sche_round_robin() {
    //     return Some(Box::new(RuleBasedScheduler {
    //         rule: ScheduleRule::RoundRobin(9999),
    //     }));
    // }
    // else if config.es.sche_pass() {
    //     return Some(Box::new(PassScheduler::new()));
    // }
    // else if config.es.sche_rule() {
    //     return Some(Box::new(PosScheduler::new()));
    // }
    // else if config.es.sche_fnsche() {
    //     return Some(Box::new(FnScheScheduler::new()));
    // }
    // else if config.es.sche_time() {
    //     return Some(Box::new(TimeScheduler::new()));
    // }
    None
}
