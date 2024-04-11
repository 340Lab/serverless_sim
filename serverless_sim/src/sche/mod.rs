use crate::{config::Config, sim_run::Scheduler};

use self::{
    faasflow::FaasFlowScheduler,
    fnsche::FnScheScheduler,
    pass::PassScheduler,
    pos::PosScheduler,
    rule_based::{RuleBasedScheduler, ScheduleRule},
    time_aware::TimeScheduler,
};

pub mod faasflow;
pub mod fnsche;
pub mod pass;
pub mod pos;
pub mod rule_based;
pub mod time_aware;

pub fn prepare_spec_scheduler(config: &Config) -> Option<Box<dyn Scheduler + Send>> {
    if config.es.sche_faas_flow() {
        return Some(Box::new(FaasFlowScheduler::new()));
    } 
    else if config.es.sche_gofs() {
        return Some(Box::new(RuleBasedScheduler {
            rule: ScheduleRule::GOFS,
        }));
    } 
    else if config.es.sche_load_least() {
        return Some(Box::new(RuleBasedScheduler {
            rule: ScheduleRule::LeastLoad,
        }));
    } 
    else if config.es.sche_random() {
        return Some(Box::new(RuleBasedScheduler {
            rule: ScheduleRule::Random,
        }));
    } 
    else if config.es.sche_round_robin() {
        return Some(Box::new(RuleBasedScheduler {
            rule: ScheduleRule::RoundRobin(9999),
        }));
    } 
    else if config.es.sche_pass() {
        return Some(Box::new(PassScheduler::new()));
    } 
    else if config.es.sche_rule() {
        return Some(Box::new(PosScheduler::new()));
    } 
    else if config.es.sche_fnsche() {
        return Some(Box::new(FnScheScheduler::new()));
    } 
    else if config.es.sche_time() {
        return Some(Box::new(TimeScheduler::new()));
    }
    None
}
