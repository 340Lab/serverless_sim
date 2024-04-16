use crate::{config::Config, fn_dag::FnId, mechanism::UpCmd, sim_env::SimEnv};

pub mod least_task;
pub mod no;

pub trait ScaleUpExec: Send {
    fn exec_scale_up(&self, target_cnt: usize, fnid: FnId, env: &SimEnv) -> Vec<UpCmd>;
}

pub fn new_scale_up_exec(conf: &Config) -> Option<Box<dyn ScaleUpExec>> {
    let es = &conf.mech;
    let (scale_up_exec_name, scale_up_exec_attr) = es.scale_up_exec_conf();
    match &*scale_up_exec_name {
        "least_task" => {
            return Some(Box::new(least_task::LeastTaskScaleUpExec::new()));
        }
        "no" => {
            return Some(Box::new(no::NoScaleUpExec));
        }
        _ => {
            return None;
        }
    }
}
