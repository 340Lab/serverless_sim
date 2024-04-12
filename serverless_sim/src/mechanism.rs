use crate::{
    config::{Config, ESConfig, ModuleESConf},
    fn_dag::FnId,
    node::NodeId,
    request::ReqId,
    scale::{
        down_exec::{new_scale_down_exec, ScaleDownExec},
        num::{new_scale_num, ScaleNum},
        up_exec::{new_scale_up_exec, ScaleUpExec},
    },
    sche::prepare_spec_scheduler,
    sim_env::SimEnv,
    sim_run::Scheduler,
};

pub struct UpCmd {
    nid: NodeId,
    fnid: FnId,
    memlimit: Option<f32>,
}
pub struct DownCmd {
    nid: NodeId,
    fnid: FnId,
}
pub struct ScheCmd {
    nid: NodeId,
    reqid: ReqId,
    fnid: FnId,
    memlimit: Option<f32>,
}

pub trait Mechanism: Send {
    fn tick(&self, env: &SimEnv) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>);
}

pub trait ConfigNewMec {
    fn new_mec(&self) -> Option<Box<dyn Mechanism>>;
}

impl ConfigNewMec for Config {
    // return none if failed
    fn new_mec(&self) -> Option<Box<dyn Mechanism>> {
        // read mechanism config
        let module_es = ModuleESConf::new();
        if !module_es.check_conf_by_module(&self.es) {
            return None;
        }

        let Some(sche) = prepare_spec_scheduler(self) else {
            return None;
        };
        let Some(scale_num) = new_scale_num(self) else {
            return None;
        };
        let Some(scale_down_exec) = new_scale_down_exec(self) else {
            return None;
        };
        let Some(scale_up_exec) = new_scale_up_exec(self) else {
            return None;
        };
        Some(Box::new(MechanismImpl {
            sche,
            scale_num,
            scale_down_exec,
            scale_up_exec,
        }))
    }
}

pub struct MechanismImpl {
    sche: Box<dyn Scheduler>,
    scale_num: Box<dyn ScaleNum>,
    scale_down_exec: Box<dyn ScaleDownExec>,
    scale_up_exec: Box<dyn ScaleUpExec>,
}

impl Mechanism for MechanismImpl {
    fn tick(&self, env: &SimEnv) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        log::warn!("not implemented");
        (vec![], vec![], vec![])
    }
}
