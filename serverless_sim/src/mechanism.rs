use std::{
    cell::{RefCell, RefMut},
    collections::HashMap,
};

use crate::{
    actions::ESActionWrapper,
    config::Config,
    fn_dag::FnId,
    mechanism_conf::{MechConfig, ModuleMechConf},
    node::NodeId,
    request::ReqId,
    scale::{
        down_exec::{new_scale_down_exec, ScaleDownExec},
        num::{
            down_filter::{CarefulScaleDownFilter, ScaleFilter},
            new_scale_num, ScaleNum,
        },
        up_exec::{new_scale_up_exec, ScaleUpExec},
    },
    sche::prepare_spec_scheduler,
    sim_env::SimEnv,
    sim_run::Scheduler,
};

pub struct UpCmd {
    pub nid: NodeId,
    pub fnid: FnId,
}
pub struct DownCmd {
    pub nid: NodeId,
    pub fnid: FnId,
}
pub struct ScheCmd {
    pub nid: NodeId,
    pub reqid: ReqId,
    pub fnid: FnId,
    // TODO: memlimit
    pub memlimit: Option<f32>,
}

pub const SCHE_NAMES: [&'static str; 4] = [
    "faasflow", "pass", "pos", "fnsche",
    // "gofs",
    // "load_least",
    // "random",
];
pub const SCALE_NUM_NAMES: [&'static str; 4] = ["no", "hpa", "lass", "temp_scaler"];
pub const SCALE_DOWN_EXEC_NAMES: [&'static str; 1] = ["default"];
pub const SCALE_UP_EXEC_NAMES: [&'static str; 2] = ["least_task", "no"];
pub const MECH_NAMES: [&'static str; 3] = ["no_scale", "scale_sche_separated", "scale_sche_joint"];
pub const FILTER_NAMES: [&'static str; 1] = ["careful_down"];

pub trait Mechanism: Send {
    fn step(
        &self,
        env: &SimEnv,
        raw_action: ESActionWrapper,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>);
}

pub trait ConfigNewMec {
    fn new_mec(&self) -> Option<MechanismImpl>;
}

impl ConfigNewMec for Config {
    // return none if failed
    fn new_mec(&self) -> Option<MechanismImpl> {
        // read mechanism config
        let module_es = ModuleMechConf::new();
        if !module_es.check_conf_by_module(&self.mech) {
            return None;
        }

        fn check_config(
            conf: &MechConfig,
            allow_sche: &Vec<&'static str>,
            allow_scale_num: &Vec<&'static str>,
            allow_scale_down_exec: &Vec<&'static str>,
            allow_scale_up_exec: &Vec<&'static str>,
        ) -> bool {
            if !allow_sche.contains(&&*conf.sche_conf().0) {
                log::warn!(
                    "mech_type {} not support sche {}",
                    conf.mech_type().0,
                    conf.sche_conf().0
                );
                return false;
            }
            if !allow_scale_num.contains(&&*conf.scale_num_conf().0) {
                log::warn!(
                    "mech_type {} not support scale_num {}",
                    conf.mech_type().0,
                    conf.scale_num_conf().0
                );
                return false;
            }
            if !allow_scale_down_exec.contains(&&*conf.scale_down_exec_conf().0) {
                log::warn!(
                    "mech_type {} not support scale_down_exec {}",
                    conf.mech_type().0,
                    conf.scale_down_exec_conf().0
                );
                return false;
            }
            if !allow_scale_up_exec.contains(&&*conf.scale_up_exec_conf().0) {
                log::warn!(
                    "mech_type no_scale not support scale_up_exec {}",
                    conf.scale_up_exec_conf().0
                );
                return false;
            }
            true
        }

        // check conf relation
        match &*self.mech.mech_type().0 {
            "no_scale" => {
                let allow_sche = vec!["faasflow", "pass", "fnsche"];
                let allow_scale_num = vec!["no"];
                let allow_scale_down_exec = vec!["default"];
                let allow_scale_up_exec = vec!["no"];

                if !check_config(
                    &self.mech,
                    &allow_sche,
                    &allow_scale_num,
                    &allow_scale_down_exec,
                    &allow_scale_up_exec,
                ) {
                    return None;
                }
            }
            "scale_sche_separated" => {
                return None;
            }
            "scale_sche_joint" => {
                let allow_sche = vec!["pos"];
                let allow_scale_num = vec!["hpa", "lass", "temp_scaler"];
                let allow_scale_down_exec = vec!["default"];
                let allow_scale_up_exec = vec!["least_task"];
                if !check_config(
                    &self.mech,
                    &allow_sche,
                    &allow_scale_num,
                    &allow_scale_down_exec,
                    &allow_scale_up_exec,
                ) {
                    return None;
                }
            }
            _ => {
                panic!("mech_type not supported {}", self.mech.mech_type().0);
            }
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
        let filters = FILTER_NAMES
            .iter()
            .filter(|v| self.mech.filter.get(**v).unwrap().is_some())
            .map(|filters| {
                let filter = match *filters {
                    "careful_down" => CarefulScaleDownFilter::new(),
                    _ => {
                        panic!("filter not supported {}", filters);
                    }
                };
                let filter: Box<dyn ScaleFilter> = Box::new(filter);
                RefCell::new(filter)
            })
            .collect();
        Some(MechanismImpl {
            sche: RefCell::new(sche),
            scale_num: RefCell::new(scale_num),
            scale_down_exec: RefCell::new(scale_down_exec),
            scale_up_exec: RefCell::new(scale_up_exec),
            filters,
            fn_scale_num: RefCell::new(HashMap::new()),
        })
    }
}

pub struct MechanismImpl {
    sche: RefCell<Box<dyn Scheduler>>,
    scale_num: RefCell<Box<dyn ScaleNum>>,
    scale_down_exec: RefCell<Box<dyn ScaleDownExec>>,
    scale_up_exec: RefCell<Box<dyn ScaleUpExec>>,
    filters: Vec<RefCell<Box<dyn ScaleFilter>>>,
    fn_scale_num: RefCell<HashMap<FnId, usize>>,
}

impl Mechanism for MechanismImpl {

    // 执行步进操作前的准备，根据配置选择调度、扩缩容模式
    fn step(
        &self,
        env: &SimEnv,
        raw_action: ESActionWrapper,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        match &*env.help.config().mech.mech_type().0 {
            "no_scale" => self.step_no_scaler(env, raw_action),
            "scale_sche_separated" => self.step_scale_sche_separated(env, raw_action),

            // 目前只实现了这个
            "scale_sche_joint" => self.step_scale_sche_joint(env, raw_action),
            _ => {
                panic!(
                    "mech_type not supported {}",
                    env.help.config().mech.mech_type().0
                )
            }
        }
    }
}

impl MechanismImpl {
    pub fn scale_down_exec<'a>(&'a self) -> RefMut<'a, Box<dyn ScaleDownExec>> {
        self.scale_down_exec.borrow_mut()
    }
    pub fn scale_up_exec<'a>(&'a self) -> RefMut<'a, Box<dyn ScaleUpExec>> {
        self.scale_up_exec.borrow_mut()
    }
    // pub fn scale_num<'a>(&'a self) -> RefMut<'a, Box<dyn ScaleNum>> {
    //     self.scale_num.borrow_mut()
    // }
    // no scale
    // 表示只进行调度，不主动对容器数量进行干涉
    fn step_no_scaler(
        &self,
        env: &SimEnv,
        raw_action: ESActionWrapper,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        log::info!("step_no_scaler");
        let (up_cmds, sche_cmds, down_cmds) = self.sche.borrow_mut().schedule_some(env);
        (up_cmds, down_cmds, sche_cmds)
    }

    fn update_scale_num(&self, env: &SimEnv, fnid: FnId, action: &ESActionWrapper) {
        let mut target = self.scale_num.borrow_mut().scale_for_fn(env, fnid, action);
        for filter in self.filters.iter() {
            target = filter
                .borrow_mut()
                .filter_desired(fnid, target, env.fn_container_cnt(fnid));
        }
        self.fn_scale_num.borrow_mut().insert(fnid, target);
    }

    pub fn scale_num(&self, fnid: FnId) -> usize {
        self.fn_scale_num.borrow().get(&fnid).unwrap().clone()
    }

    // scale and sche separated
    // 先进行扩缩容，再进行调度
    fn step_scale_sche_separated(
        &self,
        env: &SimEnv,
        raw_action: ESActionWrapper,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        log::info!("step_separated");
        let mut up_cmds = Vec::new();
        let mut down_cmds = Vec::new();

        // 遍历每个函数
        for func in env.core.fns().iter() {
            self.update_scale_num(env, func.fn_id, &raw_action);
            let target = self.scale_num(func.fn_id);

            let cur = env.fn_container_cnt(func.fn_id);

            // 扩容
            if target > cur {
                up_cmds.extend(
                    self.scale_up_exec
                        .borrow_mut()
                        .exec_scale_up(target, func.fn_id, env),
                );
            }
            // 缩容 
            else if target < cur {
                down_cmds.extend(self.scale_down_exec.borrow_mut().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                ));
            }
        }

        // 进行调度
        let (up, sche_cmds, down) = self.sche.borrow_mut().schedule_some(env);
        
        // 扩缩容和调度分离，所以要求调度后不能再主动调节容器数量
        assert!(up.is_empty());
        assert!(down.is_empty());

        (up_cmds, down_cmds, sche_cmds)
    }

    // scale and sche joint
    fn step_scale_sche_joint(
        &self,
        env: &SimEnv,
        raw_action: ESActionWrapper,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        
        // 遍历每个函数（每一帧都对每个函数进行scale_for_fn，每个函数都进行扩缩容判断）
        for func in env.core.fns().iter() {
            self.update_scale_num(env, func.fn_id, &raw_action);

            // 获取对该函数当前容器数量
            let cur = env.fn_container_cnt(func.fn_id);
            let tar = self.scale_num(func.fn_id);

            // log::info!("scale fn {} from {} to {}", func.fn_id, cur, tar);
            // 不进行扩缩容，在调度时候一起进行
            log::info!("scale fn {} from {} to {}", func.fn_id, cur, tar);
        }
        
        // 获得扩容、调度、缩容指令
        let (up_cmds, sche_cmds, down_cmds) = self.sche.borrow_mut().schedule_some(env);

        (up_cmds, down_cmds, sche_cmds)
    }
}
