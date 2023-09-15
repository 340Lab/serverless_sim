use std::{ cell::RefMut, collections::{ BTreeMap, HashMap, VecDeque } };

use enum_as_inner::EnumAsInner;

use crate::{
    sim_env::SimEnv,
    request::ReqId,
    fn_dag::FnId,
    algos::ContainerMetric,
    node::NodeId,
    actions::{ RawAction, ESActionWrapper },
    schedule::{ Scheduler },
    es_lass::LassESScaler,
    es_fnsche::FnScheScaler,
    es_faas_flow::FaasFlowScheduler,
    es_hpa::HpaESScaler,
    es_ai,
    config::Config,
};

pub trait ActionEffectStage {
    fn prepare_next(&mut self) -> bool;
}

pub trait ESScaler {
    fn scale_for_fn(&mut self, env: &SimEnv, fnid: FnId, metric: &ContainerMetric);
}
#[derive(Debug)]
pub struct StageScaleForFns {
    pub current_fnid: Option<FnId>,
    pub fn_cnt: usize,
    pub fn_need_schedule: HashMap<FnId, ContainerMetric>,
    // pub fns: HashMap<FnId, ContainerMetric>,
    // pub current_fn_to_scale: Option<(FnId, ContainerMetric)>,
    // // action target_size
    // pub scaled: Vec<(FnId, usize, RawAction)>,
}
impl ActionEffectStage for StageScaleForFns {
    fn prepare_next(&mut self) -> bool {
        if let Some(fnid) = self.current_fnid.as_mut() {
            *fnid += 1;
        } else {
            self.current_fnid = Some(0);
        }

        if self.current_fnid.unwrap() >= self.fn_cnt {
            return false;
        }

        // log::info!("prepare_next fnid: {:?}", self.current_fnid.unwrap());
        true
        // let fnid = if let Some((&fnid, _metric)) = self.ready_2_schedule.iter().next() {
        //     fnid
        // } else {
        //     self.current_fn_to_scale = None;
        //     return false;
        // };
        // let metric = self.ready_2_schedule.remove(&fnid).unwrap();
        // self.current_fn_to_scale = Some((fnid, metric));
        // true
    }
}

#[derive(Debug)]
pub struct StageSchedule {
    ready_2_schedule: BTreeMap<ReqId, VecDeque<FnId>>,
    pub next_2_schedule: (ReqId, FnId),
    pub scheduled: Vec<(ReqId, FnId, Option<NodeId>, RawAction)>,
}

impl StageSchedule {
    fn new(env: &SimEnv) -> Self {
        let map = env.algo_collect_req_ready_2_schedule();
        let new = Self {
            ready_2_schedule: map,
            next_2_schedule: (0, 0),
            scheduled: Vec::new(),
        };
        new
    }
}
impl ActionEffectStage for StageSchedule {
    fn prepare_next(&mut self) -> bool {
        if self.ready_2_schedule.len() > 0 {
            let next: ReqId = *self.ready_2_schedule.iter().next().unwrap().0;
            let next_fn: FnId = self.ready_2_schedule.get_mut(&next).unwrap().pop_front().unwrap();
            self.next_2_schedule = (next, next_fn);
            if self.ready_2_schedule.get(&next).unwrap().len() == 0 {
                self.ready_2_schedule.remove(&next);
            }
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct StageScaleDown {
    pub idle_containers: Vec<(NodeId, FnId)>,
    pub cur_idle_container_idx: isize,
    pub records: Vec<(NodeId, FnId, RawAction)>,
}

impl StageScaleDown {
    fn new(env: &SimEnv) -> Self {
        let mut idle_containers = Vec::new();
        let nodes = env.nodes.borrow();
        for node in nodes.iter() {
            for (&fnid, container) in node.fn_containers.iter() {
                if container.is_idle() {
                    idle_containers.push((node.node_id(), fnid));
                }
            }
        }

        let new = Self {
            idle_containers,
            cur_idle_container_idx: 0,
            records: Vec::new(),
        };
        new
    }
    pub fn cur_container(&self) -> Option<(NodeId, FnId)> {
        if self.cur_idle_container_idx >= 0 {
            Some(self.idle_containers[self.cur_idle_container_idx as usize])
        } else {
            None
        }
    }
}

impl ActionEffectStage for StageScaleDown {
    fn prepare_next(&mut self) -> bool {
        self.cur_idle_container_idx += 1;
        (self.cur_idle_container_idx as usize) < self.idle_containers.len()
    }
}

#[derive(EnumAsInner, Debug)]
pub enum EFStage {
    FrameBegin,
    ScaleForFns(StageScaleForFns),
    Schedule(StageSchedule),
    SimCompute,
    ScaleDown(StageScaleDown),
}

impl EFStage {
    pub fn type_str(&self) -> &'static str {
        match self {
            EFStage::FrameBegin => "FrameBegin",
            EFStage::ScaleForFns(_) => "ScaleForFns",
            EFStage::Schedule(_) => "Schedule",
            EFStage::SimCompute => "SimCompute",
            EFStage::ScaleDown(_) => "ScaleDown",
        }
    }
}

pub struct ESState {
    pub step_cnt: usize,
    pub stage: EFStage,
    pub computed: bool,
}
impl ESState {
    pub fn new() -> Self {
        Self {
            stage: EFStage::FrameBegin,
            computed: false,
            step_cnt: 0,
        }
    }
    fn unwrap_aes_prepare_next(&mut self) -> bool {
        match self.stage {
            EFStage::FrameBegin =>
                panic!("FrameBegin stage should not call unwrap_aes_prepare_next"),
            EFStage::ScaleForFns(ref mut stage) => stage.prepare_next(),
            EFStage::Schedule(ref mut stage) => stage.prepare_next(),
            EFStage::SimCompute =>
                panic!("SimCompute stage should not call unwrap_aes_prepare_next"),
            EFStage::ScaleDown(ref mut stage) => stage.prepare_next(),
        }
    }
    fn is_action_effect_stage(&self) -> bool {
        match self.stage {
            EFStage::FrameBegin => false,
            EFStage::ScaleForFns(_) => true,
            EFStage::Schedule(_) => true,
            EFStage::SimCompute => false,
            EFStage::ScaleDown(_) => true,
        }
    }
    // return true if arrive next action_effect_stage
    pub fn trans_stage(&mut self, env: &SimEnv) -> bool {
        loop {
            if self.stage.is_frame_begin() {
                // collect scale infos
                self.stage = EFStage::ScaleForFns(StageScaleForFns {
                    current_fnid: None,
                    fn_cnt: env.fns.borrow().len(),
                    fn_need_schedule: env.algo_collect_ready_2_schedule_metric(),
                    // scaled: Vec::new(),
                    // current_fn_to_scale: None,
                });
                if self.stage.as_scale_for_fns_mut().unwrap().prepare_next() {
                    return true;
                }
            } else if self.stage.is_scale_for_fns() {
                self.stage = EFStage::Schedule(StageSchedule::new(env));
                if self.stage.as_schedule_mut().unwrap().prepare_next() {
                    return true;
                }
            } else if self.stage.is_schedule() {
                self.stage = EFStage::SimCompute;
                // break common stage to run simu compute
                return false;
            } else if self.stage.is_sim_compute() {
                self.stage = EFStage::FrameBegin; // AiEFStage::ScaleDown(StageScaleDown::new(env));
                // if self.stage.as_scale_down_mut().unwrap().prepare_next() {
                // return true;
                // }
            }
            //  else if self.stage.is_scale_down() {
            //     self.stage = AiEFStage::FrameBegin;
            //     // break common stage to run simu compute
            return false;
            // }
        }
    }
}

pub fn prepare_spec_scheduler(config: &Config) -> Option<Box<dyn Scheduler + Send>> {
    if config.es.sche_faas_flow() {
        return Some(Box::new(FaasFlowScheduler::new()));
    }
    None
}

pub fn prepare_spec_scaler(config: &Config) -> Option<Box<dyn ESScaler + Send>> {
    let es = &config.es;

    if es.scale_lass() {
        return Some(Box::new(LassESScaler::new()));
    } else if es.sche_fnsche() {
        return Some(Box::new(FnScheScaler::new()));
    } else if es.scale_hpa() {
        return Some(Box::new(HpaESScaler::new()));
    }

    None
}

impl SimEnv {
    // return false if schedule failed
    fn step_schedule(&self, raw_action: u32, stage: &mut StageSchedule) -> bool {
        let mut ret = true;
        let (reqid, fnid) = stage.next_2_schedule;
        if raw_action > ((self.node_cnt() - 1) as u32) {
            stage.scheduled.push((reqid, fnid, None, raw_action));
        } else {
            let nodeid = raw_action as usize;
            if self.node(nodeid).container(fnid).is_some() {
                assert!(self.request_mut(reqid).req_id == reqid, "reqid not match");
                self.schedule_reqfn_on_node(&mut self.request_mut(reqid), fnid, nodeid);
            } else {
                ret = false;
            }
            stage.scheduled.push((reqid, fnid, Some(nodeid), raw_action));
        }
        ret
    }

    // fn step_scale_down(&self, raw_action: RawAction, stage: &mut StageScaleDown) {
    //     let (nodeid, fnid) = stage.cur_container().unwrap();
    //     if RawActionHelper(raw_action).is_scale_down() {
    //         self.scale_executor
    //             .borrow_mut()
    //             .scale_down(self, ScaleOption::new().for_spec_node_fn(nodeid, fnid));
    //     }
    //     stage.records.push((nodeid, fnid, raw_action));
    // }

    /// return (scores, states)
    // pub fn step_aief_batch(&self, mut raw_actions: Vec<Vec<f32>>) -> (Vec<f32>, String) {
    //     let stage = self.aief_state.as_ref().unwrap().borrow_mut().stage.type_str();
    //     let mut scores = Vec::new();
    //     let mut state = String::new();
    //     for action in raw_actions {
    //         if self.aief_state.as_ref().unwrap().borrow_mut().stage.type_str() == stage {
    //             // same stage
    //             let (score, state_) = self.step_aief(action);
    //             scores.push(score);
    //             state = state_;
    //         } else {
    //             scores.push(0.0);
    //             // states.push(String::new());
    //         }
    //     }
    //     let score = scores
    //         .iter()
    //         .map(|v| *v)
    //         .sum();
    //     (vec![score], state)
    // }

    /// raw_action[0] container count
    pub fn step_es(&mut self, raw_action: ESActionWrapper) -> (f32, String) {
        self.avoid_gc();

        let mut ef_state: RefMut<'_, ESState> = self.ef_state.borrow_mut();

        let mut frame_score = self.score();
        let mut action_score = 0.0;
        let mut action_done = false;
        // 只有确定了下一个action，才会有可以返回的state

        let config_es = || { &self.config.es };

        loop {
            if ef_state.stage.is_frame_begin() {
                if (self.current_frame() == 0 && ef_state.computed) || self.current_frame() > 0 {
                    // log::info!("score: {} frame:{}", score, self.current_frame());
                    self.on_frame_end();
                    log::info!("frame end");
                    if self.current_frame() > 1000 {
                        break;
                    }
                }
                log::info!("frame begin");
                self.on_frame_begin();

                //没有正在调度的请求了，分配一个正在调度的请求
                self.req_sim_gen_requests();
                ef_state.trans_stage(self);
            } else if ef_state.stage.is_scale_for_fns() {
                // faas flow don't do scale for fns
                if config_es().sche_faas_flow() {
                    ef_state.trans_stage(self);
                    continue;
                }
                if config_es().scale_ai() {
                    if
                        !es_ai::step_scale(
                            self,
                            &raw_action,
                            &mut action_done,
                            &mut action_score,
                            &mut ef_state
                        )
                    {
                        break;
                    }
                } else {
                    let fn_2_schedule_metrics = self.algo_collect_ready_2_schedule_metric();
                    let fn_schedule_metrics = self.algo_get_fn_all_scheduled_metric(
                        &fn_2_schedule_metrics
                    );
                    for (&fnid, metric) in fn_2_schedule_metrics
                        .iter()
                        .chain(fn_schedule_metrics.iter().map(|(p1, p2)| (p1, p2))) {
                        self.spec_ef_scaler
                            .borrow_mut()
                            .as_mut()
                            .unwrap()
                            .scale_for_fn(self, fnid, metric);
                    }
                    ef_state.trans_stage(self);
                }
            } else if ef_state.stage.is_schedule() {
                log::info!("schedule");
                if self.config.es.sche_ai() {
                    if action_done {
                        // next action effect stage is prepared
                        break;
                    }
                    action_done = true;
                    let action = match raw_action {
                        // ESActionWrapper::Float(raw_action) => (raw_action * 31.0) as u32,
                        ESActionWrapper::Int(raw_action) => raw_action,
                    };
                    if !self.step_schedule(action, ef_state.stage.as_schedule_mut().unwrap()) {
                        action_score -= 100.0;
                    }
                    if !ef_state.stage.as_scale_for_fns_mut().unwrap().prepare_next() {
                        ef_state.trans_stage(self);
                    }
                } else if self.config.es.sche_rule() {
                    self.try_put_fn();
                    ef_state.trans_stage(self);
                } else if self.config.es.sche_faas_flow() {
                    let mut spec = self.spec_scheduler.borrow_mut();
                    spec.as_mut().unwrap().schedule_some(self);
                    ef_state.trans_stage(self);
                } else if self.config.es.sche_fnsche() {
                    // sche is done in scale stage
                    ef_state.trans_stage(self);
                } else {
                    panic!("no schedule method");
                }

                //当前stage score
            } else if ef_state.stage.is_sim_compute() {
                log::info!("sim compute");
                ef_state.computed = true;
                self.sim_run();
                frame_score = self.score();
                self.metric_record.borrow_mut().add_frame(self);

                ef_state.trans_stage(self);
            }
            // else if aief_state.stage.is_scale_down() {
            //     if action_done {
            //         // next action effect stage is prepared
            //         break;
            //     }
            //     action_done = true;

            //     self.step_scale_down(
            //         (raw_action * 11.0) as u32,
            //         aief_state.stage.as_scale_down_mut().unwrap()
            //     );
            // }

            // if aief_state.is_action_effect_stage() {
            //     if !aief_state.unwrap_aes_prepare_next() {
            //         // stage 已经完成了, 转到下一个stage
            //         aief_state.trans_stage(self);
            //     } else {
            //         // action effect stage not changed
            //         break;
            //     }
            // }
        }

        // let mut state_buf = StateBuffer::new();
        // if aief_state.stage.is_scale_for_fns() {
        //     self.make_state_scale_for_fns(
        //         &mut state_buf,
        //         aief_state.stage.as_scale_for_fns_mut().unwrap()
        //     );
        //     self.make_common_state(&mut state_buf, SCALE_FOR_FNS_IDX);
        // } else if aief_state.stage.is_schedule() {
        //     self.make_state_schedule(&mut state_buf, aief_state.stage.as_schedule_mut().unwrap());
        //     self.make_common_state(&mut state_buf, SCHEDULE_IDX);
        // } else if aief_state.stage.is_scale_down() {
        //     self.make_state_scale_down(
        //         &mut state_buf,
        //         aief_state.stage.as_scale_down_mut().unwrap()
        //     );
        //     self.make_common_state(&mut state_buf, SCALE_DOWN_IDX);
        // }

        // fnid    container_busy    container_count    fn running tasks
        //
        let state = if ef_state.stage.is_scale_for_fns() {
            let scale_stage = ef_state.stage.as_scale_for_fns().unwrap();

            let fnid = scale_stage.current_fnid.unwrap();

            let mut fn_container_busy = 0.0;
            self.fn_containers_for_each(fnid, |c| {
                fn_container_busy += c.busyness();
            });

            let fn_container_count = self.fn_container_cnt(fnid);

            let mut fn_running_tasks = 0;
            let mut fn_avg_cpu = 0.0;
            self.fn_containers_for_each(fnid, |c| {
                fn_running_tasks += c.req_fn_state.len();
                fn_avg_cpu += c.cpu_use_rate();
            });
            if fn_container_count > 0 {
                fn_avg_cpu /= fn_container_count as f32;
            }

            let state = vec![
                fnid as f32,
                fn_container_busy,
                fn_container_count as f32,
                fn_running_tasks as f32,
                scale_stage.fn_need_schedule
                    .get(&fnid)
                    .map(|v| { v.ready_2_schedule_fn_count() })
                    .unwrap_or(0) as f32,
                fn_avg_cpu
            ];
            log::info!("state: {:?}", state);
            state
        } else {
            vec![]
        };

        log::info!(
            "ai logic step:{} stage:{} frame:{} request time:{} score:{}",
            ef_state.step_cnt,
            &ef_state.stage.type_str(),
            self.current_frame(),
            self.req_done_time_avg(),
            frame_score + action_score
        );
        ef_state.step_cnt += 1;

        // state should has prompt info for next action
        (frame_score + action_score, serde_json::to_string(&state).unwrap())
    }
}
