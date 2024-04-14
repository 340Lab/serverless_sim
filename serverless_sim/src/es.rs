use crate::{
    actions::{ESActionWrapper, RawAction},
    algos::ContainerMetric,
    config::Config,
    fn_dag::FnId,
    node::NodeId,
    request::ReqId,
    scale::num::{ai::AIScaleNum, hpa::HpaScaleNum, lass::LassScaleNum, no::NoScaleNum, ScaleNum},
    sim_env::SimEnv,
    sim_run::Scheduler,
};
use enum_as_inner::EnumAsInner;
use std::{
    cell::RefMut,
    collections::{BTreeMap, VecDeque},
};

// 确定当前阶段是否准备好进行下一个 action 的处理
pub trait ActionEffectStage {
    fn prepare_next(&mut self) -> bool;
}

#[derive(Debug)]
pub struct StageScaleForFns {
    // 当前正在处理的函数在 fn_metrics 中的索引
    current_index: Option<usize>,
    // pub fn_need_schedule: HashMap<FnId, ContainerMetric>,
    pub fn_metrics: Vec<(FnId, ContainerMetric)>,
    // pub fns: HashMap<FnId, ContainerMetric>,
    // pub current_fn_to_scale: Option<(FnId, ContainerMetric)>,
    // // action target_size
    // pub scaled: Vec<(FnId, usize, RawAction)>,
}
impl StageScaleForFns {
    fn current_fn<'a>(&'a self) -> Option<&'a (FnId, ContainerMetric)> {
        if let Some(current_index) = self.current_index.as_ref() {
            return self.fn_metrics.get(*current_index);
        }
        None
    }
}
impl ActionEffectStage for StageScaleForFns {
    fn prepare_next(&mut self) -> bool {
        if let Some(current_index) = self.current_index.as_mut() {
            *current_index += 1;
        } else {
            self.current_index = Some(0);
        }
        
        // 所有函数已处理完毕
        if self.current_index.unwrap() >= self.fn_metrics.len() {
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

// 负责请求调度的阶段
#[derive(Debug)]
pub struct StageSchedule {
    // 与该请求关联的待调度函数ID, 每个请求可能有多个待调度的函数
    ready_2_schedule: BTreeMap<ReqId, VecDeque<FnId>>,
    // 下一个将要调度的请求及其关联的函数
    pub next_2_schedule: (ReqId, FnId),
    // 已调度的请求信息, 记录了请求已调度到哪个函数以及可能的执行节点（如果已知）和相关动作
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
            let next_fn: FnId = self
                .ready_2_schedule
                .get_mut(&next)
                .unwrap()
                .pop_front()
                .unwrap();
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

// 闲置容器缩放阶段
#[derive(Debug)]
pub struct StageScaleDown {
    // 某个节点上的某个函数对应的容器处于闲置状态 
    pub idle_containers: Vec<(NodeId, FnId)>,
    // 当前正在处理的闲置容器在 idle_containers 中的索引
    pub cur_idle_container_idx: isize,
    // 记录了已缩放的容器信息
    pub records: Vec<(NodeId, FnId, RawAction)>,
}

impl StageScaleDown {
    fn new(env: &SimEnv) -> Self {
        let mut idle_containers = Vec::new();
        let nodes = env.core.nodes();
        for node in nodes.iter() {
            for (&fnid, container) in node.fn_containers.borrow().iter() {
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
    // 按照当前索引获取闲置容器信息
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

// Each Frame Stage
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

// 存储仿真环境的当前状态
pub struct ESState {
    // 已执行步数
    pub step_cnt: usize,
    // 当前帧所处阶段
    pub stage: EFStage,
    // 当前帧计算状态
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
    // 根据当前的阶段，调用相应的 prepare_next()
    fn unwrap_aes_prepare_next(&mut self) -> bool {
        match self.stage {
            EFStage::FrameBegin => {
                panic!("FrameBegin stage should not call unwrap_aes_prepare_next")
            }
            EFStage::ScaleForFns(ref mut stage) => stage.prepare_next(),
            EFStage::Schedule(ref mut stage) => stage.prepare_next(),
            EFStage::SimCompute => {
                panic!("SimCompute stage should not call unwrap_aes_prepare_next")
            }
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
    // 进入下一阶段
    pub fn trans_stage(&mut self, env: &SimEnv) -> bool {
        loop {
            if self.stage.is_frame_begin() {
                // collect scale infos
                let mut fn_metrics_map = env.algo_collect_ready_2_schedule_metric();
                let fn_all_sched_metrics = env.algo_get_fn_all_scheduled_metric(&fn_metrics_map);
                let mut fn_metrics = Vec::new();
                while fn_metrics_map.len() > 0 {
                    let fnid = *fn_metrics_map.iter().next().unwrap().0;
                    fn_metrics.push((fnid, fn_metrics_map.remove(&fnid).unwrap()));
                }
                fn_metrics.extend(fn_all_sched_metrics);

                assert_eq!(fn_metrics.len(), env.core.fns().len());
                self.stage = EFStage::ScaleForFns(StageScaleForFns {
                    current_index: None,
                    fn_metrics,
                    // scaled: Vec::new(),
                    // current_fn_to_scale: None,
                });
                if self.stage.as_scale_for_fns_mut().unwrap().prepare_next() {
                    // pre load info of scheduler because scaler need to know the info of scheduler
                    env.mechanisms
                        .spec_scheduler_mut()
                        .as_mut()
                        .unwrap()
                        .prepare_this_turn_will_schedule(env);
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

impl SimEnv {
    // return false if schedule failed
    // 根据用户提供的raw_action执行一次调度操作
    fn step_schedule(&self, raw_action: u32, stage: &mut StageSchedule) -> bool {
        let mut ret = true;
        let (reqid, fnid) = stage.next_2_schedule;
        if raw_action > ((self.node_cnt() - 1) as u32) {
            stage.scheduled.push((reqid, fnid, None, raw_action));
        } else {
            let nodeid = raw_action as usize;
            // 节点上存在该函数的容器
            if self.node(nodeid).container(fnid).is_some() {
                assert!(self.request_mut(reqid).req_id == reqid, "reqid not match");
                self.schedule_reqfn_on_node(&mut self.request_mut(reqid), fnid, nodeid);
            } else {
                ret = false;
            }
            stage
                .scheduled
                .push((reqid, fnid, Some(nodeid), raw_action));
        }
        ret
    }

    /// raw_action[0] container count
    pub fn step_es(&mut self, raw_action: ESActionWrapper) -> (f32, String) {
        self.avoid_gc();

        let mut ef_state: RefMut<'_, ESState> = self.ef_state.borrow_mut();

        let mut frame_score = self.score();
        let mut action_score = 0.0;
        let mut action_done = false;
        // 只有确定了下一个action，才会有可以返回的state

        loop {
            if ef_state.stage.is_frame_begin() {
                // 当前帧结束
                if (self.current_frame() == 0 && ef_state.computed) || self.current_frame() > 0 {
                    // log::info!("score: {} frame:{}", score, self.current_frame());
                    self.on_frame_end();
                    log::info!("frame {} end", self.current_frame());
                    // 模拟超过1000帧时退出循环
                    if self.current_frame() > 1000 {
                        break;
                    }
                }
                log::info!("frame begin");
                // 开启新帧
                self.on_frame_begin();

                // 没有正在调度的请求了，分配一个正在调度的请求
                // 生成新的请求，更新 ef_state 以进入下一阶段
                self.req_sim_gen_requests();
                ef_state.trans_stage(self);
            } else if ef_state.stage.is_scale_for_fns() {
                if action_done {
                    // next action effect stage is prepared
                    break;
                }
                let mut scale_num_opt = self.mechanisms.spec_scale_num_mut();
                if let Some(scale_num) = scale_num_opt.as_mut() {
                    let has_next = {
                        let stage = ef_state.stage.as_scale_for_fns_mut().unwrap();
                        // let fnid = stage.current_fnid.unwrap();
                        let &(fnid, ref metric) = stage.current_fn().unwrap();
                        let (action_score_, action_done_) =
                            scale_num.scale_for_fn(self, fnid, metric, &raw_action);
                        action_score += action_score_;
                        action_done = action_done_;
                        stage.prepare_next()
                    };
                    if !has_next {
                        ef_state.trans_stage(self);
                    }
                } else {
                    log::debug!("skip scale for this env");
                }
            } else if ef_state.stage.is_schedule() {
                log::info!("schedule");
                if let Some(spec_sche) = self.mechanisms.spec_scheduler_mut().as_mut() {
                    // let mut spec = self.spec_scheduler.borrow_mut();
                    spec_sche.schedule_some(self);
                    ef_state.trans_stage(self);
                } else {
                    panic!("no schedule method");
                }
                //当前stage score
            } 
            // 进行模拟计算（如运行容器任务），更新frame_score为当前帧得分，记录模拟指标，标记计算完成，并转至下一阶段
            else if ef_state.stage.is_sim_compute() {
                log::info!("sim compute");
                ef_state.computed = true;
                self.sim_run();
                frame_score = self.score();
                self.help.metric_record_mut().add_frame(self);

                ef_state.trans_stage(self);
            }
        }

        // fnid    container_busy    container_count    fn running tasks
        // 构建状态信息
        let state = if ef_state.stage.is_scale_for_fns() {
            let scale_stage = ef_state.stage.as_scale_for_fns().unwrap();

            let fnid = scale_stage.current_fn().unwrap().0;

            let mut fn_container_busy = 0.0;
            self.fn_containers_for_each(fnid, |c| {
                fn_container_busy += c.busyness();
            });

            let fn_container_count = self.fn_container_cnt(fnid);

            let mut fn_running_tasks = 0;
            let mut fn_avg_cpu = 0.0;
            let mut fn_avg_mem_rate = 0.0;
            self.fn_containers_for_each(fnid, |c| {
                fn_running_tasks += c.req_fn_state.len();
                fn_avg_cpu +=
                    self.node(c.node_id).last_frame_cpu / self.node(c.node_id).rsc_limit.cpu;
                fn_avg_mem_rate += self.node(c.node_id).mem() / self.node(c.node_id).rsc_limit.mem;
            });
            if fn_container_count > 0 {
                fn_avg_cpu /= fn_container_count as f32;
                fn_avg_mem_rate /= fn_container_count as f32;
            }

            let state = vec![
                fnid as f32,
                fn_container_busy,
                fn_container_count as f32,
                fn_running_tasks as f32,
                scale_stage
                    .fn_metrics
                    .iter()
                    .filter(|&&(fnid_, _)| fnid_ == fnid)
                    .map(|(_, v)| v.ready_2_schedule_fn_count())
                    .sum::<usize>() as f32,
                fn_avg_cpu,
                fn_avg_mem_rate,
                // *self.hpa_action.borrow() as f32,
                self.req_done_time_avg(),
                self.cost_each_req(),
                self.cost_perform(),
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
        (
            frame_score + action_score,
            serde_json::to_string(&state).unwrap(),
        )
    }
}
