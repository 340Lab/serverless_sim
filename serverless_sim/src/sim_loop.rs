use thread_priority::{set_current_thread_priority, ThreadPriority};

use crate::{
    actions::ESActionWrapper,
    mechanism::SimEnvObserve,
    mechanism_thread::{MechScheduleOnce, MechScheduleOnceRes},
    node::EnvNodeExt,
    rl_target::RL_TARGET,
    sim_env::SimEnv,
    with_env_sub::WithEnvHelp,
};

use std::{
    sync::mpsc::{self, Receiver},
    thread::sleep,
    time::Duration,
};

impl SimEnv {
    fn one_frame(
        &mut self,
        hook_frame_begin: &mut Option<Box<dyn FnMut(&SimEnv) + 'static>>,
        hook_req_gen: &mut Option<Box<dyn FnMut(&SimEnv) + 'static>>,
    ) -> bool {
        // 进行帧开始时处理
        self.on_frame_begin();
        if let Some(f) = hook_frame_begin.as_mut() {
            f(self);
        }
        // 生成新的请求，并添加到环境对象的请求映射中
        self.req_sim_gen_requests();
        if let Some(f) = hook_req_gen.as_mut() {
            f(self);
        }

        // 新请求生成之后将系统中请求和节点更新到最新状态
        self.help.mech_metric_mut().on_new_req_generated(self);

        // // 获得 扩容、缩容、调度 的指令
        // let (ups, downs, sches) = self.new_mech.step(self, raw_action.clone());

        self.sim_run();

        self.on_frame_end();

        if self.current_frame() > self.help().config().total_frame {
            self.help.metric_record_mut().as_ref().unwrap().flush(self);
            RL_TARGET.as_ref().map(|v| v.set_stop());
            // self.reset();
            false
        } else {
            true
        }
    }
    /// raw_action[0] container count
    pub fn step_es(
        &mut self,
        raw_action: ESActionWrapper,
        mut hook_frame_begin: Option<Box<dyn FnMut(&SimEnv) + 'static>>,
        mut hook_req_gen: Option<Box<dyn FnMut(&SimEnv) + 'static>>,
        mut hook_algo_begin: Option<Box<dyn FnMut(&SimEnv) + 'static>>,
        mut hook_algo_end: Option<Box<dyn FnMut(&SimEnv) + 'static>>,
    ) -> (f32, String) {
        // 尝试设置当前线程的优先级
        if let Err(e) = set_current_thread_priority(ThreadPriority::Min) {
            eprintln!("设置线程优先级失败: {:?}", e);
        }
        self.avoid_gc();
        let mut master_mech_resp_rx: Option<Receiver<MechScheduleOnceRes>> = None;
        let mut frame_when_master_mech_begin = 0;
        'outer: loop {
            if let Some(rx) = &master_mech_resp_rx {
                let mut end_recv_algo_loop = false;
                while !end_recv_algo_loop {
                    let res = if self.help.config().no_mech_latency {
                        // wait until algo done;
                        let res = rx.recv().unwrap();
                        if res.is_end() {
                            end_recv_algo_loop = true;
                        }
                        res
                    } else {
                        // don't wait algo, run algo async
                        let Ok(res) = rx.try_recv() else {
                            break;
                        };
                        res
                    };
                    match res {
                        MechScheduleOnceRes::Cmds {
                            sche_cmds,
                            scale_up_cmds,
                            scale_down_cmds,
                        } => {
                            // 2. handle_master's commands
                            {
                                // FIXME: Should transfer the cmds for a while.
                                // FIXME: should remove conflict cmds
                                // TODO: ScheCmd has memlimit
                                for sche in sche_cmds.iter() {
                                    self.schedule_reqfn_on_node(
                                        &mut self.request_mut(sche.reqid),
                                        sche.fnid,
                                        sche.nid,
                                    );
                                }
                                for down in scale_down_cmds.iter() {
                                    //更新cache
                                    self.node_mut(down.nid)
                                        .try_unload_container(down.fnid, self, true);
                                }
                                for up in scale_up_cmds.iter() {
                                    self.node_mut(up.nid).try_load_container(up.fnid, self);
                                }
                            }
                        }
                        MechScheduleOnceRes::ScheCmd(sche) => {
                            self.schedule_reqfn_on_node(
                                &mut self.request_mut(sche.reqid),
                                sche.fnid,
                                sche.nid,
                            );
                        }
                        MechScheduleOnceRes::ScaleDownCmd(down) => {
                            //更新cache
                            self.node_mut(down.nid)
                                .try_unload_container(down.fnid, self, true);
                        }
                        MechScheduleOnceRes::ScaleUpCmd(up) => {
                            self.node_mut(up.nid).try_load_container(up.fnid, self);
                        }
                        MechScheduleOnceRes::End { mech_run_ms } => {
                            // 1. need to handle the gap between
                            //    master_mech time and simulation time
                            //    just simulate some if mech is longer
                            {
                                // one frame reflect to 1ms
                                let master_mech_frame = mech_run_ms as usize;
                                let frame_ran = self.current_frame() - frame_when_master_mech_begin;
                                let gap = if master_mech_frame > frame_ran {
                                    master_mech_frame - frame_ran
                                } else {
                                    0
                                };
                                for _ in 0..gap {
                                    if !self.one_frame(&mut hook_frame_begin, &mut hook_req_gen) {
                                        break 'outer;
                                    }
                                }
                                log::info!(
                                    "master mech ran in {} ms, catch up {} gap frames, cur frame: {}",
                                    mech_run_ms,
                                    gap,
                                    self.current_frame()
                                );
                                self.help
                                    .algo_exc_time_mut()
                                    .insert(self.current_frame(), mech_run_ms as usize);
                            }

                            self.master_mech_not_running = true;
                            frame_when_master_mech_begin = self.current_frame();
                            hook_algo_end.as_mut().map(|f| f(self));
                        }
                    }
                }
            }
            if self.master_mech_not_running {
                self.master_mech_not_running = false;
                // just copy the algorithm needed metrics and continue run
                let (tx, rx) = mpsc::channel();
                master_mech_resp_rx = Some(rx);
                self.mech_caller
                    .send(MechScheduleOnce {
                        sim_env: SimEnvObserve::new(self.core.clone(), self.help.clone()),
                        responser: tx,
                        action: raw_action.clone(),
                    })
                    .unwrap();
                hook_algo_begin.as_mut().map(|f| f(self));
            }
            if !self.one_frame(&mut hook_frame_begin, &mut hook_req_gen) {
                break;
            }

            // 每帧跑完休息50ms
            // sleep(Duration::from_millis(20));
        }

        // state should has prompt info for next action
        (0.0, "no action".to_string())
    }
}
