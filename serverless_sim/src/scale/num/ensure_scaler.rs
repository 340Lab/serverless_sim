
use std::cell::{ RefCell };
use std::collections::{ HashMap, HashSet, VecDeque };

use crate::fn_dag::EnvFnExt;
use crate::mechanism::SimEnvObserve;
use crate::node::EnvNodeExt;
use crate::sim_run::schedule_helper;
use crate::with_env_sub::{ WithEnvCore, WithEnvHelp };
use crate::{ actions::ESActionWrapper, fn_dag::FnId, CONTAINER_BASIC_MEM };

use super::{ down_filter::{ CarefulScaleDownFilter, ScaleFilter }, ScaleNum };

pub struct EnsureScaleNum {}

impl EnsureScaleNum {
    pub fn new() -> Self {
        Self {
            
        }
    }
}

impl ScaleNum for EnsureScaleNum {
    fn scale_for_fn(&mut self, env: &SimEnvObserve, fnid: FnId, _action: &ESActionWrapper) -> usize {

        let mut need_to_schedule = false;
        // 找到这一帧需要调度的函数
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            let schedule_able_fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            for sche_fnid in schedule_able_fns.iter() {
                if sche_fnid == &fnid {
                    need_to_schedule = true;
                }
            }
        }

        let current_frame = env.core().current_frame();

        // 当前容器数量
        let cur_container_cnt = env.fn_container_cnt(fnid);

        // 取cur_container_cnt的根号
        let sqrt_container_cnt = (cur_container_cnt as f64).sqrt().ceil() as usize;

        if need_to_schedule || cur_container_cnt == 0 {
    
            if cur_container_cnt + sqrt_container_cnt == 0{
                1
            }
            else {
                cur_container_cnt + sqrt_container_cnt
            } 
        }else {
            cur_container_cnt
        }

    }
}
