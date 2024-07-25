
use std::cell::{ RefCell };
use std::collections::{ HashMap, VecDeque };

use crate::fn_dag::EnvFnExt;
use crate::mechanism::SimEnvObserve;
use crate::node::EnvNodeExt;
use crate::with_env_sub::{ WithEnvCore };
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
        
        // 当前容器数量
        let cur_container_cnt = env.fn_container_cnt(fnid);

        // 取cur_container_cnt的根号
        let sqrt_container_cnt = (cur_container_cnt as f64).sqrt().ceil() as usize;

        if cur_container_cnt + sqrt_container_cnt == 0 {
            1
        }
        else {
            cur_container_cnt + sqrt_container_cnt
        }

    }
}
