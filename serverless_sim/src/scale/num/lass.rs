use std::collections::HashMap;
use super::{
    down_filter::{CarefulScaleDownFilter, ScaleFilter},
    ScaleNum,
};
use crate::{
    actions::ESActionWrapper,fn_dag::FnId, 
    sim_env::SimEnv,
};

pub struct LassScaleNum {
    pub latency_required: f32,
    pub scale_down_policy: Box<dyn ScaleFilter + Send>,
    fn_sche_container_count: HashMap<FnId, usize>,
}

impl LassScaleNum {
    pub fn new() -> Self {
        Self {
            latency_required: 7.0,
            scale_down_policy: Box::new(CarefulScaleDownFilter::new()),
            fn_sche_container_count: HashMap::new(),
        }
    }
}

// unsafe impl Send for LassEFScaler {}

impl ScaleNum for LassScaleNum {
    fn scale_for_fn(&mut self, env: &SimEnv, fnid: FnId, action: &ESActionWrapper) -> usize {
        // 请求时间=请求数/(当前容器数(cc)*每个容器请求处理速率(r/t))
        let desired_container_cnt =
            // if metric.ready_2_schedule_fn_count() + metric.scheduled_fn_count == 0 {
            //     0
            // } else
            {
                
                let recent_speed = {
                    let mut recent_speed_sum = 0.0;
                    let mut recent_speed_cnt = 0;

                    if let Some(nodes) = env.core.fn_2_nodes().get(&fnid) {
                        nodes.iter().for_each(|&nodeid| {
                            let node = env.node(nodeid);
                            let container = node.container(fnid).unwrap();

                            recent_speed_sum += container.recent_handle_speed();
                            recent_speed_cnt += 1;
                        });

                        if recent_speed_cnt == 0 {
                            0.0
                        } else {
                            recent_speed_sum / (recent_speed_cnt as f32)
                        }
                    } else {
                        0.0
                    }
                };
                if recent_speed < 0.00001 {
                    1
                } else {
                    (env.help.mech_metric().fn_recent_req_cnt(fnid)
                        / (self.latency_required * recent_speed).ceil())
                        as usize
                }
            };


        
        // !!! move to careful_down_filter
        // desired_container_cnt =
        //     self.scale_down_policy
        //         .filter_desired(fnid, desired_container_cnt, container_cnt);

        // !!! move to basic filter                
        // if env
        //     .mechanisms
        //     .spec_scheduler()
        //     .as_ref()
        //     .unwrap()
        //     .this_turn_will_schedule(fnid)
        //     && desired_container_cnt == 0
        // {
        //     desired_container_cnt = 1;
        // }

        // !!! move out of here
        // if desired_container_cnt < container_cnt {
        //     // # scale down
        //     let scale = container_cnt - desired_container_cnt;

        //     env.mechanisms.scale_executor_mut().exec_scale_down(
        //         env,
        //         ScaleOption::new().for_spec_fn(fnid).with_scale_cnt(scale),
        //     );
        // }
        // else {
        //     // # scale up
        //     let scale = desired_container_cnt - container_cnt;
        //     env.scale_executor.borrow_mut().scale_up(env, fnid, scale);
        // }

        desired_container_cnt
    }
}
