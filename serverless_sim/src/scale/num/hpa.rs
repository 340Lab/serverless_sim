use std::collections::{HashMap, HashSet};

use crate::scale::down_exec::ScaleDownExec;
use crate::{actions::ESActionWrapper, algos::ContainerMetric, fn_dag::FnId, sim_env::SimEnv};

use super::{
    down_filter::{CarefulScaleDownFilter, ScaleFilter},
    ScaleNum,
};

enum Target {
    CpuUseRate(f32),
}

pub struct HpaScaleNum {
    target: Target,
    // target_tolerance: determines how close the target/current
    //   resource ratio must be to 1.0 to skip scaling
    target_tolerance: f32,
    pub scale_down_policy: Box<dyn ScaleFilter + Send>,
    fn_sche_container_count: HashMap<FnId, usize>,
}

impl HpaScaleNum {
    pub fn new() -> Self {
        Self {
            target: Target::CpuUseRate(0.5),
            target_tolerance: 0.1,
            scale_down_policy: Box::new(CarefulScaleDownFilter::new()),
            fn_sche_container_count: HashMap::new(),
        }
    }
}

impl ScaleNum for HpaScaleNum {
    fn scale_for_fn(&mut self, env: &SimEnv, fnid: FnId, action: &ESActionWrapper) -> usize {
        let mech_metric = env.help.mech_metric();
        let desired_container_cnt = match self.target {
            Target::CpuUseRate(cpu_target_use_rate) => {
                let container_cnt = env.fn_container_cnt(fnid);

                let mut desired_container_cnt = 
                    if container_cnt != 0 {
                        let mut avg_mem_use_rate = 0.0;
                        env.fn_containers_for_each(fnid, |c| {
                            // avg_cpu_use_rate +=
                            // env.node(c.node_id).last_frame_cpu / env.node(c.node_id).rsc_limit.cpu;
                            avg_mem_use_rate +=
                                env.node(c.node_id).last_frame_mem / env.node(c.node_id).rsc_limit.mem;
                        });
                        // avg_mem_use_rate /= container_cnt as f32;

                        {
                            // current divide target
                            let ratio = avg_mem_use_rate / cpu_target_use_rate;
                            if (1.0 > ratio && ratio >= 1.0 - self.target_tolerance)
                                || (1.0 < ratio && ratio < 1.0 + self.target_tolerance)
                                || ratio == 1.0
                            {
                                // # ratio is sufficiently close to 1.0

                            // log::info!("hpa skip {fnid} at frame {}", env.current_frame());
                            return container_cnt;
                            }
                        }
                    // log::info!("avg mem use rate {}", avg_mem_use_rate);
                    (avg_mem_use_rate / cpu_target_use_rate).ceil() as usize
                } 
                else {
                    0
                };

                if mech_metric.fn_unsche_req_cnt(fnid) > 0 && desired_container_cnt == 0 {
                    desired_container_cnt = 1;
                }

                desired_container_cnt
            }
        };
        desired_container_cnt
    }
}
