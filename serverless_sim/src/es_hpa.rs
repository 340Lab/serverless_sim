use std::collections::HashMap;

use crate::{
    actions::ESActionWrapper,
    algos::ContainerMetric,
    es::ESScaler,
    fn_dag::FnId,
    scale_down_policy::{CarefulScaleDown, ScaleDownPolicy},
    scale_executor::{ScaleExecutor, ScaleOption},
    sim_env::SimEnv,
};

enum Target {
    CpuUseRate(f32),
}

pub struct HpaESScaler {
    target: Target,
    // target_tolerance: determines how close the target/current
    //   resource ratio must be to 1.0 to skip scaling
    target_tolerance: f32,
    pub scale_down_policy: Box<dyn ScaleDownPolicy + Send>,
    fn_sche_container_count: HashMap<FnId, usize>,
}

impl HpaESScaler {
    pub fn new() -> Self {
        Self {
            target: Target::CpuUseRate(0.5),
            target_tolerance: 0.1,
            scale_down_policy: Box::new(CarefulScaleDown::new()),
            fn_sche_container_count: HashMap::new(),
        }
    }
    pub fn action(&mut self, env: &SimEnv, fnid: FnId, metric: &ContainerMetric) -> usize {
        let cpu_target_use_rate = match self.target {
            Target::CpuUseRate(cpu_target_use_rate) => cpu_target_use_rate,
        };
        let container_cnt = metric.container_count;
        let mut avg_cpu_use_rate = 0.0;
        env.fn_containers_for_each(fnid, |c| {
            // avg_cpu_use_rate +=
            // env.node(c.node_id).last_frame_cpu / env.node(c.node_id).rsc_limit.cpu;
            avg_cpu_use_rate += env.node(c.node_id).mem() / env.node(c.node_id).rsc_limit.mem;
        });
        if container_cnt != 0 {
            avg_cpu_use_rate /= container_cnt as f32;
        }

        // let container_cnt = nodes.len();
        // let avg_cpu_use_rate = nodes
        //     .iter()
        //     .map(|n: &usize| {
        //         let node = env.node(*n);
        //         let fn_container = node.fn_containers.get(fnid).unwrap();
        //         fn_container.cpu_use_rate()
        //     })
        //     .sum::<f32>()
        //     / container_cnt as f32;
        let mut desired_container_cnt = (avg_cpu_use_rate / cpu_target_use_rate).ceil() as usize;

        if metric.ready_2_schedule_fn_count() > 0 && desired_container_cnt == 0 {
            desired_container_cnt = 1;
        } else {
            // current divide target
            let ratio = avg_cpu_use_rate / cpu_target_use_rate;
            if (1.0 > ratio && ratio >= 1.0 - self.target_tolerance)
                || (1.0 < ratio && ratio < 1.0 + self.target_tolerance)
                || ratio == 1.0
            {
                // # ratio is sufficiently close to 1.0

                // log::info!("hpa skip {fnid} at frame {}", env.current_frame());
                return 11;
            }
        }

        desired_container_cnt =
            self.scale_down_policy
                .filter_desired(fnid, desired_container_cnt, container_cnt);

        desired_container_cnt
    }
}

impl ESScaler for HpaESScaler {
    fn fn_available_count(&self, fnid: FnId, env: &SimEnv) -> usize {
        self.fn_sche_container_count
            .get(&fnid)
            .map(|c| *c)
            .unwrap_or(0)
    }
    fn scale_for_fn(
        &mut self,
        env: &SimEnv,
        fnid: FnId,
        metric: &ContainerMetric,
        action: &ESActionWrapper,
    ) -> (f32, bool) {
        match self.target {
            Target::CpuUseRate(cpu_target_use_rate) => {
                let container_cnt = metric.container_count;
                let mut avg_cpu_use_rate = 0.0;
                env.fn_containers_for_each(fnid, |c| {
                    // avg_cpu_use_rate +=
                    // env.node(c.node_id).last_frame_cpu / env.node(c.node_id).rsc_limit.cpu;
                    avg_cpu_use_rate +=
                        env.node(c.node_id).mem() / env.node(c.node_id).rsc_limit.mem;
                });
                if container_cnt != 0 {
                    avg_cpu_use_rate /= container_cnt as f32;
                }

                // let container_cnt = nodes.len();
                // let avg_cpu_use_rate = nodes
                //     .iter()
                //     .map(|n: &usize| {
                //         let node = env.node(*n);
                //         let fn_container = node.fn_containers.get(fnid).unwrap();
                //         fn_container.cpu_use_rate()
                //     })
                //     .sum::<f32>()
                //     / container_cnt as f32;
                let mut desired_container_cnt =
                    (avg_cpu_use_rate / cpu_target_use_rate).ceil() as usize;

                if metric.ready_2_schedule_fn_count() > 0 && desired_container_cnt == 0 {
                    desired_container_cnt = 1;
                } else {
                    // current divide target
                    let ratio = avg_cpu_use_rate / cpu_target_use_rate;
                    if (1.0 > ratio && ratio >= 1.0 - self.target_tolerance)
                        || (1.0 < ratio && ratio < 1.0 + self.target_tolerance)
                        || ratio == 1.0
                    {
                        // # ratio is sufficiently close to 1.0

                        // log::info!("hpa skip {fnid} at frame {}", env.current_frame());
                        return (0.0, false);
                    }
                }

                desired_container_cnt = self.scale_down_policy.filter_desired(
                    fnid,
                    desired_container_cnt,
                    container_cnt,
                );

                // log::info!("hpa try scale from {} to {}", container_cnt, desired_container_cnt);

                // take the initiative in scale down to save cost
                if desired_container_cnt < container_cnt {
                    // # scale down
                    let scale = container_cnt - desired_container_cnt;
                    env.scale_executor.borrow_mut().scale_down(
                        env,
                        ScaleOption::new().for_spec_fn(fnid).with_scale_cnt(scale),
                    );
                }
                // else if desired_container_cnt > container_cnt {
                //     // # scale up
                //     let scale = desired_container_cnt - container_cnt;
                //     env.scale_executor.borrow_mut().scale_up(env, fnid, scale);
                // }
                self.fn_sche_container_count
                    .insert(fnid, desired_container_cnt);
            }
        }
        (0.0, false)
    }
}
