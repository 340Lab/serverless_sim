use crate::{
    sim_env::SimEnv,
    sim_scale_executor::{ScaleExecutor, ScaleOption},
    sim_scaler::{ScaleArg, Scaler},
};

enum Target {
    CpuUseRate(f32),
}

pub struct HpaScaler {
    target: Target,
    // target_tolerance: determines how close the target/current
    //   resource ratio must be to 1.0 to skip scaling
    target_tolerance: f32,
}

impl HpaScaler {
    pub fn new() -> Self {
        HpaScaler {
            target: Target::CpuUseRate(0.5),
            target_tolerance: 0.1,
        }
    }

    /// 对于一个fn, 可以有多个容器(一个节点一个)，增加容器数，可以减少平均cpu利用率
    fn cpu_scale_for_exist_containers(&mut self, sim_env: &SimEnv, cpu_target_use_rate: f32) {
        let fn_2_nodes = sim_env.fn_2_nodes.borrow();
        let fnid_containercnt_avgcpuuse = fn_2_nodes
            .iter()
            .map(|(fnid, nodes)| {
                let container_cnt = nodes.len();
                let avg_cpu_use_rate = nodes
                    .iter()
                    .map(|n: &usize| {
                        let node = sim_env.node(*n);
                        let fn_container = node.fn_containers.get(fnid).unwrap();
                        fn_container.cpu_use_rate()
                    })
                    .sum::<f32>()
                    / container_cnt as f32;
                (*fnid, container_cnt, avg_cpu_use_rate)
            })
            .collect::<Vec<_>>();
        // later scale up/down will operate on this data, so we release the borrow
        drop(fn_2_nodes);

        for (fnid, container_cnt, avg_cpu_use_rate) in fnid_containercnt_avgcpuuse {
            // let container_cnt = nodes.len();
            // let avg_cpu_use_rate = nodes
            //     .iter()
            //     .map(|n: &usize| {
            //         let node = sim_env.node(*n);
            //         let fn_container = node.fn_containers.get(fnid).unwrap();
            //         fn_container.cpu_use_rate()
            //     })
            //     .sum::<f32>()
            //     / container_cnt as f32;
            let desired_container_cnt = (avg_cpu_use_rate / cpu_target_use_rate).ceil() as usize;

            // current divide target
            let ratio = avg_cpu_use_rate / cpu_target_use_rate;
            if (1.0 > ratio && ratio >= 1.0 - self.target_tolerance)
                || (1.0 < ratio && ratio < 1.0 + self.target_tolerance)
                || ratio == 1.0
            {
                // # ratio is sufficiently close to 1.0
                continue;
            }

            if desired_container_cnt < container_cnt {
                // # scale down
                let scale = container_cnt - desired_container_cnt;
                sim_env.scale_executor.borrow_mut().scale_down(
                    sim_env,
                    ScaleOption::new().for_spec_fn(fnid).with_scale_cnt(scale),
                );
            } else {
                // # scale up
                let scale = desired_container_cnt - container_cnt;
                sim_env
                    .scale_executor
                    .borrow_mut()
                    .scale_up(sim_env, fnid, scale);
            }
        }
    }
}

impl Scaler for HpaScaler {
    fn scale(&mut self, sim_env: &SimEnv, arg: ScaleArg) {
        match arg {
            ScaleArg::HPAScaler => {}
            _ => panic!("not match"),
        }

        match self.target {
            Target::CpuUseRate(cpu_use_rate) => {
                self.cpu_scale_for_exist_containers(sim_env, cpu_use_rate);
            }
        }
    }
}
