use crate::{
    es::ESScaler,
    sim_env::SimEnv,
    fn_dag::FnId,
    algos::ContainerMetric,
    scale_executor::{ ScaleExecutor, ScaleOption },
    scale_down_policy::{ ScaleDownPolicy, CarefulScaleDown },
};

pub struct LassESScaler {
    pub latency_required: f32,
    pub scale_down_policy: Box<dyn ScaleDownPolicy + Send>,
}

impl LassESScaler {
    pub fn new() -> Self {
        Self {
            latency_required: 7.0,
            scale_down_policy: Box::new(CarefulScaleDown::new()),
        }
    }
}

// unsafe impl Send for LassEFScaler {}

impl ESScaler for LassESScaler {
    fn scale_for_fn(&mut self, env: &SimEnv, fnid: FnId, metric: &ContainerMetric) {
        // 请求时间=请求数/(当前容器数(cc)*每个容器请求处理速率(r/t))
        let mut desired_container_cnt = if
            metric.ready_2_schedule_fn_count() + metric.scheduled_fn_count == 0
        {
            0
        } else {
            let recent_speed = {
                let mut recent_speed_sum = 0.0;
                let mut recent_speed_cnt = 0;

                if let Some(nodes) = env.fn_2_nodes.borrow().get(&fnid) {
                    nodes.iter().for_each(|&nodeid| {
                        let node = env.node(nodeid);
                        let container = node.fn_containers.get(&fnid).unwrap();

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
                (((metric.ready_2_schedule_fn_count() + metric.scheduled_fn_count) as f32) /
                    (self.latency_required * recent_speed).ceil()) as usize
            }
        };
        let container_cnt = metric.container_count;
        desired_container_cnt = self.scale_down_policy.filter_desired(
            fnid,
            desired_container_cnt,
            container_cnt
        );
        if desired_container_cnt < container_cnt {
            // # scale down
            let scale = container_cnt - desired_container_cnt;

            env.scale_executor
                .borrow_mut()
                .scale_down(env, ScaleOption::new().for_spec_fn(fnid).with_scale_cnt(scale));
        } else {
            // # scale up
            let scale = desired_container_cnt - container_cnt;
            env.scale_executor.borrow_mut().scale_up(env, fnid, scale);
        }
    }
}
