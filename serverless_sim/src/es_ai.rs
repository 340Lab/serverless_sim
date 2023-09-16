use crate::{
    es::{ ESState, StageScaleForFns, ActionEffectStage, ESScaler },
    actions::ESActionWrapper,
    sim_env::SimEnv,
    scale_executor::{ ScaleOption, ScaleExecutor },
    config::Config,
};

pub struct AIScaler {}

impl AIScaler {
    pub fn new(config: &Config) -> Self {
        Self {}
    }
}

impl ESScaler for AIScaler {
    fn scale_for_fn(
        &mut self,
        env: &SimEnv,
        fnid: crate::fn_dag::FnId,
        metric: &crate::algos::ContainerMetric,
        action: &ESActionWrapper
    ) -> (f32, bool) {
        let raw_action = match action {
            ESActionWrapper::Int(raw_action) => *raw_action,
        };
        let mut desired_container_cnt = (raw_action % 10) as usize;
        let container_cnt = env.fn_container_cnt(fnid);
        let mut score_trans = 0.0;

        // skip
        if raw_action == 11 {
            desired_container_cnt = container_cnt;
        }

        // no need to scale up
        if metric.ready_2_schedule_fn_reqs.len() == 0 && desired_container_cnt > container_cnt {
            desired_container_cnt = container_cnt;
            score_trans -= 500.0;
        }

        // can't scale down to 0
        if metric.ready_2_schedule_fn_reqs.len() != 0 && desired_container_cnt == 0 {
            // return -1000.0;
            desired_container_cnt = 1;
            score_trans -= 1000.0;
        }

        log::info!(
            "fnid: {}, desired_container_cnt: {}, total: {}",
            fnid,
            desired_container_cnt,
            env.fns.borrow().len()
        );

        if desired_container_cnt < container_cnt {
            // # scale down
            let scale = container_cnt - desired_container_cnt;

            env.scale_executor
                .borrow_mut()
                .scale_down(env, ScaleOption::new().for_spec_fn(fnid).with_scale_cnt(scale));
        } else if desired_container_cnt > container_cnt {
            // # scale up
            let scale = desired_container_cnt - container_cnt;
            env.scale_executor.borrow_mut().scale_up(env, fnid, scale);
        }
        (score_trans, true)
    }
}

// /// return continue loop or not
// pub fn step_scale(
//     env: &SimEnv,
//     raw_action: &ESActionWrapper,
//     action_done: &mut bool,
//     action_score: &mut f32,
//     es_state: &mut ESState
// ) -> bool {
//     if *action_done {
//         // next action effect stage is prepared
//         return false;
//     }
//     log::info!("scale for fns");
//     *action_done = true;
//     let action = match raw_action {
//         // ESActionWrapper::Float(raw_action) => (*raw_action * 11.0) as u32,
//         ESActionWrapper::Int(raw_action) => *raw_action,
//     };
//     *action_score += step_scale_for_fns(
//         env,
//         action,
//         es_state.stage.as_scale_for_fns_mut().unwrap()
//     );
//     if !es_state.stage.as_scale_for_fns_mut().unwrap().prepare_next() {
//         es_state.trans_stage(env);
//     }
//     true
// }
