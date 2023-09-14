use crate::{
    sim_ef::{ EFState, StageScaleForFns, ActionEffectStage },
    actions::EFActionWrapper,
    sim_env::SimEnv,
    sim_scale_executor::{ ScaleOption, ScaleExecutor },
};

/// return continue loop or not
pub fn step_scale(
    env: &SimEnv,
    raw_action: &EFActionWrapper,
    action_done: &mut bool,
    action_score: &mut f32,
    aief_state: &mut EFState
) -> bool {
    if *action_done {
        // next action effect stage is prepared
        return false;
    }
    log::info!("scale for fns");
    *action_done = true;
    let action = match raw_action {
        EFActionWrapper::Float(raw_action) => (*raw_action * 11.0) as u32,
        EFActionWrapper::Int(raw_action) => *raw_action,
    };
    *action_score += step_scale_for_fns(
        env,
        action,
        aief_state.stage.as_scale_for_fns_mut().unwrap()
    );
    if !aief_state.stage.as_scale_for_fns_mut().unwrap().prepare_next() {
        aief_state.trans_stage(env);
    }
    true
}

/// # Panic
/// - if no fn left
///
/// return score trans
fn step_scale_for_fns(env: &SimEnv, raw_action: u32, stage: &mut StageScaleForFns) -> f32 {
    // let mut iter = stage.ready_2_schedule.iter_mut();
    let fnid = stage.current_fnid.unwrap();
    let mut desired_container_cnt = (raw_action % 10) as usize;
    let container_cnt = env.fn_container_cnt(fnid);
    let mut score_trans = 0.0;

    if !stage.fn_need_schedule.contains_key(&fnid) && desired_container_cnt > container_cnt {
        desired_container_cnt = container_cnt;
        score_trans -= 500.0;
    }

    if stage.fn_need_schedule.contains_key(&fnid) && desired_container_cnt == 0 {
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
    } else {
        // # scale up
        let scale = desired_container_cnt - container_cnt;
        env.scale_executor.borrow_mut().scale_up(env, fnid, scale);
    }
    score_trans
    // stage.scaled.push((fnid, desired_container_cnt, raw_action));
}
