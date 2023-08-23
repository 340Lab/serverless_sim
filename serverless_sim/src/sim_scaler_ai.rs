use crate::{
    actions::Action,
    sim_env::SimEnv,
    sim_scale_executor::{ScaleExecutor, ScaleOption},
    sim_scaler::{ScaleArg, Scaler},
};

pub struct AIScaler;

impl Scaler for AIScaler {
    fn scale(&mut self, sim_env: &SimEnv, arg: ScaleArg) {
        let action = match arg {
            ScaleArg::AIScaler(action) => action,
            _ => panic!("not match"),
        };
        match action {
            Action::ScaleUpWithoutElem => {
                if let Some((_req, fnid, _gid)) = sim_env.get_request_first_unscheduled_fn() {
                    sim_env
                        .scale_executor
                        .borrow_mut()
                        .scale_up(sim_env, fnid, 1);
                }
            }
            Action::ScaleUpWithElem => {
                if let Some((_req, fnid, _gid)) = sim_env.get_request_first_unscheduled_fn() {
                    sim_env
                        .scale_executor
                        .borrow_mut()
                        .scale_up(sim_env, fnid, 1);
                }
            }
            Action::ProactiveScaleDown => sim_env
                .scale_executor
                .borrow_mut()
                .scale_down(sim_env, ScaleOption::new()),
            Action::DoNothing => {}
        }
    }
}
