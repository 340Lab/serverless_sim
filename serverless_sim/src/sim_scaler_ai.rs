// use crate::{
//     actions::{ Action, AdjustThres },
//     sim_env::SimEnv,
//     sim_scaler::{ ScaleArg, Scaler },
//     // sim_scaler_lass::LassScaler,
// };

// pub struct AIScaler {
//     pub lass_scaler: LassScaler,
// }

// impl AIScaler {
//     pub fn new() -> Self {
//         Self { lass_scaler: LassScaler::new() }
//     }

//     fn apply_adjust(&mut self, adj: AdjustThres) {
//         match adj {
//             AdjustThres::Up => {
//                 self.lass_scaler.latency_required += 0.1;
//             }
//             AdjustThres::DOwn => {
//                 self.lass_scaler.latency_required -= 0.1;
//             }
//             AdjustThres::Keep => {}
//         }
//     }
// }
// impl Scaler for AIScaler {
//     fn scale(&mut self, sim_env: &SimEnv, arg: ScaleArg) {
//         let action = match arg {
//             ScaleArg::AIScaler(action) => action,
//             _ => panic!("not match"),
//         };

//         match action {
//             Action::ScaleUp(adjust) => {
//                 self.apply_adjust(adjust);
//                 self.lass_scaler.scale(sim_env, ScaleArg::LassScaler(Action::ScaleUp(adjust)));
//             }
//             Action::ScaleDown(adjust) => {
//                 self.apply_adjust(adjust);
//                 self.lass_scaler.scale(sim_env, ScaleArg::LassScaler(Action::ScaleDown(adjust)));
//             }
//             Action::DoNothing => {}
//             Action::AllowAll(adjust) => {
//                 self.apply_adjust(adjust);
//                 self.lass_scaler.scale(sim_env, ScaleArg::LassScaler(Action::AllowAll(adjust)));
//             }
//         }
//     }
// }
