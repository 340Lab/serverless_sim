pub mod down_exec;
pub mod num;
pub mod up_exec;

// pub fn prepare_spec_scaler(config: &Config) -> Option<Box<dyn ScaleNum + Send>> {
//     let es = &config.es;

//     if es.scale_lass() {
//         return Some(Box::new(LassScaleNum::new()));
//     }
//     // } else if es.sche_fnsche() {
//     //     return Some(Box::new(FnScheScaler::new()));
//     // } else
//     if es.scale_hpa() {
//         return Some(Box::new(HpaScaleNum::new()));
//     } else if es.scale_ai() {
//         return Some(Box::new(AIScaleNum::new(config)));
//     } else if es.scale_up_no() {
//         return Some(Box::new(NoScaleNum::new()));
//     }

//     None
// }
