// use crate::{
//     sim_env::SimEnv,
//     sim_scaler::{ Scaler, ScaleArg },
//     sim_scale_executor::{ ScaleExecutor, ScaleOption },
// };

// pub struct LassScaler {
//     pub latency_required: f32,
// }

// impl LassScaler {
//     pub fn new() -> Self {
//         Self {
//             latency_required: 7.0,
//         }
//     }
// }

// impl Scaler for LassScaler {
//     fn scale(&mut self, sim_env: &SimEnv, arg: ScaleArg) {
//         let collect = sim_env.algo_collect_ready_2_schedule_metric();
//         for (&fnid, metric) in collect.iter() {
//             // 请求时间=请求数/(当前容器数(cc)*每个容器请求处理速率(r/t))
//             let desired_container_cnt = if
//                 metric.ready_2_schedule_fn_count + metric.scheduled_fn_count == 0
//             {
//                 0
//             } else {
//                 let recent_speed = {
//                     let mut recent_speed_sum = 0.0;
//                     let mut recent_speed_cnt = 0;

//                     if let Some(nodes) = sim_env.fn_2_nodes.borrow().get(&fnid) {
//                         nodes.iter().for_each(|&nodeid| {
//                             let node = sim_env.node(nodeid);
//                             let container = node.fn_containers.get(&fnid).unwrap();

//                             recent_speed_sum += container.recent_handle_speed();
//                             recent_speed_cnt += 1;
//                         });

//                         if recent_speed_cnt == 0 {
//                             0.0
//                         } else {
//                             recent_speed_sum / (recent_speed_cnt as f32)
//                         }
//                     } else {
//                         0.0
//                     }
//                 };
//                 if recent_speed < 0.00001 {
//                     1
//                 } else {
//                     (((metric.ready_2_schedule_fn_count + metric.scheduled_fn_count) as f32) /
//                         (self.latency_required * recent_speed).ceil()) as usize
//                 }
//             };

//             let container_cnt = metric.container_count;
//             if desired_container_cnt < container_cnt {
//                 // # scale down
//                 let scale = container_cnt - desired_container_cnt;
//                 if arg.lass_down() {
//                     sim_env.scale_executor
//                         .borrow_mut()
//                         .scale_down(
//                             sim_env,
//                             ScaleOption::new().for_spec_fn(fnid).with_scale_cnt(scale)
//                         );
//                 }
//             } else {
//                 if arg.lass_up() {
//                     // # scale up
//                     let scale = desired_container_cnt - container_cnt;
//                     sim_env.scale_executor.borrow_mut().scale_up(sim_env, fnid, scale);
//                 }
//             }
//         }
//     }
// }
