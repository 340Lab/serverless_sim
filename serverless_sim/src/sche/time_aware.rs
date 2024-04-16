// use std::{borrow::BorrowMut, collections::HashMap, env::consts, u128::MAX};

// use daggy::{
//     petgraph::visit::{EdgeRef, IntoEdgeReferences},
//     EdgeIndex, Walker,
// };
// use rand::{thread_rng, Rng};

// use crate::{
//     fn_dag::{DagId, FnId},
//     node::NodeId,
//     request::{ReqId, Request},
//     sim_env::SimEnv,
//     sim_run::{
//         schedule_helper::{collect_task_to_sche, CollectTaskConfig},
//         Scheduler,
//     },
//     util,
// };

// struct RequestSchedulePlan {
//     fn_nodes: HashMap<FnId, NodeId>,
// }

// pub struct TimeScheduler {
//     // dag_fn_prorities: HashMap<DagId, Vec<(FnId, f32)>>,
//     // dag_fn_prorities_: HashMap<DagId, HashMap<FnId, f32>>,
//     // 记录task的触发时间（入度为0的函数是请求到达的时间，否则是前驱函数完成的时间）
//     fn_trigger_time: HashMap<(ReqId, FnId), usize>,
//     starve_threshold: usize, //设置一个等待阈值
// }

// impl Scheduler for TimeScheduler {
//     fn schedule_some(&mut self, env: &SimEnv) {
//         let tasks = self.step_1_collct_all_task(env);
//         let (starve, mut unstarve) = self.step_2_split_tasks(tasks, env);
//         log::info!("starve len {}", starve.len());
//         log::info!("unstarve len {}", unstarve.len());
//         self.step_3_sort_unstarve_tasks(&mut unstarve, env);
//         self.step_4_select_node_for_tasks(env, starve);
//         self.step_4_select_node_for_tasks(env, unstarve);
//     }

//     fn prepare_this_turn_will_schedule(&mut self, env: &SimEnv) {}
//     fn this_turn_will_schedule(&self, fnid: FnId) -> bool {
//         true
//     }
// }

// // 基于时间感知的函数调度算法
// impl TimeScheduler {
//     pub fn new() -> Self {
//         Self {
//             fn_trigger_time: HashMap::new(),
//             starve_threshold: 10, //设置等待时间的阈值
//         }
//     }

//     fn step_1_collct_all_task(&mut self, env: &SimEnv) -> Vec<(ReqId, FnId)> {
//         // 1、collect前驱已经执行完，当前函数未被调度的
//         let mut tasks: Vec<(ReqId, FnId)> = vec![];
//         for (reqid, r) in env.core.requests().iter() {
//             let ts = collect_task_to_sche(&r, env, CollectTaskConfig::PreAllDone);
//             tasks.append(
//                 &mut ts
//                     .into_iter()
//                     .map(|fnid| (*reqid, fnid))
//                     .collect::<Vec<_>>(),
//             )
//         }
//         return tasks;
//     }

//     fn step_2_split_tasks(
//         &mut self,
//         tasks: Vec<(ReqId, FnId)>,
//         env: &SimEnv,
//     ) -> (Vec<(ReqId, FnId)>, Vec<(ReqId, FnId)>) {
//         // 返回饥饿数组和非饥饿数组

//         // 2、处理超时的task，增加优先级
//         // 当使用 FuncSched 调度时，如果优先级P[k]较高的函数请求不断到达，可能会导致有些函数的饥饿.这是因为整个服务器无感知计算平台的资源可能会
//         // 一直分配给不断到达的函数高优先级请求，而其他函数请求一直处于待执行状态.
//         // 对于这种状况，FuncSched 会维护一个更高的优先级队列Qstarve ，并设置一个可调节的阈值 StarveThreshold.
//         // 当一 个 函 数 请 求 的 等 待 时 间F[k]s − F[k]a > StarveThreshold 时，该请求将会被调入 .
//         // 在Qstarve中，所有函数请求按照等待时间从大到小排序，
//         // 队头函数请求将被 FuncSched 调度器最先执行. 当Qstarve为空时 ，剩余函数请求再按照函数请求优先级P[k]执行. 依靠这一机制，
//         // FuncSched 能够避免低优先级函数请求的饥饿情况

//         let mut startve_tasks: Vec<(ReqId, FnId)> = vec![];
//         let mut unstartve_tasks: Vec<(ReqId, FnId)> = vec![];

//         for (reqid, fnid) in tasks {
//             let req = env.request(reqid);
//             let func = env.func(fnid);
//             let func_pres_id = func.parent_fns(env);
//             // 计算函数的触发时间
//             if (!self.fn_trigger_time.contains_key(&(reqid, fnid))) {
//                 // 没有前驱函数
//                 if func_pres_id.len() == 0 {
//                     self.fn_trigger_time.insert((reqid, fnid), req.begin_frame);
//                 } else {
//                     // 最晚完成的前驱函数的结束时间
//                     //   Request has pub done_fns: HashMap<FnId, usize>,
//                     // let mut fun_pres_time = HashMap::<FnId, usize>::new();
//                     let mut pres_end_time = 0;
//                     for id in func_pres_id {
//                         let func_pre_time = *req.done_fns.get(&id).unwrap();
//                         if func_pre_time > pres_end_time {
//                             pres_end_time = func_pre_time;
//                         }
//                         // fun_pres_time.insert(id, func_pre_time);
//                     }
//                     self.fn_trigger_time.insert((reqid, fnid), pres_end_time);
//                 }
//             }
//             // 计算函数的等待时间
//             let mut wait_time =
//                 *env.core.current_frame() - self.fn_trigger_time.get(&(reqid, fnid)).unwrap();

//             // 拿出超时任务
//             if wait_time > self.starve_threshold {
//                 startve_tasks.push((reqid, fnid));
//             } else {
//                 unstartve_tasks.push((reqid, fnid));
//             }
//         }
//         // 计算函数等待的优先级队列Qstarve
//         startve_tasks.sort_by(|(reqid1, fnid1), (reqid2, fnid2)| {
//             let mut time1 =
//                 *env.core.current_frame() - self.fn_trigger_time.get(&(*reqid1, *fnid1)).unwrap();
//             let mut time2 =
//                 *env.core.current_frame() - self.fn_trigger_time.get(&(*reqid2, *fnid2)).unwrap();
//             // 降序排
//             time2.cmp(&time1)
//         });

//         return (startve_tasks, unstartve_tasks);
//     }

//     // 计算非饥饿任务的优先级
//     fn step_3_sort_unstarve_tasks(
//         &mut self,
//         unstartve_tasks: &mut Vec<(ReqId, FnId)>,
//         env: &SimEnv,
//     ) {
//         let mut tasks_prio: HashMap<(ReqId, FnId), f32> = HashMap::new();

//         for (reqid, fnid) in unstartve_tasks.iter() {
//             let func = env.func(*fnid);
//             // P = 函数的资源消耗量×(启动时间+函数执行时间(已知，故这设置了固定的CPU表示))
//             // let t_exe = func.cpu / 100.0;
//             let t_exe = func.cpu;
//             let p = func.mem * (t_exe + func.cold_start_time as f32);
//             tasks_prio.insert((*reqid, *fnid), p);
//         }
//         // 升序排序任务的优先级

//         unstartve_tasks.sort_by(|(reqid1, fnid1), (reqid2, fnid2)| {
//             let mut p1 = *tasks_prio.get(&(*reqid1, *fnid1)).unwrap();
//             let mut p2 = *tasks_prio.get(&(*reqid2, *fnid2)).unwrap();
//             p1.partial_cmp(&p2).unwrap()
//         });
//     }

//     // 4、为每一个task选择node Least Loaded:将请求派发到负载最低的Worker中

//     fn step_4_select_node_for_tasks(&mut self, env: &SimEnv, tasks: Vec<(ReqId, FnId)>) {
//         for (reqid, fnid) in tasks {
//             let mut least_task = 1000000;
//             let mut least_task_id = None;

//             let func = env.func(fnid);
//             let mut req = env.request_mut(reqid);
//             // 选择任务数最小的节点依次分配给任务
//             for nodeid in 0..env.nodes().len() {
//                 let node = env.node(nodeid);
//                 if func.mem < node.left_mem() + 500.0 {
//                     if node.all_task_cnt() < least_task {
//                         least_task = node.all_task_cnt();
//                         least_task_id = Some(nodeid);
//                     }
//                 }
//             }
//             if let Some(least_task_id) = least_task_id {
//                 log::info!(
//                     "schedule_reqfn_on_node {} {} {}",
//                     reqid,
//                     fnid,
//                     least_task_id
//                 );
//                 env.schedule_reqfn_on_node(&mut req, fnid, least_task_id);
//             } else {
//                 log::info!("schedule_reqfn_on_node didn't find node");
//             }
//         }
//     }
// }
