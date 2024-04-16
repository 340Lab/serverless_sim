// // fn	20000	函数id
// // action	30000	action
// // con_tar	40000	目标容器数
// // con	50000	容器真实数
// // win	60000	历史观测窗口大小
// // stage	70000	阶段类型
// // frame	80000	帧数
// // time	90000	平均请求延迟
// // cold	100000	函数冷启动时间
// // dag	110000	dagid
// // nodefncon	120000	是否有该fn container
// // noderun	130000	预计放在node处运行时间
// // running	140000	容器运行任务数
// // tosche	150000	fn待调度任务数
// // speed	160000	容器处理速率
// // cpu	170000	节点cpu
// // mem	180000	节点mem
// // nodecon	190000	节点容器数
// // nodetask	200000	节点任务数

// use crate::{
//     sim_env::SimEnv,
//     es::{
//         StageSchedule,
//         StageScaleDown,
//         StageScaleForFns,
//         SCALE_DOWN_IDX,
//         SCHEDULE_IDX,
//         SCALE_FOR_FNS_IDX,
//     },
//     actions::RawActionHelper,
//     util::OrdF32,
// };

// #[allow(dead_code)]
// enum StateTag {
//     /// node	10000	节点id
//     Node,
//     /// fn	20000	函数id
//     Fn,
//     /// action	30000	action
//     Action,
//     /// con_tar	40000	目标容器数
//     ConTar,
//     /// con	50000	容器真实数
//     Con,
//     /// win	60000	历史观测窗口大小
//     Win,
//     /// stage	70000	阶段类型
//     Stage,
//     /// frame	80000	帧数
//     Frame,
//     /// time	90000	平均请求延迟
//     Time,
//     /// cold	100000	函数冷启动时间
//     Cold,
//     /// dag	110000	dagid
//     Dag,
//     /// nodefncon	120000	是否有该fn container
//     NodeFnCon,
//     /// noderun	130000	预计放在node处运行时间
//     NodeRun,
//     /// running	140000	容器运行任务数
//     Running,
//     /// tosche	150000	fn待调度任务数
//     ToSche,
//     /// speed	160000	容器处理速率
//     Speed,
//     /// cpu	170000	节点cpu
//     Cpu,
//     /// mem	180000	节点mem
//     Mem,
//     /// nodecon	190000	节点容器数
//     NodeCon,
//     /// nodetask	200000	节点任务数
//     NodeTask,
//     /// req    210000	请求id
//     Req,
//     /// conbusy    220000	容器忙碌度
//     ConBusy,
// }
// impl StateTag {
//     fn get_tag(&self) -> u32 {
//         match self {
//             Self::Node => 10000,
//             Self::Fn => 20000,
//             Self::Action => 30000,
//             Self::ConTar => 40000,
//             Self::Con => 50000,
//             Self::Win => 60000,
//             Self::Stage => 70000,
//             Self::Frame => 80000,
//             Self::Time => 90000,
//             Self::Cold => 100000,
//             Self::Dag => 110000,
//             Self::NodeFnCon => 120000,
//             Self::NodeRun => 130000,
//             Self::Running => 140000,
//             Self::ToSche => 150000,
//             Self::Speed => 160000,
//             Self::Cpu => 170000,
//             Self::Mem => 180000,
//             Self::NodeCon => 190000,
//             Self::NodeTask => 200000,
//             Self::Req => 210000,
//             Self::ConBusy => 220000,
//         }
//     }
// }

// pub struct StateBuffer {
//     state: Vec<f32>,
// }
// impl StateBuffer {
//     pub fn new() -> Self {
//         let mut state = Vec::new();
//         state.resize(400 * 400, 0.0);
//         Self { state }
//     }
//     pub fn serialize(&self) -> String {
//         serde_json::to_string(&self.state).unwrap()
//     }
//     fn set_x_y(&mut self, x: usize, y: usize, val: f32) {
//         let idx = x * 400 + y;
//         self.state[idx] = val;
//     }
//     #[allow(dead_code)]
//     fn set_col_pair(&mut self, x: usize, y: usize, tag: StateTag, val: f32) {
//         self.set_x_y(x, y, tag.get_tag() as f32);
//         self.set_x_y(x, y + 1, val);
//     }
//     fn set_row_pair(&mut self, x: usize, y: usize, tag: StateTag, val: f32) {
//         self.set_x_y(x, y, tag.get_tag() as f32);
//         self.set_x_y(x + 1, y, val);
//     }
//     fn set_x_y_tag(&mut self, x: usize, y: usize, tag: StateTag) {
//         self.set_x_y(x, y, tag.get_tag() as f32);
//     }
// }

// impl SimEnv {
//     // stage	阶段类型	fn	4	fn	1	2	。。。
//     // node	node	node	action
//     // nodecon	是否有该fn container			con_tar
//     // cold	node 冷启动时间（如果已有容器就不会有）			con
//     // nodetrans	预计放在node处传播时间			fn	1	2	。。。
//     // noderun	预计放在node处运行时间			action
//     //                 con_tar
//     //                 con
//     pub fn make_state_scale_for_fns(&self, state: &mut StateBuffer, stage: &mut StageScaleForFns) {
//         // let (fn_2_schedule, _metric) = stage.current_fn_to_scale.clone().unwrap();
//         // // let fn_2_schedule_cnt = metric.ready_2_schedule_fn_count;
//         // state.set_row_pair(0, 0, StateTag::Stage, SCALE_FOR_FNS_IDX as f32);
//         // let mut idx = 0;

//         // let history_offset_x = 3;
//         // let history_width = 400 - history_offset_x;
//         // for unscaled in stage.ready_2_schedule.iter() {
//         //     let &fnid = unscaled.0;
//         //     // let action = scaled.2;
//         //     // let scale_cnt = scaled.1;
//         //     let offset_x = history_offset_x + (idx % history_width);
//         //     let offset_y = (idx / history_width) * 1;
//         //     if idx % history_width == 0 {
//         //         state.set_x_y_tag(history_offset_x - 1, offset_y, StateTag::Fn);
//         //     }
//         //     state.set_x_y(offset_x, offset_y, fnid as f32);
//         //     idx += 1;
//         // }
//     }

//     // stage	阶段类型	fn	4	fn	1	2	。。。
//     //     node	node	node	action
//     // nodecon	是否有该fn container			node	-1 代表未选
//     // 预计放在node处运行时间
//     //                 fn	1	2	。。。
//     //                 action
//     //                 node
//     pub fn make_state_schedule(&self, state: &mut StateBuffer, stage: &mut StageSchedule) {
//         let (reqid, fnid) = stage.next_2_schedule;
//         // state.set_row_pair(0, 0, StateTag::Stage, SCHEDULE_IDX as f32);
//         // state.set_row_pair(2, 0, StateTag::Fn, fnid as f32);
//         // state.set_row_pair(2, 1, StateTag::Req, reqid as f32);
//         // for x in 0..self.node_cnt() {
//         //     let x = x + 1;
//         //     state.set_x_y_tag(x, 1, StateTag::Node);
//         // }
//         // state.set_x_y_tag(0, 2, StateTag::NodeFnCon);
//         // state.set_x_y_tag(0, 3, StateTag::Running);

//         // for x in 0..self.node_cnt() {
//         //     let x = x + 1;
//         //     let node = self.node(x - 1);
//         //     state.set_x_y(x, 2, if node.fn_containers.contains_key(&fnid) { 1.0 } else { 0.0 });
//         //     if let Some(_fn_con) = node.fn_containers.get(&fnid) {
//         //         let time = self.algo_predict_fn_on_node_work_time(
//         //             &self.request_mut(reqid),
//         //             fnid,
//         //             node.node_id()
//         //         );
//         //         state.set_x_y(x, 3, time);
//         //     }
//         // }
//         // let mut idx = 0;
//         // let history_offset_x = 4 + 1 + self.node_cnt();
//         // let history_width = 400 - history_offset_x;
//         // for &(reqid, fnid, nodeid, action) in stage.scheduled.iter() {
//         //     let offset_y = (idx / history_width) * 4;

//         //     if idx % history_width == 0 {
//         //         state.set_x_y_tag(history_offset_x - 1, offset_y, StateTag::Fn);
//         //         state.set_x_y_tag(history_offset_x - 1, offset_y + 1, StateTag::Req);

//         //         state.set_x_y_tag(history_offset_x, offset_y + 2, StateTag::Action);
//         //         state.set_x_y_tag(history_offset_x, offset_y + 3, StateTag::Node);
//         //     }
//         //     let offset_x = idx % history_width;
//         //     state.set_x_y(offset_x, offset_y, fnid as f32);
//         //     state.set_x_y(offset_x, offset_y + 1, reqid as f32);
//         //     state.set_x_y(offset_x, offset_y + 2, action as f32);
//         //     state.set_x_y(
//         //         offset_x,
//         //         offset_y + 3,
//         //         nodeid.map_or_else(
//         //             || -1.0,
//         //             |n| n as f32
//         //         )
//         //     );
//         //     idx += 1;
//         // }
//     }

//     pub fn make_state_scale_down(&self, state: &mut StateBuffer, stage: &mut StageScaleDown) {
//         let (nodeid, fnid) = stage.cur_container().unwrap();
//         state.set_row_pair(0, 0, StateTag::Stage, SCALE_DOWN_IDX as f32);
//         // state.set_row_pair(2, 0, StateTag::Fn, fnid as f32);
//         // state.set_row_pair(2, 1, StateTag::Node, nodeid as f32);

//         // state.set_row_pair(
//         //     0,
//         //     2,
//         //     StateTag::ConBusy,
//         //     self.node(nodeid).container(fnid).unwrap().busyness()
//         // );

//         let mut idx = 0;
//         let history_offset_x = 3;
//         let history_width = 400 - history_offset_x;
//         for &(nodeid, fnid, action) in stage.records.iter() {
//             let offset_y = (idx / history_width) * 2;
//             if idx % history_width == 0 {
//                 state.set_x_y_tag(history_offset_x - 1, offset_y, StateTag::Fn);
//                 // state.set_x_y_tag(history_offset_x - 1, offset_y + 1, StateTag::Action);
//                 state.set_x_y_tag(history_offset_x - 1, offset_y + 1, StateTag::Node);
//                 // state.set_x_y_tag(history_offset_x - 1, offset_y + 3, StateTag::Con);
//                 // state.set_x_y_tag(history_offset_x - 1, offset_y + 4, StateTag::NodeFnCon); // 代表是否有做scale down
//             }
//             let offset_x = history_offset_x + (idx % history_width);
//             state.set_x_y(offset_x, offset_y, fnid as f32);
//             // state.set_x_y(offset_x, offset_y + 1, action as f32);
//             state.set_x_y(offset_x, offset_y + 1, nodeid as f32);
//             // state.set_x_y(
//             //     offset_x,
//             //     offset_y + 3,
//             //     self.fn_2_nodes
//             //         .borrow()
//             //         .get(&fnid)
//             //         .map_or_else(
//             //             || 0.0,
//             //             |nodes| nodes.len() as f32
//             //         )
//             // );
//             // state.set_x_y(offset_x, offset_y + 4, if RawActionHelper(action).is_scale_down() {
//             //     1.0
//             // } else {
//             //     0.0
//             // });
//             idx += 1;
//         }
//     }

//     // win	历史窗口大小
//     // stage	阶段类型	frame	帧数	time	平均请求延迟	win	历史窗口大小
//     //         fn	fn	fn	fn	fn	fn
//     //         1	2	3	4	5	6
//     //     con	fn容器数
//     //     cold	函数冷启动时间
//     //     dag	函数dagid
//     //         函数输出大小

//     // node
//     // node
//     // node
//     pub fn make_common_state(&self, state: &mut StateBuffer, stage_idx: usize) {
//         let offset_y = 10;
//         state.set_x_y_tag(0, offset_y, StateTag::Win);
//         let offset_y = 11;
//         state.set_row_pair(0, offset_y, StateTag::Stage, stage_idx as f32);
//         state.set_row_pair(2, offset_y, StateTag::Frame, self.current_frame() as f32);
//         state.set_row_pair(4, offset_y, StateTag::Time, self.req_done_time_avg());
//         state.set_row_pair(6, offset_y, StateTag::Win, *self.each_fn_watch_window.borrow() as f32);
//         // let offset_y = 12;

//         let fns = self.fns.borrow();
//         let fwidth = 400 - 2;
//         for fid in 0..fns.len() {
//             let offset_y = (fid / fwidth) * (10 + self.node_cnt());
//             let fn_ = &fns[fid];
//             if fid % fwidth == 0 {
//                 state.set_x_y_tag(1, offset_y + 2, StateTag::Con);
//                 state.set_x_y_tag(1, offset_y + 3, StateTag::Cold);
//                 state.set_x_y_tag(1, offset_y + 4, StateTag::Dag);
//                 for n in self.nodes.borrow().iter() {
//                     let offset_y = offset_y + 10 + n.node_id() * 2;
//                     state.set_x_y_tag(0, offset_y, StateTag::Node);
//                     state.set_x_y(0, offset_y + 1, n.node_id() as f32);
//                     state.set_x_y_tag(1, offset_y, StateTag::Running);
//                     state.set_x_y_tag(1, offset_y + 1, StateTag::Speed);
//                 }
//             }
//             let x = 2 + (fid % fwidth);
//             state.set_x_y_tag(x, offset_y, StateTag::Fn);
//             state.set_x_y(x, offset_y + 1, fid as f32);
//             state.set_x_y(
//                 x,
//                 offset_y + 2,
//                 self.fn_2_nodes
//                     .borrow()
//                     .get(&fid)
//                     .map(|v| v.len())
//                     .unwrap_or(0) as f32
//             );
//             state.set_x_y(x, offset_y + 3, fn_.cold_start_time as f32);
//             state.set_x_y(x, offset_y + 4, fn_.dag_id as f32);
//             state.set_x_y(x, offset_y + 5, fn_.out_put_size as f32);
//             for n in self.nodes.borrow().iter() {
//                 let offset_y = offset_y + 10 + n.node_id() * 3;
//                 state.set_x_y(
//                     x,
//                     offset_y,
//                     n.fn_containers
//                         .get(&fid)
//                         .map(|con| { con.req_fn_state.len() as f32 })
//                         .unwrap_or(-1.0)
//                 );
//                 state.set_x_y(
//                     x,
//                     offset_y + 1,
//                     n.fn_containers
//                         .get(&fid)
//                         .map(|con| { con.recent_handle_speed() })
//                         .unwrap_or(-1.0)
//                 );
//             }
//         }
//         let offset_y = (self.node_cnt() * 2 + 10) * (fns.len() / fwidth);
//         state.set_x_y_tag(2, offset_y, StateTag::Cpu);
//         state.set_x_y_tag(3, offset_y, StateTag::Mem);
//         state.set_x_y_tag(4, offset_y, StateTag::NodeCon);
//         state.set_x_y_tag(5, offset_y, StateTag::NodeTask);

//         let offset_y = offset_y + 1;
//         for n in self.nodes.borrow().iter() {
//             let offset_y = offset_y + n.node_id();
//             state.set_row_pair(0, offset_y, StateTag::Node, n.node_id() as f32);
//             state.set_x_y(2, offset_y, n.cpu);
//             state.set_x_y(3, offset_y, n.mem);
//             state.set_x_y(4, offset_y, n.fn_containers.len() as f32);
//             state.set_x_y(5, offset_y, n.task_cnt() as f32);
//         }
//     }
// }
