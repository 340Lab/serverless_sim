use std::collections::HashMap;

use crate::{
    actions::ESActionWrapper,
    algos::ContainerMetric,
    fn_dag::FnId,
    mechanism::{DownCmd, ScheCmd, UpCmd},
    node::NodeId,
    sim_env::SimEnv,
    sim_run::{schedule_helper, Scheduler},
};

pub struct FnScheScheduler {
    // fn_default:HashMap<FnId,NodeId>
}

impl FnScheScheduler {
    pub fn new() -> Self {
        Self {
            // fn_default:HashMap::new(),
        }
    }
}

impl FnScheScheduler {
    fn select_node_for_fn(&mut self, env: &SimEnv, fnid: FnId) -> NodeId {
        for n in 0..env.node_cnt() {
            if env.node(n).last_frame_cpu < 0.8 {
                // self.fn_default.insert(fnid,n);
                return n;
            }
        }
        env.nodes()
            .iter()
            .min_by(|a, b| a.all_task_cnt().partial_cmp(&b.all_task_cnt()).unwrap())
            .unwrap()
            .node_id()
    }
}

impl Scheduler for FnScheScheduler {
    fn schedule_some(&mut self, env: &SimEnv) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        let mut sche_cmds = vec![];
        for (_req_id, req) in env.core.requests_mut().iter_mut() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            for fnid in fns {
                let nodeid = self.select_node_for_fn(env, fnid);
                // {
                // env.schedule_reqfn_on_node(req, fnid, nodeid);
                // }
                sche_cmds.push(ScheCmd {
                    nid: nodeid,
                    reqid: req.req_id,
                    fnid,
                    memlimit: None,
                })
            }
        }
        (vec![], sche_cmds, vec![])
    }

    // fn fn_available_count(&self, fnid: FnId, env: &SimEnv) -> usize {
    //     0
    // }
    // fn scale_for_fn(
    //     &mut self,
    //     env: &SimEnv,
    //     fnid: FnId,
    //     metric: &ContainerMetric,
    //     action: &ESActionWrapper,
    // ) -> (f32, bool) {
    //     // 对于容器一段时间未使用，就执行缩减
    //     // 优先扩容到索引小的node上
    //     let mut containers_2_zero = vec![];
    //     if let Some(nodes) = env.fn_2_nodes.borrow().get(&fnid) {
    //         for &nodeid in nodes.iter() {
    //             let node = env.node(nodeid);
    //             let container = node.container(fnid).unwrap();

    //             if container.recent_frame_is_idle(3) && container.req_fn_state.len() == 0 {
    //                 containers_2_zero.push((fnid, nodeid));
    //                 // log::info!("scale down fn {} on node {}", fnid, nodeid);
    //             } else {
    //                 // log::info!(
    //                 //     "keep fn {} on node {} left task {} working_rec {:?}",
    //                 //     fnid,
    //                 //     nodeid,
    //                 //     container.req_fn_state.len(),
    //                 //     container.recent_frames_working_cnt
    //                 // );
    //             }
    //         }
    //     }
    //     containers_2_zero.iter().for_each(|&(fnid, nodeid)| {
    //         env.scale_executor
    //             .borrow_mut()
    //             .scale_down(env, ScaleOption::ForSpecNodeFn(nodeid, fnid));
    //     });

    //     for &req_id in &metric.ready_2_schedule_fn_reqs {
    //         // 寻找一个有空间的node进行调度
    //         let mut found_node = None;
    //         for n in env.nodes.borrow_mut().iter_mut() {
    //             if n.container(fnid).is_some() {
    //                 if n.left_mem() / ((n.running_task_cnt() + 1) as f32) < env.func(fnid).mem {
    //                     continue;
    //                 }
    //                 found_node = Some(n.node_id());
    //                 break;
    //             } else if n.mem_enough_for_container(&env.func(fnid)) {
    //                 found_node = Some(n.node_id());
    //                 break;
    //             }
    //         }
    //         if let Some(found_node) = found_node {
    //             // log::info!("Found node for fn {} on node {}", fnid, found_node);
    //             if env.node(found_node).container(fnid).is_none() {
    //                 env.scale_executor.borrow_mut().scale_up_fn_to_nodes(
    //                     env,
    //                     fnid,
    //                     &vec![found_node],
    //                 );
    //             }
    //             let mut req = env.request_mut(req_id);
    //             env.schedule_reqfn_on_node(&mut *req, fnid, found_node);
    //         } else {
    //         }
    //         // log::info!("schedule req {} to node {}", req_id, found_node);
    //     }
    //     (0.0, false)
    // }
}
