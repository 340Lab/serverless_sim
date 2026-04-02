use super::ScaleUpExec;
use crate::cache::flame::{global_fn_score, global_is_hot};
use crate::cache::rbuc::global_replica_delta;
use crate::mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes};
use crate::node::EnvNodeExt;
use crate::with_env_sub::WithEnvHelp;
use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{SimEnvObserve, UpCmd},
};

pub struct LeastTaskScaleUpExec;

impl LeastTaskScaleUpExec {
    pub fn new() -> Self {
        LeastTaskScaleUpExec {}
    }
}

impl ScaleUpExec for LeastTaskScaleUpExec {
    fn exec_scale_up(
        &self,
        target_cnt: usize,
        fnid: FnId,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) -> Vec<UpCmd> {
        let mech_metric = || env.help().mech_metric_mut();
        let mut up_cmds = vec![];

        let mut nodes_no_container = env
            .nodes()
            .iter()
            .filter(|n| n.container(fnid).is_none())
            .map(|n| n.node_id())
            .collect::<Vec<_>>();

        let nodes_with_container_cnt = env.nodes().len() - nodes_no_container.len();

        if nodes_with_container_cnt < target_cnt && !nodes_no_container.is_empty() {
            let to_scale_up_cnt = std::cmp::min(
                target_cnt - nodes_with_container_cnt,
                nodes_no_container.len(),
            );

            let cache_policy = env.help().config().mech.instance_cache_policy_conf().0;
            if cache_policy == "flame" && global_is_hot(fnid) {
                let need_mem = env
                    .func(fnid)
                    .cold_start_container_mem_use
                    .max(env.func(fnid).container_mem());
                nodes_no_container.sort_by(|&a, &b| {
                    let flame_priority = |nid| {
                        let node = env.node(nid);
                        let reclaimable_temp_mem = node
                            .fn_containers
                            .borrow()
                            .iter()
                            .filter_map(|(fid, container)| {
                                if container.is_idle() && !global_is_hot(*fid) {
                                    Some(env.func(*fid).container_mem())
                                } else {
                                    None
                                }
                            })
                            .sum::<f32>();
                        let available_mem =
                            node.left_mem_for_place_container() + reclaimable_temp_mem;
                        let hot_score = node
                            .fn_containers
                            .borrow()
                            .keys()
                            .filter(|fid| global_is_hot(**fid))
                            .map(|fid| global_fn_score(*fid))
                            .sum::<f32>();
                        let priority = if available_mem + 0.00001 < need_mem {
                            -1.0
                        } else if hot_score <= 0.00001 {
                            available_mem
                        } else {
                            available_mem / hot_score
                        };
                        (priority, available_mem)
                    };
                    let ap = flame_priority(a);
                    let bp = flame_priority(b);
                    ap.0.partial_cmp(&bp.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| ap.1.partial_cmp(&bp.1).unwrap_or(std::cmp::Ordering::Equal))
                });
            } else if cache_policy == "rbuc" {
                let replica_delta = global_replica_delta(fnid);
                nodes_no_container.sort_by(|&a, &b| {
                    let a_mem = env.node(a).left_mem_for_place_container();
                    let b_mem = env.node(b).left_mem_for_place_container();
                    let a_tasks = mech_metric().node_task_new_cnt(a);
                    let b_tasks = mech_metric().node_task_new_cnt(b);

                    if replica_delta > 0 {
                        a_mem
                            .partial_cmp(&b_mem)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| b_tasks.cmp(&a_tasks))
                    } else {
                        b_tasks
                            .cmp(&a_tasks)
                            .then_with(|| {
                                a_mem
                                    .partial_cmp(&b_mem)
                                    .unwrap_or(std::cmp::Ordering::Equal)
                            })
                    }
                });
            } else {
                // Keep original behavior: prefer nodes with fewer tasks.
                nodes_no_container.sort_by(|&a, &b| {
                    let acnt = mech_metric().node_task_new_cnt(a);
                    let bcnt = mech_metric().node_task_new_cnt(b);
                    acnt.partial_cmp(&bcnt).unwrap()
                });
            }

            if cache_policy != "rbuc" && cache_policy != "flame" {
                nodes_no_container.reverse();
            }
            for _ in 0..to_scale_up_cnt {
                let node_2_load_contaienr = nodes_no_container.pop().unwrap();
                let _ = cmd_distributor.send(MechScheduleOnceRes::ScaleUpCmd(UpCmd {
                    nid: node_2_load_contaienr,
                    fnid,
                }));
                up_cmds.push(UpCmd {
                    nid: node_2_load_contaienr,
                    fnid,
                })
            }
        }

        up_cmds
    }
}
