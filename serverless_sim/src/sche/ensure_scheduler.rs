use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{DownCmd, MechType, MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{self, EnvNodeExt, Node, NodeId},
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

struct NodeCpuResc {
    cpu_limit: f32,
    all_task_cnt: f32,
}

pub struct EnsureScheduler {
    fn_nodes: HashMap<FnId, HashSet<NodeId>>,
    node_cpu_usage: HashMap<NodeId, NodeCpuResc>,
}

impl EnsureScheduler {
    pub fn new() -> Self {
        Self {
            fn_nodes: HashMap::new(),
            node_cpu_usage: HashMap::new(),
        }
    }

    fn select_best_node_to_fn(&self, fnid: usize, env: &SimEnvObserve) -> usize {
        // 先取出该函数所需要的cpu
        let fn_cpu_use = env.func(fnid).cpu;

        let nodes = self.fn_nodes.get(&fnid).unwrap();

        let mut best_nodeid = 9999;
        let mut best_node_cpu_local = 0.0;

        for nodeid in nodes {
            if best_nodeid == 9999 {
                best_nodeid = *nodeid;
            }

            let node_rsc = self.node_cpu_usage.get(nodeid).unwrap();

            // 取出cpu分配额
            let cpu_local = node_rsc.cpu_limit / (node_rsc.all_task_cnt + 1.0);

            if fn_cpu_use / cpu_local <= 5.0 {
                return *nodeid;
            }

            if cpu_local > best_node_cpu_local {
                best_nodeid = *nodeid;
                best_node_cpu_local = cpu_local;
            }

        }
        
        best_nodeid
    }
}

impl Scheduler for EnsureScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,) 
    {
        // 遍历每个节点，更新其资源使用情况
        for node in env.core().nodes().iter() {
            // cpu
            let cpu_limit = env.node(node.node_id()).rsc_limit.cpu;

            // 任务数量
            let all_task_cnt = node.all_task_cnt() as f32;

            self.node_cpu_usage.insert(node.node_id(), NodeCpuResc{cpu_limit, all_task_cnt});
        }

        let mut need_schedule_fn = HashSet::new();
        // 找到这一帧需要调度的函数
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            let schedule_able_fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::PreAllSched,
            );
            for fnid in schedule_able_fns.iter() {
                need_schedule_fn.insert(*fnid);
            }
        }

        for func in env.core().fns().iter() {

            let mut nodes = env
                .core().fn_2_nodes()
                .get(&func.fn_id)
                .map(|v| { v.clone() })
                .unwrap_or(HashSet::new());

            let target = mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target > cur{
                let up_cmd = mech.scale_up_exec().exec_scale_up(
                    target,
                    func.fn_id, env,
                    cmd_distributor
                );

                // 实时更新函数的节点情况
                for cmd in up_cmd.iter() {
                    nodes.insert(cmd.nid);
                }
            }

            if !need_schedule_fn.contains(&func.fn_id) {
                env.fn_containers_for_each(func.fn_id, |container| {
                    // 如果该容器最近50帧都是空闲则缩容
                    if container.recent_frame_is_idle(50) && container.req_fn_state.len() == 0  {
                        // 发送缩容命令
                        cmd_distributor
                            .send(MechScheduleOnceRes::ScaleDownCmd(DownCmd 
                                {
                                    nid: container.node_id,
                                    fnid: func.fn_id
                                }
                            ))
                            .unwrap();
                        nodes.remove(&container.node_id);
                    }
                });
            }
            
            log::info!("fn {}, nodes.len() = {}", func.fn_id, nodes.len());
            self.fn_nodes.insert(func.fn_id, nodes.clone());
        }

        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );

            //迭代请求中的函数，选择最合适的节点进行调度
            for fnid in fns {
                let sche_nodeid = self.select_best_node_to_fn(fnid, env);

                log::info!("schedule fn {} to node {}", fnid, sche_nodeid);

                if sche_nodeid != 9999 {
                    cmd_distributor
                        .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                            nid: sche_nodeid,
                            reqid: req.req_id,
                            fnid,
                            memlimit: None,
                        }))
                        .unwrap();
                    self.node_cpu_usage.get_mut(&sche_nodeid).unwrap().all_task_cnt += 1.0;
                }
            }

        }

    }
}
