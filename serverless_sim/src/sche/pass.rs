use std::collections::HashMap;

use daggy::Walker;
use rand::Rng;

use crate::{
    fn_dag::{DagId, EnvFnExt, FnId},
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::Request,
    sim_run::Scheduler,
    with_env_sub::WithEnvCore,
};

struct RequestSchedulePlan {
    fn_nodes: HashMap<FnId, NodeId>,
}

pub struct PassScheduler {
    dag_fn_prorities: HashMap<DagId, Vec<(FnId, f32)>>,
    // dag_fn_prorities_: HashMap<DagId, HashMap<FnId, f32>>,
}

impl PassScheduler {
    pub fn new() -> Self {
        Self {
            dag_fn_prorities: HashMap::new(),
        }
    }

    fn prepare_priority_for_dag(&mut self, req: &Request, env: &SimEnvObserve) {
        let dag = env.dag(req.dag_i);

        //计算函数的优先级：当函数i有多个后继，则优先分配选择传输时间+执行时间最大的后继函数
        if !self.dag_fn_prorities.contains_key(&dag.dag_i) {
            // map存储每个函数的优先级
            let mut map: HashMap<usize, f32> = HashMap::new();
            let mut walker = dag.new_dag_walker();
            let mut stack = vec![];
            //计算执行时间+数据传输时间
            while let Some(func_g_i) = walker.next(&dag.dag_inner) {
                let fnid = dag.dag_inner[func_g_i];
                let func = env.func(fnid);
                let node_low_id = env.node_get_lowest();
                let node = env.node(node_low_id);
                let t_exe = func.cpu / node.rsc_limit.cpu;

                let low_btw = env.node_btw_get_lowest();
                assert!(low_btw > 0.000001);
                let t_dir_trans = func.out_put_size / low_btw;

                map.insert(fnid, t_exe + t_dir_trans);

                stack.push(func_g_i);
            }
            //计算每个函数的优先级
            while let Some(func_g_i) = stack.pop() {
                let fnid = dag.dag_inner[func_g_i];
                let nexts: daggy::Children<usize, f32, u32> = dag.dag_inner.children(func_g_i);
                if let Some(max_node) = nexts.iter(&dag.dag_inner).max_by(|a, b| {
                    let fnid_a = dag.dag_inner[a.1];
                    let fnid_b = dag.dag_inner[b.1];

                    map.get(&fnid_a)
                        .unwrap()
                        .total_cmp(map.get(&fnid_b).unwrap())
                }) {
                    let fnid_max = dag.dag_inner[max_node.1];
                    let max = *map.get(&fnid_max).unwrap();

                    (*map.get_mut(&fnid).unwrap()) += max;
                }
            }

            let mut prio_order = map.into_iter().collect::<Vec<_>>();
            // Sort the vector by the value in the second element of the tuple.
            prio_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            self.dag_fn_prorities.insert(dag.dag_i, prio_order);
        }
    }

    fn select_node_for_fn(
        &self,
        schedule_to_map: &mut HashMap<FnId, NodeId>,
        // schedule_to: &mut Vec<(FnId, NodeId)>,
        cmd_distributor: &MechCmdDistributor,
        func_id: FnId,
        req: &Request,
        env: &SimEnvObserve,
    ) {
        let func = env.func(func_id);
        let nodes = env.core().nodes();

        let func_pres_id = func.parent_fns(env);
        log::info!("func {} pres {:?}", func_id, func_pres_id);

        if func_pres_id.len() == 0 {
            let mut rng = rand::thread_rng();
            let rand = rng.gen_range(0..nodes.len());
            schedule_to_map.insert(func_id, rand);
            // schedule_to.push((func_id, rand));
            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    nid: rand,
                    reqid: req.req_id,
                    fnid: func_id,
                    memlimit: None,
                }))
                .unwrap();
        } else {
            let mut min_tran_time_min_tran_node_id: Option<(f32, usize)> = None;

            for i in 0..nodes.len() {
                let get_trantime_from_prevs = || -> f32 {
                    let mut t_tran_max = 0.0;
                    // 多个前驱节点的数据传输时间，取最大
                    for &func_pre_id in &func_pres_id {
                        let func_pre = env.func(func_pre_id);
                        let node_id = *schedule_to_map.get(&func_pre_id).unwrap_or_else(|| {
                            panic!(
                                "funcpre:{:?}, func:{}, schedule: {:?}",
                                func_pre.fn_id, func_id, schedule_to_map
                            );
                        });
                        // Calculate data transmission time of edge (pre, func)
                        // 计算从上个节点到当前节点的数据传输时间，取最小
                        let t_tran: f32 =
                            func_pre.out_put_size / env.node_get_speed_btwn(node_id, i);
                        if t_tran > t_tran_max {
                            t_tran_max = t_tran;
                        }
                    }
                    t_tran_max
                };
                let trantime_from_prevs = get_trantime_from_prevs();

                if let Some(min) = min_tran_time_min_tran_node_id.as_mut() {
                    if trantime_from_prevs < min.0 {
                        *min = (trantime_from_prevs, i);
                    }
                } else {
                    min_tran_time_min_tran_node_id = Some((trantime_from_prevs, i));
                }
            }

            let nodeid = min_tran_time_min_tran_node_id
                .unwrap_or_else(|| {
                    panic!("NODES len {}", nodes.len());
                })
                .1;
            schedule_to_map.insert(func_id, nodeid);
            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    nid: nodeid,
                    reqid: req.req_id,
                    fnid: func_id,
                    memlimit: None,
                }))
                .unwrap()
        }
    }

    fn schedule_for_one_req(
        &mut self,
        req: &Request,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) {
        self.prepare_priority_for_dag(req, env);

        let dag = env.dag(req.dag_i);

        // let mut schedule_to = Vec::<(FnId, NodeId)>::new();
        let mut schedule_to_map = HashMap::<FnId, NodeId>::new();
        //实现PASS算法
        // 按照优先级降序排列函数
        // Convert the HashMap into a vector of (_, &value) pairs.

        // println!("Sorted: {:?}", prio_order);
        let prio_order = self.dag_fn_prorities.get(&dag.dag_i).unwrap();

        log::info!("prio order: {:?}", prio_order);
        for (func_id, _fun_prio) in prio_order {
            self.select_node_for_fn(&mut schedule_to_map, cmd_distributor, *func_id, req, env);
        }

        // schedule_to
        //     .into_iter()
        //     .map(|(fnid, nid)| ScheCmd {
        //         nid,
        //         reqid: req.req_id,
        //         fnid,
        //         memlimit: None,
        //     })
        //     .collect()
    }
}

impl Scheduler for PassScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        for (_, req) in env.core().requests().iter() {
            if req.fn_node.len() == 0 {
                self.schedule_for_one_req(req, env, cmd_distributor);
            }
        }
    }
}
