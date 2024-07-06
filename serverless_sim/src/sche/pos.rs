use rand::seq::SliceRandom;
use rand::{ thread_rng, Rng };

use crate::fn_dag::EnvFnExt;
use crate::mechanism_thread::{ MechCmdDistributor, MechScheduleOnceRes };
use crate::node::EnvNodeExt;
use crate::request::ReqId;
use crate::util;
use crate::with_env_sub::{ WithEnvCore, WithEnvHelp };
use crate::{
    fn_dag::FnId,
    mechanism::{ MechanismImpl, ScheCmd, SimEnvObserve },
    node::NodeId,
    request::Request,
    sim_run::{ schedule_helper, Scheduler },
};
use std::cell::RefCell;
use std::collections::{ HashMap, HashSet, VecDeque };

enum PosMode {
    Greedy,
    Random,
    Auto,
}

pub struct PosScheduler {
    // new_scale_up_nodes: HashMap<FnId, HashSet<NodeId>>,
    schealeable_fns: RefCell<HashMap<FnId, HashSet<NodeId>>>,
    sche_queue: Vec<(ReqId, Vec<FnId>)>,
    // node_new_task_cnt: HashMap<NodeId, usize>,
    mode: PosMode,
}

impl PosScheduler {
    pub fn new(arg: &str) -> Self {
        Self {
            // node_new_task_cnt: HashMap::new(),
            // new_scale_up_nodes: HashMap::new(),
            // recent_schedule_node: vec![],
            schealeable_fns: HashMap::new().into(),
            sche_queue: {
                let mut v = Vec::new();
                v.reserve(1024);
                v
            },
            mode: match arg {
                "greedy" => { PosMode::Greedy }
                "random" => { PosMode::Random }
                _ => { panic!("pos arg can only be 1 of: greedy, random") }
                // "auto"
            },
        }
    }
}

impl PosScheduler {
    fn collect_scheable_fns_for_req(&mut self, env: &SimEnvObserve, req: &Request) {
        let dag_i = req.dag_i;
        let mut dag_walker = env.dag(dag_i).new_dag_walker();

        let mut schefns = vec![];

        // let mut schedule_able_fns = vec![];
        'next_fn: while let Some(fngi) = dag_walker.next(&*env.dag_inner(dag_i)) {
            let fnid = env.dag_inner(dag_i)[fngi];
            if req.fn_node.contains_key(&fnid) {
                //scheduled
                continue;
            }
            let parents = env.func(fnid).parent_fns(env);
            for p in &parents {
                // parent has't been scheduled
                if !req.fn_node.contains_key(p) {
                    continue 'next_fn;
                }
            }
            // if
            //     env.fn_2_nodes.borrow().contains_key(&fnid) &&
            //     env.fn_running_containers_nodes(fnid).len() > 0
            {
                // parents all done schedule able
                // schedule_able_fns.push(fnid);
                schefns.push(fnid);
                if self.schealeable_fns.borrow().contains_key(&fnid) {
                    continue;
                }
                self.schealeable_fns.borrow_mut().insert(
                    fnid,
                    env
                        .core()
                        .fn_2_nodes()
                        .get(&fnid)
                        .map(|v| { v.clone() })
                        .unwrap_or(HashSet::new())
                );
            }
        }
        self.sche_queue.push((req.req_id, schefns));
    }
    fn record_new_scale_up_node(&self, fnid: FnId, node_id: NodeId) {
        self.schealeable_fns.borrow_mut().get_mut(&fnid).unwrap().insert(node_id);
    }
    fn new_scale_up_nodes(&self, fnid: FnId) -> HashSet<NodeId> {
        self.schealeable_fns.borrow().get(&fnid).cloned().unwrap_or_default()
    }
    // fn node_new_task_cnt(&self, node_id: NodeId) -> usize {
    //     self.node_new_task_cnt.get(&node_id).cloned().unwrap_or(0)
    // }
    fn schedule_one_req_fns(
        &self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        req: &(ReqId, Vec<FnId>),
        cmd_distributor: &MechCmdDistributor
    ) {
        //     let mut schedule_able_fns = schedule_helper::collect_task_to_sche(
        //         req,
        //         env,
        //         schedule_helper::CollectTaskConfig::PreAllSched
        //     );

        // let mut node2node_connection_count = env.core.node2node_connection_count().clone();
        // schedule_able_fns.sort_by(|&a, &b| {
        //     env.func(a).cpu.partial_cmp(&env.func(b).cpu).unwrap().reverse()
        // });

        log::info!(
            "schedule_some sort fns cost {}",
            // req.req_id,
            util::now_ms() - *mech.step_begin.borrow()
        );

        let scale_up_exec = mech.scale_up_exec();
        let mech_metric = || env.help().mech_metric_mut();

        for &fnid in req.1.iter() {
            // 容器预加载下沉到 schedule阶段， scaler阶段只进行容器数的确定
            let mut target_cnt = mech.scale_num(fnid);
            if target_cnt == 0 {
                target_cnt = 1;
            }

            let fn_scale_up_cmds = scale_up_exec.exec_scale_up(
                target_cnt,
                fnid,
                env,
                cmd_distributor
            );
            for cmd in fn_scale_up_cmds.iter() {
                self.record_new_scale_up_node(cmd.fnid, cmd.nid);
            }
            log::info!(
                "schedule_some schduling fn {} {}",
                fnid,
                // req.req_id,
                util::now_ms() - *mech.step_begin.borrow()
            );

            // 选择节点算法，首先选出包含当前函数容器的节点
            let mut nodes2select = self.new_scale_up_nodes(fnid);

            // random
            let best_node = match &self.mode {
                PosMode::Random => {
                    let i = thread_rng().gen_range(0..nodes2select.len());
                    let best_node = *nodes2select.iter().nth(i).unwrap();
                    best_node
                }
                PosMode::Greedy => {
                    {
                        //greedy
                        let nodes_task_cnt = nodes2select
                            .iter()
                            .map(|n| mech_metric().node_task_new_cnt(*n) as f32)
                            .collect::<Vec<_>>();
                        // let fparents = env.func(fnid).parent_fns(env);
                        // let nodes_parent_distance = nodes2select
                        //     .iter()
                        //     .map(|n| {
                        //         if fparents.len() == 0 {
                        //             return 0.0;
                        //         }
                        //         fparents
                        //             .iter()
                        //             .map(|&p| {
                        //                 if req.get_fn_node(p).unwrap() == *n { 0.0 } else { 1.0 }
                        //             })
                        //             .sum::<f32>() / (fparents.len() as f32)
                        //     })
                        //     .collect::<Vec<_>>();

                        let score_of_idx = |idx: usize| {
                            let task_cnt = nodes_task_cnt[idx];
                            // let parent_distance = nodes_parent_distance[idx];
                            let score = 1.0 / (task_cnt + 1.0);
                            // let score = 1.0 / (task_cnt + 1.0) - parent_distance;
                            score
                        };

                        let best_node = *nodes2select
                            .iter()
                            .enumerate()
                            .max_by(|(idx1, _n1), (idx2, _n2)| {
                                let score1 = score_of_idx(*idx1);
                                let score2 = score_of_idx(*idx2);
                                score1.partial_cmp(&score2).unwrap()
                            })
                            .unwrap().1;
                        best_node
                    }
                }
                PosMode::Auto => {
                    todo!();
                }
            };

            // let best_node = { *nodes2select.choose(&mut thread_rng()).unwrap() };

            // env.schedule_reqfn_on_node(req, fnid, best_node);
            mech_metric().add_node_task_new_cnt(best_node);
            cmd_distributor
                .send(
                    MechScheduleOnceRes::ScheCmd(ScheCmd {
                        reqid: req.0,
                        fnid,
                        nid: best_node,
                        memlimit: None,
                    })
                )
                .unwrap();
        }
    }
}

impl Scheduler for PosScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor
    ) {
        // self.new_scale_up_nodes.clear();
        self.sche_queue.clear();
        self.schealeable_fns.borrow_mut().clear();
        for (_req_id, req) in env.core().requests().iter() {
            self.collect_scheable_fns_for_req(env, req);
        }
        log::info!(
            "schedule_some collect_scheable_fns_for_req cost {}",
            util::now_ms() - *mech.step_begin.borrow()
        );
        // log::info!("try put fn");
        // let nodes_taskcnt = env
        //     .nodes()
        //     .iter()
        //     .map(|n| (n.node_id(), n.task_cnt()))
        //     .collect::<HashMap<NodeId, usize>>();

        // 遍历每个函数，看是否需要缩容
        for func in env.core().fns().iter() {
            let target = mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target < cur {
                mech.scale_down_exec().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                    cmd_distributor
                );
                log::info!(
                    "schedule_some scale_down_exec {} cost {}",
                    func.fn_id,
                    util::now_ms() - *mech.step_begin.borrow()
                );
            }
        }

        for r in &self.sche_queue {
            self.schedule_one_req_fns(env, mech, r, cmd_distributor);
            log::info!(
                "schedule_some schedule_one_req_fns {} cost {}",
                r.0,
                util::now_ms() - *mech.step_begin.borrow()
            );
        }
    }
}
