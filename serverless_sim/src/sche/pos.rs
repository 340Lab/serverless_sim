use std::collections::{HashMap, HashSet};

use crate::{
    fn_dag::FnId,
    mechanism::{DownCmd, ScheCmd, UpCmd},
    node::NodeId,
    request::{ReqId, Request},
    sim_env::SimEnv,
    sim_run::{schedule_helper, Scheduler},
};

pub struct PosScheduler {
    new_scale_up_nodes: HashMap<FnId, HashSet<NodeId>>,
    schealeable_fns: HashSet<FnId>,
    // node_new_task_cnt: HashMap<NodeId, usize>,
}

impl PosScheduler {
    pub fn new() -> Self {
        Self {
            // node_new_task_cnt: HashMap::new(),
            new_scale_up_nodes: HashMap::new(),
            // recent_schedule_node: vec![],
            schealeable_fns: HashSet::new(),
        }
    }
}

impl PosScheduler {
    fn collect_scheable_fns_for_req(&mut self, env: &SimEnv, req: &Request) {
        let dag_i = req.dag_i;
        let mut dag_walker = env.dag(dag_i).new_dag_walker();
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
                self.schealeable_fns.insert(fnid);
            }
        }
    }
    fn record_new_scale_up_node(&mut self, fnid: FnId, node_id: NodeId) {
        if !self.new_scale_up_nodes.contains_key(&fnid) {
            self.new_scale_up_nodes.insert(fnid, HashSet::new());
        }
        self.new_scale_up_nodes
            .get_mut(&fnid)
            .unwrap()
            .insert(node_id);
    }
    fn new_scale_up_nodes(&self, fnid: FnId) -> HashSet<NodeId> {
        self.new_scale_up_nodes
            .get(&fnid)
            .cloned()
            .unwrap_or_default()
    }
    // fn node_new_task_cnt(&self, node_id: NodeId) -> usize {
    //     self.node_new_task_cnt.get(&node_id).cloned().unwrap_or(0)
    // }
    fn schedule_one_req_fns(
        &mut self,
        env: &SimEnv,
        req: &mut Request,
    ) -> (Vec<UpCmd>, Vec<DownCmd>, Vec<ScheCmd>) {
        let mut schedule_able_fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::PreAllSched,
        );

        // let mut node2node_connection_count = env.core.node2node_connection_count().clone();
        schedule_able_fns.sort_by(|&a, &b| {
            env.func(a)
                .cpu
                .partial_cmp(&env.func(b).cpu)
                .unwrap()
                .reverse()
        });

        let scale_up_exec = env.new_mech.scale_up_exec();
        let mech_metric = || env.help.mech_metric_mut();

        let mut sche_cmds = vec![];
        // let scale_up_cmds = vec![];
        let mut scale_up_cmds = vec![];

        for &fnid in &schedule_able_fns {
            // 容器预加载下沉到 schedule阶段， scaler阶段只进行容器数的确定
            let mut target_cnt = env.new_mech.scale_num(fnid);
            if target_cnt == 0 {
                target_cnt = 1;
            }

            let fn_scale_up_cmds = scale_up_exec.exec_scale_up(target_cnt, fnid, env);
            for cmd in fn_scale_up_cmds.iter() {
                self.record_new_scale_up_node(cmd.fnid, cmd.nid);
            }
            scale_up_cmds.extend(fn_scale_up_cmds);

            // 选择节点算法，首先选出包含当前函数容器的节点
            let mut nodes2select: Vec<NodeId> = env
                .nodes()
                .iter()
                .filter(|n| n.container(fnid).is_some())
                .map(|n| n.node_id())
                .collect();
            let nodes_with_container_cnt = nodes2select.len();
            nodes2select.extend(self.new_scale_up_nodes(fnid));

            // let nodes = env.fn_running_containers_nodes(fnid);
            // if nodes.len() == 0 {
            // assert_eq!(env.fn_2_nodes.borrow().get(&fnid).unwrap().len(), 1);
            // let fn_2_nodes = env.fn_2_nodes.borrow();
            // let nodes = fn_2_nodes.get(&fnid).unwrap();

            // let mut on_node_time = HashMap::new();
            // let mut get_on_node_time = |node_id: NodeId| {
            //     if !on_node_time.contains_key(&node_id) {
            //         on_node_time.insert(
            //             node_id,
            //             env.algo_predict_fn_on_node_work_time(req, fnid, node_id, None),
            //         );
            //     }
            //     *on_node_time.get(&node_id).unwrap()
            // };

            // !! not used
            // let each_node_time = nodes2select
            //     .iter()
            //     .enumerate()
            //     .map(|(idx, &n): (usize, &usize)| {
            //         (
            //             n,
            //             if idx < nodes_with_container_cnt {
            //                 // node with container
            //                 env.node(n).rsc_limit.cpu / env.node(n).all_task_cnt() as f32
            //             } else {
            //                 // node without container
            //                 env.node(n).rsc_limit.cpu / env.node(n).all_task_cnt() as f32
            //                     + env.func(fnid).cold_start_time as f32
            //             },
            //         )
            //     })
            //     .collect::<Vec<_>>();

            let nodes_task_cnt = nodes2select
                .iter()
                .map(|n| (mech_metric().node_task_new_cnt(*n) as f32))
                .collect::<Vec<_>>();
            let fparents = env.func(fnid).parent_fns(env);
            let nodes_parent_distance = nodes2select
                .iter()
                .map(|n| {
                    if fparents.len() == 0 {
                        return 0.0;
                    }
                    fparents
                        .iter()
                        .map(|&p| {
                            if req.get_fn_node(p).unwrap() == *n {
                                0.0
                            } else {
                                1.0
                            }
                        })
                        .sum::<f32>()
                        / fparents.len() as f32
                })
                .collect::<Vec<_>>();

            let score_of_idx = |idx: usize| {
                let task_cnt = nodes_task_cnt[idx];
                let parent_distance = nodes_parent_distance[idx];
                let score = 1.0 / (task_cnt + 1.0) - parent_distance;
                score
            };

            let best_node = *nodes2select
                .iter()
                .enumerate()
                .max_by(|(idx1, n1), (idx2, n2)| {
                    let score1 = score_of_idx(*idx1);
                    let score2 = score_of_idx(*idx2);
                    score1.partial_cmp(&score2).unwrap()
                })
                .unwrap()
                .1;
            // .min_by(|&&a, &&b| {
            //     // let atime = get_on_node_time(a);
            //     // let btime = get_on_node_time(b);
            //     env.node(a)
            //         .all_task_cnt()
            //         .partial_cmp(&env.node(b).all_task_cnt())
            //         .unwrap()
            //     // atime.partial_cmp(&btime).unwrap()
            //     // a.total_cmp(&b)
            // })
            // .unwrap();

            // env.schedule_reqfn_on_node(req, fnid, best_node);
            mech_metric().add_node_task_new_cnt(best_node);
            sche_cmds.push(ScheCmd {
                reqid: req.req_id,
                fnid,
                nid: best_node,
                memlimit: None,
            })
        }
        (scale_up_cmds, vec![], sche_cmds)
    }
}

impl Scheduler for PosScheduler {
    fn schedule_some(&mut self, env: &SimEnv) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        self.new_scale_up_nodes.clear();
        self.schealeable_fns.clear();
        for (_req_id, req) in env.core.requests().iter() {
            self.collect_scheable_fns_for_req(env, req);
        }
        // log::info!("try put fn");
        // let nodes_taskcnt = env
        //     .nodes()
        //     .iter()
        //     .map(|n| (n.node_id(), n.task_cnt()))
        //     .collect::<HashMap<NodeId, usize>>();
        let mut up_cmds = vec![];
        let mut sche_cmds = vec![];
        let mut down_cmds = vec![];

        // 遍历每个函数，看是否需要缩容
        for func in env.core.fns().iter() {
            let target = env.new_mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target < cur {
                down_cmds.extend(env.new_mech.scale_down_exec().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                ));
            }
        }

        for (_req_id, req) in env.core.requests_mut().iter_mut() {
            let (sub_up, sub_down, sub_sche) = self.schedule_one_req_fns(env, req);
            up_cmds.extend(sub_up);
            down_cmds.extend(sub_down);
            sche_cmds.extend(sub_sche);
        }

        (up_cmds, sche_cmds, down_cmds)
    }
}
