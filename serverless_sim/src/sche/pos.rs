use std::collections::{HashMap, HashSet};

use crate::{
    fn_dag::FnId,
    node::NodeId,
    request::{ReqId, Request},
    sim_env::SimEnv,
    sim_run::{schedule_helper, Scheduler},
};

pub struct PosScheduler {
    // dag_fn_prorities_: HashMap<DagId, HashMap<FnId, f32>>,

    //
    // recent_schedule_node: Vec<NodeId>,
    schealeable_fns: HashSet<FnId>,
}

impl PosScheduler {
    pub fn new() -> Self {
        Self {
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

    fn schedule_one_req_fns(&self, env: &SimEnv, req: &mut Request) {
        let mut schedule_able_fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::PreAllSched,
        );

        let mut node2node_connection_count = env.core.node2node_connection_count().clone();
        schedule_able_fns.sort_by(|&a, &b| {
            env.func(a)
                .cpu
                .partial_cmp(&env.func(b).cpu)
                .unwrap()
                .reverse()
        });
        for &fnid in &schedule_able_fns {
            // 可以调度的节点数 ???
            // 可以调度的容器数量 ???
            let mut scheable_node_count = env.spec_scaler().fn_available_count(fnid, env);
            if scheable_node_count == 0 {
                log::warn!("scaler should ask scheduler for requirement and prepare enough nodes");
                continue;
            }
            // if scheable_node_count == 0 {
            //     scheable_node_count = 1;
            // }

            // 容器预加载下沉到 schedule阶段， scaler阶段只进行容器数的确定
            let target_cnt = env.spec_scaler().fn_available_count(fnid, env);
            env.mechanisms.scale_up_exec_mut()
                .exec_scale_up(target_cnt, fnid, env);

            // 选择节点算法，首先选出包含当前函数容器的节点
            let nodes_with_container: Vec<NodeId> = env
                .nodes()
                .iter()
                .filter(|n| n.container(fnid).is_some())
                .map(|n| n.node_id())
                .collect();

            let nodes_with_container_cnt = nodes_with_container.len();

            let nodes2select =
                &nodes_with_container[0..env.spec_scaler().fn_available_count(fnid, env)];

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
            let each_node_time = nodes2select
                .iter()
                .enumerate()
                .map(|(idx, &n): (usize, &usize)| {
                    (
                        n,
                        if idx < nodes_with_container_cnt {
                            // node with container
                            env.node(n).rsc_limit.cpu / env.node(n).all_task_cnt() as f32
                                + env.func(fnid).cold_start_time as f32
                        } else {
                            // node without container
                            env.node(n).rsc_limit.cpu / env.node(n).all_task_cnt() as f32
                        },
                    )
                })
                .collect::<Vec<_>>();

            let nodes_task_cnt = nodes2select
                .iter()
                .map(|n| env.node(*n).all_task_cnt() as f32)
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

            if env.func(fnid).parent_fns(env).len() > 0 {}
            // for &n in nodes.iter() {
            //     let time = env.node(n).running_task_cnt() as f32;
            //     if let Some((best_n, besttime)) = best_node.take() {
            //         if time < besttime {
            //             best_node = Some((n, time));
            //         } else {
            //             best_node = Some((best_n, besttime));
            //         }
            //     } else {
            //         best_node = Some((n, time));
            //     }
            // }
            env.schedule_reqfn_on_node(req, fnid, best_node);
            // continue;
            // }

            // let mut best_node = None;
            // if env.func(fnid).parent_fns(env).len() == 0 {
            //     for &n in nodes.iter() {
            //         let time = env.node(n).task_cnt() as f32;
            //         if let Some((best_n, besttime)) = best_node.take() {
            //             if time < besttime {
            //                 best_node = Some((n, time));
            //             } else {
            //                 best_node = Some((best_n, besttime));
            //             }
            //         } else {
            //             best_node = Some((n, time));
            //         }
            //     }
            // } else {
            //     for &n in nodes.iter() {
            //         let time = env.algo_predict_fn_on_node_work_time(
            //             req,
            //             fnid,
            //             n,
            //             (&node2node_connection_count).into(),
            //         );
            //         if let Some((best_n, besttime)) = best_node.take() {
            //             if time < besttime {
            //                 best_node = Some((n, time));
            //             } else {
            //                 best_node = Some((best_n, besttime));
            //             }
            //         } else {
            //             best_node = Some((n, time));
            //         }
            //     }
            // }

            // let (node_to_run_req_fn, run_time) = best_node.unwrap();
            // env.schedule_reqfn_on_node(req, fnid, node_to_run_req_fn);
            // update connection count map
            // {
            //     let parents = env.func(fnid).parent_fns(env);
            //     for &p in &parents {
            //         let pnode = *req.fn_node.get(&p).unwrap();
            //         if pnode == best_node {
            //         } else {
            //             let connection_count = env
            //                 .node_get_connection_count_between_by_offerd_graph(
            //                     pnode,
            //                     best_node,
            //                     &node2node_connection_count,
            //                 );
            //             env.node_set_connection_count_between_by_offerd_graph(
            //                 pnode,
            //                 best_node,
            //                 connection_count + 1,
            //                 &mut node2node_connection_count,
            //             );
            //         }
            //     }
            // }
            // if do_prewarm_check {
            // // 有一个请求函数（预测所有前驱函数结束时间点，计数）表
            // // 在调度一个函数时，预估该函数的结束时间，对于后继节点，
            // // 其冷启动时间点应该为前驱结束时间点-冷启动时间，
            // // 若当前预测结束时间点大于预测启动表中时间，更新该时间，
            // // 同时计数+1，直到计数等于该后继函数的所有前驱节点数时，
            // // 代表预计时间以及确定，从表中移除该项，并注册定时器，
            // // 时间为（预测所有前驱函数结束时间点-观测到的冷启动时间），
            // // 在对应时间判断容器数是否为0，若为0，则进行预热。
            // let mut children = env.dag_inner(dag_i).children(env.func(fnid).graph_i);
            // // update children predict
            // while let Some((e, child_n)) = children.walk_next(&env.dag_inner(dag_i)) {
            //     let child_fnid = env.dag_inner(dag_i)[child_n];
            //     let cunrrent_fn_done_time = (env.current_frame() as f32) + run_time;
            //     let mut prev_done_time_all_collected = false;
            //     req.fn_predict_prevs_done_time
            //         .entry(child_fnid)
            //         .and_modify(|(time, cnt, total)| {
            //             if cunrrent_fn_done_time > *time {
            //                 *time = cunrrent_fn_done_time;
            //             }
            //             *cnt += 1;
            //             if *cnt == *total {
            //                 prev_done_time_all_collected = true;
            //             }
            //         })
            //         .or_insert_with(|| {
            //             (cunrrent_fn_done_time, 1, env.func(child_fnid).parent_fns(self).len())
            //         });
            //     if prev_done_time_all_collected {
            //         let (time, _, _) = req.fn_predict_prevs_done_time
            //             .remove(&child_fnid)
            //             .unwrap();
            //         let time =
            //             (time as isize) + 1 - (env.func(child_fnid).cold_start_time as isize);
            //         if time > 0 {
            //             let req_id = req.req_id;
            //             env.start_timer(time as usize, move |env: &SimEnv| {
            //                 let requests = env.real_time.requests();
            //                 if let Some(req) = requests.get(&req_id) {
            //                     // 子函数未调度
            //                     if !req.fn_node.contains_key(&child_fnid) {
            //                         if env.fn_container_cnt(child_fnid) == 0 {
            //                             env.scale_executor
            //                                 .borrow_mut()
            //                                 .scale_up(env, child_fnid, 1);
            //                         }
            //                     }
            //                 }
            //             });
            //         }
            //     }
            // }
            // }
        }
    }
}

impl Scheduler for PosScheduler {
    fn schedule_some(&mut self, env: &SimEnv) {
        // log::info!("try put fn");
        // let nodes_taskcnt = env
        //     .nodes()
        //     .iter()
        //     .map(|n| (n.node_id(), n.task_cnt()))
        //     .collect::<HashMap<NodeId, usize>>();
        for (_req_id, req) in env.core.requests_mut().iter_mut() {
            self.schedule_one_req_fns(env, req);
        }
    }

    fn prepare_this_turn_will_schedule(&mut self, env: &SimEnv) {
        self.schealeable_fns.clear();
        for (_req_id, req) in env.core.requests().iter() {
            self.collect_scheable_fns_for_req(env, req);
        }
    }
    fn this_turn_will_schedule(&self, fnid: FnId) -> bool {
        self.schealeable_fns.contains(&fnid)
    }
}
