use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    hash::Hash,
};

use crate::{
    fn_dag::FnId,
    node::{Node, NodeId},
    request::{ReqId, Request},
    sim_env::SimEnv,
};

#[derive(Clone, Debug)]
pub struct ContainerMetric {
    // 当前容器的数量
    pub container_count: usize,
    // 已经调度到这些容器上的函数数量
    pub scheduled_fn_count: usize,
    // 等待调度到容器上的函数对应的请求
    pub ready_2_schedule_fn_reqs: Vec<ReqId>,
}

impl ContainerMetric {
    // 获取等待调度到容器上的函数对应的的请求数量
    pub fn ready_2_schedule_fn_count(&self) -> usize {
        self.ready_2_schedule_fn_reqs.len()
    }
}

impl SimEnv {
    // 获取不在fns_ready_2_schedule中的所有函数及其对应的ContainerMetric，即所有已经被调度的函数及其ContainerMetric
    pub fn algo_get_fn_all_scheduled_metric(
        &self,
        fns_ready_2_schedule: &HashMap<FnId, ContainerMetric>,
    ) -> Vec<(FnId, ContainerMetric)> {
        self.core
            .fns()
            .iter()
            .filter(|f| !fns_ready_2_schedule.contains_key(&f.fn_id))
            .map(|f| {
                (
                    f.fn_id,
                    ContainerMetric {
                        container_count: self.fn_container_cnt(f.fn_id),
                        scheduled_fn_count: self.core.fn_2_nodes().get(&f.fn_id).map_or_else(
                            || 0,
                            |nodes| {
                                nodes
                                    .iter()
                                    .map(|n| {
                                        self.node(*n).container(f.fn_id).unwrap().req_fn_state.len()
                                    })
                                    .sum()
                            },
                        ),
                        ready_2_schedule_fn_reqs: vec![],
                    },
                )
            })
            .collect()
    }
    // 收集所有前驱函数已执行完毕且尚未调度的函数以及其对应的请求
    pub fn algo_collect_req_ready_2_schedule(&self) -> BTreeMap<ReqId, VecDeque<FnId>> {
        let env = self;
        let mut collect_map: BTreeMap<ReqId, VecDeque<FnId>> = BTreeMap::new();
        // 对于已经进来的请求，scale up 已经没有前驱的fns
        // 遍历requests
        for (&reqid, req) in env.core.requests().iter() {
            let req_dag = env.dag(req.dag_i);
            let mut walker = req_dag.new_dag_walker();
            // 遍历DAG
            'outer: while let Some(f) = walker.next(&req_dag.dag_inner) {
                let fnid = req_dag.dag_inner[f];
                // 如果函数已执行完成或已调度，则跳过
                if req.done_fns.contains_key(&fnid) || req.fn_node.contains_key(&fnid) {
                    // log::info!("req {} fn {} done, no need to scale for", req.req_id, fnid,);
                    continue;
                }

                //确定前驱已完成
                // 检查其所有前驱函数是否都已完成且已调度。若有未完成或未调度的前驱函数，则跳过当前函数
                let parent_fns = env.func(fnid).parent_fns(env);
                for p in &parent_fns {
                    if req.get_fn_node(*p).is_none() || !req.done_fns.contains_key(p) {
                        // exist a parent fn not done
                        // log::info!(
                        //     "req {} fn {} parent_fn {} on node {} not done",
                        //     req.req_id,
                        //     fnid,
                        //     p,
                        //     req.get_fn_node(*p).unwrap_or(1000000)
                        // );
                        continue 'outer;
                    }
                }

                collect_map
                    .entry(reqid)
                    // 存在即添加
                    .and_modify(|q| q.push_back(fnid))
                    // 不存在则创建并添加
                    .or_insert_with(|| {
                        let mut q = VecDeque::new();
                        q.push_back(fnid);
                        q
                    });
            }
        }
        collect_map
    }
    // 获取所有准备好调度的函数（即前驱函数全部执行完毕）及其对应的ContainerMetric
    pub fn algo_collect_ready_2_schedule_metric(&self) -> HashMap<FnId, ContainerMetric> {
        let env = self;
        let mut collect_map: HashMap<FnId, ContainerMetric> = HashMap::new();
        // 对于已经进来的请求，scale up 已经没有前驱的fns
        for (_reqid, req) in env.core.requests().iter() {
            let req_dag = env.dag(req.dag_i);
            let mut walker = req_dag.new_dag_walker();
            'outer: while let Some(f) = walker.next(&req_dag.dag_inner) {
                let fnid = req_dag.dag_inner[f];
                if req.done_fns.contains_key(&fnid) {
                    // log::info!("req {} fn {} done, no need to scale for", req.req_id, fnid,);
                    continue;
                }

                //已经调度
                if req.fn_node.contains_key(&fnid) {
                    continue;
                }

                if !req.parents_all_done(env, fnid) {
                    continue;
                }

                let _metric = collect_map
                    .entry(fnid)
                    .and_modify(|metric| {
                        metric.ready_2_schedule_fn_reqs.push(req.req_id);
                    })
                    .or_insert(ContainerMetric {
                        container_count: env
                            .core
                            .fn_2_nodes()
                            .get(&fnid)
                            .map_or_else(|| 0, |nodes| nodes.len()),
                        scheduled_fn_count: env.core.fn_2_nodes().get(&fnid).map_or_else(
                            || 0,
                            |nodes| {
                                nodes
                                    .iter()
                                    .map(|n| {
                                        env.node(*n).container(fnid).unwrap().req_fn_state.len()
                                    })
                                    .sum()
                            },
                        ),
                        ready_2_schedule_fn_reqs: vec![req.req_id],
                    });
            }
        }
        collect_map
    }

    
}
