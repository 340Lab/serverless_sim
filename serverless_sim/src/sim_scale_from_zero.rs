use clap::ValueEnum;
use enum_dispatch::enum_dispatch;

use crate::{
    sim_env::{self, SimEnv},
    sim_scale_executor::{ScaleExecutor, ScaleOption},
};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ScaleFromZeroType {
    LazyScaleFromZero,
    DirectlyScaleFromZero,
}

#[enum_dispatch]
pub trait ScaleFromZero {
    fn scale_some(&mut self, env: &SimEnv);
}

#[enum_dispatch(ScaleFromZero)]
pub enum ScaleFromZeroImpl {
    LazyScaleFromZero(LazyScaleFromZero),
    DirectlyScaleFromZero(DirectlyScaleFromZero),
}

// 发现有请求进来，但是没有未完成的前驱的fns，且函数容器数量为0，就scale up
pub struct LazyScaleFromZero;

impl ScaleFromZero for LazyScaleFromZero {
    fn scale_some(&mut self, env: &SimEnv) {
        // 对于已经进来的请求，scale up 已经没有前驱的fns
        for (_reqid, req) in env.requests.borrow().iter() {
            let req_dag = env.dag(req.dag_i);
            let mut walker = req_dag.new_dag_walker();
            'outer: while let Some(f) = walker.next(&req_dag.dag) {
                let fnid = req_dag.dag[f];
                if req.done_fns.contains(&fnid) {
                    log::info!("req {} fn {} done, no need to scale for", req.req_id, fnid,);
                    continue;
                }
                if let Some(nodes) = env.fn_2_nodes.borrow().get(&fnid) {
                    if nodes.len() > 0 {
                        log::info!(
                            "req {} fn {} has container, no need to scale for",
                            req.req_id,
                            fnid,
                        );
                        continue;
                    }
                }

                let parent_fns = env.func(fnid).parent_fns(env);
                for p in &parent_fns {
                    if req.get_fn_node(*p).is_none() || !req.done_fns.contains(p) {
                        // exist a parent fn not done
                        log::info!(
                            "req {} fn {} parent_fn {} on node {} not done",
                            req.req_id,
                            fnid,
                            p,
                            req.get_fn_node(*p).unwrap_or(1000000)
                        );
                        continue 'outer;
                    }
                }

                // scale up
                if env.scale_executor.borrow_mut().scale_up(env, fnid, 1) == 0 {
                    // panic!("scale up failed");
                    // just ensure there is at least one fn container
                    env.scale_executor
                        .borrow_mut()
                        .scale_down(env, ScaleOption::new().with_scale_cnt(1));
                    assert_eq!(env.scale_executor.borrow_mut().scale_up(env, fnid, 1), 1);
                }
                log::info!("scale up for req {} fn {fnid}", req.req_id);
            }
        }
    }
}

// 发现有请求进来，且函数容器数量为0，就立刻全都scale up
pub struct DirectlyScaleFromZero;

impl ScaleFromZero for DirectlyScaleFromZero {
    fn scale_some(&mut self, env: &SimEnv) {
        // 对于已经进来的请求，scale up 已经没有前驱的fns
        for (_reqid, req) in env.requests.borrow().iter() {
            let req_dag = env.dag(req.dag_i);
            let mut walker = req_dag.new_dag_walker();
            while let Some(f) = walker.next(&req_dag.dag) {
                let fnid = req_dag.dag[f];
                if !req.done_fns.contains(&fnid) && env.fn_2_nodes.borrow().get(&fnid).is_none() {
                    // scale up
                    env.scale_executor.borrow_mut().scale_up(env, fnid, 1);
                }
            }
        }
    }
}
