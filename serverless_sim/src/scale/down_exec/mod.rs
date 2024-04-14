use crate::{
    config::Config,
    fn_dag::FnId,
    node::{Node, NodeId},
    sim_env::SimEnv,
    SPEED_SIMILAR_THRESHOLD,
};

// 原 ScaleExecutor
pub trait ScaleDownExec: Send {
    fn exec_scale_down(&mut self, sim_env: &SimEnv, opt: ScaleOption);

    // /// return success scale up cnt
    // fn scale_up(&mut self, sim_env: &SimEnv, fnid: FnId, scale_cnt: usize) -> usize;
}

pub const SCALE_DOWN_EXEC_NAMES: [&'static str; 1] = ["default"];

pub fn new_scale_down_exec(c: &Config) -> Option<Box<dyn ScaleDownExec>> {
    let es = &c.es;
    let (scale_down_exec_name, scale_down_exec_attr) = es.scale_down_exec_conf();

    match &*scale_down_exec_name {
        "default" => {
            return Some(Box::new(DefaultScaleDownExec));
        }
        _ => {
            return None;
        }
    }
}

#[allow(dead_code)]
pub enum ScaleOption {
    /// scale cnt
    NoSpec(usize),
    /// fnid - scale cnt
    ForSpecFn(FnId, usize),
    /// nodeid - scale cnt
    ForSpecNode(NodeId, usize),
    /// nodeid - fnid
    ForSpecNodeFn(NodeId, FnId),
}

impl ScaleOption {
    fn scale_cnt(&self) -> usize {
        match self {
            ScaleOption::ForSpecFn(_, scale_cnt) => *scale_cnt,
            ScaleOption::ForSpecNode(_, scale_cnt) => *scale_cnt,
            ScaleOption::NoSpec(scale_cnt) => *scale_cnt,
            ScaleOption::ForSpecNodeFn(_, _) => {
                panic!("ScaleOption::ForSpecNodeFn can't scale_cnt")
            }
        }
    }

    pub fn new() -> Self {
        ScaleOption::NoSpec(1)
    }

    pub fn for_spec_fn(self, spec_fn: FnId) -> Self {
        let scale_cnt = self.scale_cnt();
        ScaleOption::ForSpecFn(spec_fn, scale_cnt)
    }

    #[allow(dead_code)]
    pub fn for_spec_node(self, spec_node: NodeId) -> Self {
        let scale_cnt = self.scale_cnt();
        ScaleOption::ForSpecNode(spec_node, scale_cnt)
    }

    pub fn for_spec_node_fn(self, spec_node: NodeId, spec_fn: FnId) -> Self {
        // let scale_cnt = self.scale_cnt();
        ScaleOption::ForSpecNodeFn(spec_node, spec_fn)
    }

    pub fn with_scale_cnt(self, scale_cnt: usize) -> Self {
        assert!(scale_cnt > 0);
        match self {
            ScaleOption::NoSpec(_) => ScaleOption::NoSpec(scale_cnt),
            ScaleOption::ForSpecFn(fnid, _) => ScaleOption::ForSpecFn(fnid, scale_cnt),
            ScaleOption::ForSpecNode(nodeid, _) => ScaleOption::ForSpecNode(nodeid, scale_cnt),
            ScaleOption::ForSpecNodeFn(_nodeid, _fnid) => {
                panic!("ScaleOption::ForSpecNodeFn can't with_scale_cnt");
            }
        }
    }
}

pub struct DefaultScaleDownExec;

impl DefaultScaleDownExec {
    fn collect_idle_containers(&self, env: &SimEnv) -> Vec<(NodeId, FnId)> {
        let mut idle_container_node_fn = Vec::new();

        for n in env.core.nodes().iter() {
            for (fnid, fn_ct) in n.fn_containers.borrow().iter() {
                if fn_ct.is_idle() {
                    idle_container_node_fn.push((n.node_id(), *fnid));
                }
            }
        }

        idle_container_node_fn
    }

    fn scale_down_no_spec(&mut self, env: &SimEnv, mut scale_cnt: usize) {
        let collect_idle_containers = self.collect_idle_containers(env);
        if collect_idle_containers.len() < scale_cnt {
            log::warn!(
                "scale down has failed partly, target:{scale_cnt}, actual:{}",
                collect_idle_containers.len()
            );
            scale_cnt = collect_idle_containers.len();
        }

        for (nodeid, fnid) in collect_idle_containers[0..scale_cnt].iter() {
            env.set_scale_down_result(*fnid, *nodeid);
        }
    }

    fn scale_down_for_fn(&mut self, env: &SimEnv, fnid: FnId, mut scale_cnt: usize) {
        let mut collect_idle_containers = self.collect_idle_containers(env);
        collect_idle_containers.retain(|&(_nodeid, fnid_)| fnid_ == fnid);

        if collect_idle_containers.len() < scale_cnt {
            // log::warn!(
            //     "scale down for spec fn {fnid} has failed partly, target:{scale_cnt}, actual:{}",
            //     collect_idle_containers.len()
            // );
            scale_cnt = collect_idle_containers.len();
        }
        for (nodeid, fnid) in collect_idle_containers[0..scale_cnt].iter() {
            env.set_scale_down_result(*fnid, *nodeid);
        }
    }

    fn scale_down_for_node(&mut self, env: &SimEnv, nodeid: NodeId, mut scale_cnt: usize) {
        let mut collect_idle_containers = self.collect_idle_containers(env);
        collect_idle_containers.retain(|&(nodeid_, _fnid)| nodeid_ == nodeid);

        if collect_idle_containers.len() < scale_cnt {
            // log::warn!(
            //     "scale down for spec node {nodeid} has failed partly, target:{scale_cnt}, actual:{}",
            //     collect_idle_containers.len()
            // );
            scale_cnt = collect_idle_containers.len();
        }
        for (nodeid, fnid) in collect_idle_containers[0..scale_cnt].iter() {
            env.set_scale_down_result(*fnid, *nodeid);
        }
    }
}

impl ScaleDownExec for DefaultScaleDownExec {
    fn exec_scale_down(&mut self, env: &SimEnv, opt: ScaleOption) {
        match opt {
            ScaleOption::NoSpec(scale_cnt) => {
                self.scale_down_no_spec(env, scale_cnt);
            }
            ScaleOption::ForSpecFn(fnid, scale_cnt) => {
                self.scale_down_for_fn(env, fnid, scale_cnt);
            }
            ScaleOption::ForSpecNode(nodeid, scale_cnt) => {
                self.scale_down_for_node(env, nodeid, scale_cnt);
            }
            ScaleOption::ForSpecNodeFn(nodeid, fnid) => env.set_scale_down_result(fnid, nodeid),
        }
    }
}
