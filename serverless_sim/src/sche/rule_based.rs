use std::collections::BTreeMap;

use rand::Rng;

use crate::{
    fn_dag::FnId,
    mechanism::{DownCmd, ScheCmd, UpCmd},
    node::NodeId,
    request::Request,
    sim_env::SimEnv,
    sim_run::Scheduler,
};

pub enum ScheduleRule {
    // last node
    RoundRobin(usize),
    Random,
    LeastLoad,
    GOFS,
}

pub struct RuleBasedScheduler {
    pub rule: ScheduleRule,
}

impl Scheduler for RuleBasedScheduler {
    fn schedule_some(
        &mut self,
        env: &crate::sim_env::SimEnv,
    ) -> (Vec<UpCmd>, Vec<ScheCmd>, Vec<DownCmd>) {
        for (_req_id, req) in env.core.requests_mut().iter_mut() {
            env.schedule_one_req_fns_by_rule(req, &mut self.rule);
        }
    }
}

impl SimEnv {
    fn schedule_one_req_fns_by_rule(&self, req: &mut Request, rule: &mut ScheduleRule) {
        // let dag_i = req.dag_i;
        // let mut dag_walker = self.dag(dag_i).new_dag_walker();
        // let mut schedule_able_fns = vec![];
        // 'next_fn: while let Some(fngi) = dag_walker.next(&*self.dag_inner(dag_i)) {
        //     let fnid = self.dag_inner(dag_i)[fngi];
        //     if req.fn_node.contains_key(&fnid) {
        //         //scheduled
        //         continue;
        //     }
        //     let parents = self.func(fnid).parent_fns(self);
        //     for p in &parents {
        //         if !req.done_fns.contains(p) {
        //             continue 'next_fn;
        //         }
        //     }
        //     if self.fn_2_nodes.borrow().contains_key(&fnid)
        //         && self.fn_running_containers_nodes(fnid).len() > 0
        //     {
        //         // parents all done schedule able
        //         schedule_able_fns.push(fnid);
        //     }
        // }
        // for &fnid in &schedule_able_fns {
        //     match rule {
        //         ScheduleRule::RoundRobin(last_node) => {
        //             self.schedule_fn_round_robin(req, fnid, last_node);
        //         }
        //         ScheduleRule::Random => {
        //             self.schedule_fn_random(req, fnid);
        //         }
        //         ScheduleRule::LeastLoad => {
        //             self.schedule_fn_score(req, fnid, rule);
        //         }
        //         ScheduleRule::GOFS => {
        //             self.schedule_fn_gofs(req, fnid);
        //         }
        //     }
        // }
    }
    // fn schedule_fn_gofs(&self, req: &mut Request, fnid: FnId) {
    //     //找出剩余量最大的一批节点，然后选最快的XQ
    //     let mut left_space_2_nodes: BTreeMap<usize, Vec<NodeId>> = BTreeMap::new();
    //     let nodes = self.fn_running_containers_nodes(fnid);

    //     for &n in nodes.iter() {
    //         let left_space = (self.node(n).left_mem() / self.func(fnid).mem) as usize;
    //         left_space_2_nodes
    //             .entry(left_space)
    //             .and_modify(|nodes| nodes.push(n))
    //             .or_insert(vec![n]);
    //     }
    //     let take = *left_space_2_nodes.iter().next().unwrap().0;
    //     let most_space_nodes = left_space_2_nodes.remove(&take).unwrap();
    //     let most_space_nodes_transtime = most_space_nodes
    //         .iter()
    //         .map(|&n| {
    //             (
    //                 n,
    //                 self.algo_predict_fn_on_node_work_time(req, fnid, n, None),
    //             )
    //         })
    //         .collect::<Vec<_>>();
    //     let res = most_space_nodes_transtime
    //         .iter()
    //         .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
    //         .unwrap();
    //     self.schedule_reqfn_on_node(req, fnid, res.0);
    // }
    // fn schedule_fn_round_robin(&self, req: &mut Request, fnid: FnId, last_node: &mut NodeId) {
    //     let nodes = self.fn_running_containers_nodes(fnid);

    //     let next_node = *last_node;
    //     for n in 0..self.node_cnt() {
    //         if nodes.contains(&n) {
    //             if n > next_node {
    //                 self.schedule_reqfn_on_node(req, fnid, n);
    //                 *last_node = n;
    //                 return;
    //             }
    //         }
    //     }
    //     for n in 0..self.node_cnt() {
    //         if nodes.contains(&n) {
    //             self.schedule_reqfn_on_node(req, fnid, n);
    //             *last_node = n;
    //             return;
    //         }
    //     }
    // }

    // fn schedule_fn_random(&self, req: &mut Request, fnid: FnId) {
    //     let nodes = self.fn_running_containers_nodes(fnid);

    //     let mut nodes = nodes.iter().map(|v| *v).collect::<Vec<_>>();
    //     nodes.sort();
    //     let mut rng = rand::thread_rng();
    //     let n = nodes[rng.gen_range(0..nodes.len())];
    //     self.schedule_reqfn_on_node(req, fnid, n);
    // }

    // fn schedule_fn_score(&self, req: &mut Request, fnid: FnId, rule: &ScheduleRule) {
    //     let nodes = self.fn_running_containers_nodes(fnid);

    //     let mut best_node = None;
    //     for &n in nodes.iter() {
    //         let score = match rule {
    //             ScheduleRule::RoundRobin(_) => panic!("not score rule"),
    //             ScheduleRule::Random => panic!("not score rule"),
    //             ScheduleRule::GOFS => panic!("not score rule"),
    //             ScheduleRule::LeastLoad => self.node(n).running_task_cnt() as f32,
    //         };

    //         if let Some((best_n, besttime)) = best_node.clone() {
    //             if score > besttime {
    //                 best_node = Some((n, score));
    //             }
    //         } else {
    //             best_node = Some((n, score));
    //         }
    //     }
    //     let (node_to_run_req_fn, _score) = best_node.unwrap();
    //     self.schedule_reqfn_on_node(req, fnid, node_to_run_req_fn);
    // }
}
