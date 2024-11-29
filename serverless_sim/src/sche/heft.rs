use std::{cmp::Ordering, collections::HashMap};

use daggy::Walker;
use rand::Rng;

use crate::{
    fn_dag::{DagId, EnvFnExt, FnId},
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::Request,
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::{WithEnvCore, WithEnvHelp},
};

pub struct HEFTScheduler {
    dag_fns_priority: HashMap<DagId, Vec<(FnId, f32)>>,

    node_task_count: HashMap<NodeId, (usize, usize)>,

    node2node_all_bw: Vec<f32>,
}

impl HEFTScheduler {
    pub fn new() -> Self {
        HEFTScheduler {
            dag_fns_priority: HashMap::new(),

            node_task_count: HashMap::new(),

            node2node_all_bw: Vec::new(),
        }
    }

    fn prepare(&mut self, env: &SimEnvObserve) {
        for node in env.nodes().iter() {
            let node_id = node.node_id();

            self.node_task_count
                .insert(node_id, (node.all_task_cnt(), node.running_task_cnt()));
        }
    }

    fn prepare_node2node_all_bw(&mut self, env: &SimEnvObserve) {
        let node_count = env.nodes().len();

        let node2node_graph = env.core().node2node_graph();

        for i in 0..node_count {
            for j in 0..i {
                self.node2node_all_bw.push(node2node_graph[i][j]);
            }
        }
    }

    fn calculate_priority_for_dag_fns(&mut self, req: &Request, env: &SimEnvObserve) {
        let dag = env.dag(req.dag_i);

        if !self.dag_fns_priority.contains_key(&dag.dag_i) {
            let mut walker = dag.new_dag_walker();

            let mut map = HashMap::new();

            let mut stack = vec![];

            let n = env.nodes().len();

            while let Some(func_g_i) = walker.next(&dag.dag_inner) {
                let fnid = dag.dag_inner[func_g_i];
                let func = env.func(fnid);

                let mut t_sum_exec = 0.0;
                for node in env.nodes().iter() {
                    let node_running_task_count =
                        self.node_task_count.get(&node.node_id()).unwrap().1;
                    let each_running_task_cpu = node.rsc_limit.cpu / node_running_task_count as f32;
                    t_sum_exec += func.cpu / each_running_task_cpu
                }
                let t_avg_exec = t_sum_exec / n as f32;

                let mut t_sum_trans = 0.0;
                for bw in &self.node2node_all_bw {
                    t_sum_trans += func.out_put_size / bw * 5.0;
                }
                let t_avg_trans = t_sum_trans / self.node2node_all_bw.len() as f32;

                let initial_priority = t_avg_exec + t_avg_trans;
                map.insert(fnid, initial_priority);
                stack.push(func_g_i);
            }

            while let Some(func_g_i) = stack.pop() {
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

                    let fnid = dag.dag_inner[func_g_i];
                    (*map.get_mut(&fnid).unwrap()) += max;
                }
            }

            let mut priority_order = map.into_iter().collect::<Vec<_>>();
            priority_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or_else(|| Ordering::Equal));
            self.dag_fns_priority.insert(dag.dag_i, priority_order);
        }
    }

    fn schedule_one_req(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        req: &Request,
        cmd_distributor: &MechCmdDistributor,
    ) {
        self.calculate_priority_for_dag_fns(req, env);

        let mech_metric = || env.help().mech_metric_mut();

        let dag_fns_priority = self.dag_fns_priority.get(&req.dag_i).unwrap();

        let scheduleable_fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::All,
        );

        // 可调度的而函数按优先级排序
        let sorted_scheduleable_fns = {
            let mut sorted = Vec::new();
            for (fn_id, _) in dag_fns_priority {
                if scheduleable_fns.contains(fn_id) {
                    sorted.push(*fn_id);
                }
            }
            sorted
        };

        let mut scheduleable_fns_nodes = schedule_helper::collect_node_to_sche_task_to(&sorted_scheduleable_fns, env);

        for fnid in sorted_scheduleable_fns {
            let func = env.func(fnid);

            let mut target_cnt = mech.scale_num(fnid);
            if target_cnt == 0 {
                target_cnt = 1;
            }

            let fn_scale_up_cmds =
                mech.scale_up_exec()
                    .exec_scale_up(target_cnt, fnid, env, cmd_distributor);

            for cmd in fn_scale_up_cmds.iter() {
                scheduleable_fns_nodes
                    .get_mut(&cmd.fnid)
                    .unwrap()
                    .insert(cmd.nid);
            }

            let scheduleable_nodes = scheduleable_fns_nodes
                .get(&fnid)
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<usize>>();

            let mut best_node_id = 999;
            let mut min_exec_time = 100.0;

            for node_id in scheduleable_nodes {
                let node = env.node(node_id);
                let node_running_task_count = self.node_task_count.get(&node_id).unwrap().1;
                let each_running_task_cpu =  node.rsc_limit.cpu / node_running_task_count as f32;

                let exec_time = func.cpu / each_running_task_cpu;

                if min_exec_time > exec_time {
                    min_exec_time = exec_time;
                    best_node_id = node_id;
                }
            }

            if best_node_id == 999 {
                best_node_id = rand::thread_rng().gen_range(0..env.nodes().len());
            }

            mech_metric().add_node_task_new_cnt(best_node_id);

            if let Some((all_task_count, _)) = self.node_task_count.get_mut(&best_node_id) {
                *all_task_count += 1;
            }

            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    reqid: req.req_id,
                    fnid,
                    nid: best_node_id,
                    memlimit: None,
                }))
                .unwrap();
        }
    }
}

impl Scheduler for HEFTScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        self.node2node_all_bw.clear();
        self.node_task_count.clear();

        self.prepare(env);
        self.prepare_node2node_all_bw(env);

        for (_, req) in env.core().requests().iter() {
            self.schedule_one_req(env, mech, req, cmd_distributor);
        }

        for func in env.core().fns().iter() {
            let target = mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target < cur {
                mech.scale_down_exec().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                    cmd_distributor,
                );
            }
        }
    }
}
