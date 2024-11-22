use std::{
    cmp::Ordering,
    collections::HashMap,
};

use daggy::Walker;
use rand::Rng;

use crate::{
    fn_dag::{DagId, EnvFnExt, FnId}, 
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve}, 
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, 
    node::{EnvNodeExt, NodeId}, request::Request, 
    sim_run::{schedule_helper, Scheduler}, 
    util, 
    with_env_sub::{WithEnvCore, WithEnvHelp}
};

pub struct PriorityScheduler {
    // 每种DAG中每个函数的优先级（Vec降序，优先调度优先级高的函数）
    dag_fns_priority: HashMap<DagId, Vec<(FnId, f32)>>,

    // 每个节点剩余的资源（cpu，mem）
    node_resource_left: HashMap<NodeId, (f32, f32)>,

    // 每个节点上的任务数（running + pending，running）
    node_task_count: HashMap<NodeId, (usize, usize)>,

    // 每一对节点之间的带宽bandwidth
    node2node_all_bw: Vec<f32>,

    // 单位内存的开销
    mem_cost_per_unit: f32,

    mode: String,
}

impl PriorityScheduler {
    pub fn new(arg: &str) -> Self {
        Self {
            dag_fns_priority: HashMap::new(),

            node_resource_left: HashMap::new(),

            node_task_count: HashMap::new(),

            node2node_all_bw: Vec::new(),

            // mem_cost_per_unit: arg.parse::<f32>().unwrap(),
            mem_cost_per_unit: 0.005,

            mode: arg.to_string(),
        }
    }

    // 初始化node_resource_left、node_task_count
    fn prepare(&mut self, env: &SimEnvObserve) {

        for node in env.nodes().iter() {
            let node_id = node.node_id();

            self.node_resource_left.insert(
                node_id,
                (
                    node.rsc_limit.cpu - node.last_frame_cpu,
                    node.rsc_limit.mem - node.last_frame_mem,
                ),
            );

            self.node_task_count
                .insert(node_id, (node.all_task_cnt(), node.running_task_cnt()));
        }
    }

    // 初始化node2node_all_bw
    fn prepare_node2node_all_bw(&mut self, env: &SimEnvObserve) {

        let node_count = env.nodes().len();

        // 节点间带宽
        let node2node_graph = env.core().node2node_graph();

        for i in 0..node_count {
            for j in 0..i {
                self.node2node_all_bw
                    .push(node2node_graph[i][j]);
            }
        }
    }

    // 初始化dag_fns_priority
    fn calculate_priority_for_dag_fns(&mut self, req: &Request, env: &SimEnvObserve) {

        // 请求对应的DAG
        let dag = env.dag(req.dag_i);

        // 不同请求可能对应相同的DAG，已经计算过的DAG不再重复计算
        if !self.dag_fns_priority.contains_key(&dag.dag_i) {
            // DAG中每个函数对应的优先级
            let mut map = HashMap::new();

            let mut walker = dag.new_dag_walker();

            // 记录逆拓扑排序，按此顺序给函数赋予优先级
            let mut stack = vec![];

            // DAG中关键路径上的节点
            let critical_path_nodes = util::graph::aoe_critical_path(&dag.dag_inner);

            let m = self.node2node_all_bw.len();
            let n = env.nodes().len();

            // 拓扑排序
            while let Some(func_g_i) = walker.next(&dag.dag_inner) {
                let fnid = dag.dag_inner[func_g_i];
                let func = env.func(fnid);

                // 是否为DAG中关键路径上的节点
                let flag = critical_path_nodes.contains(&func_g_i);

                let mut t_sum_exec = 0.0;
                for node in env.nodes().iter() {
                    let node_running_task_count =
                            self.node_task_count.get(&node.node_id()).unwrap().1;

                    let each_running_task_cpu = node.rsc_limit.cpu / node_running_task_count as f32;
                    t_sum_exec += func.cpu / each_running_task_cpu
                }
                // 函数平均执行时间
                let t_avg_exec = t_sum_exec / n as f32;

                let mut t_sum_trans = 0.0;
                for bw in &self.node2node_all_bw {
                    t_sum_trans += func.out_put_size / bw * 5.0;
                }
                // 平均数据传输时间
                let t_avg_trans = t_sum_trans / m as f32;

                // 函数内存占用
                let mem_cost = func.mem as f32 * self.mem_cost_per_unit;

                // log::info!(
                //     "t_avg_exec{} t_avg_trans{} mem_cost{}",
                //     t_avg_exec,
                //     t_avg_trans,
                //     mem_cost
                // );

                // 函数初始优先级
                let mut initial_priority = t_avg_exec + t_avg_trans - mem_cost;

                // 考虑关键路径对优先级的影响
                if self.mode == "b" && flag {
                    initial_priority += 1.0;
                }

                map.insert(fnid, initial_priority);

                stack.push(func_g_i);
            }

            // 按逆拓扑排序为每一个函数计算priority，因为函数的优先级与其后继有关
            while let Some(func_g_i) = stack.pop() {
                let nexts: daggy::Children<usize, f32, u32> = dag.dag_inner.children(func_g_i);
                // 取后继中优先级最大的
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

            // 降序排序，优先调度优先级高的函数
            priority_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or_else(|| Ordering::Equal));

            // 记录当前dag中函数的优先级序列,避免重复计算
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
        // 计算请求对应的DAG中函数的优先级
        self.calculate_priority_for_dag_fns(req, env);

        let mech_metric = || env.help().mech_metric_mut();

        // 该请求对应的DAG中函数的优先级
        let dag_fns_priority = self.dag_fns_priority.get(&req.dag_i).unwrap();

        // 可调度的函数
        let fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::PreAllSched
        );

        // 可调度的函数及已经有其容器的节点
        let mut scheduleable_fns_nodes = schedule_helper::collect_node_to_sche_task_to(&fns, env);

        // 本次可调度的函数
        let scheduleable_fns = 
        // // 不排序直接调度
        // if self.mode == "a" {
        //     fns
        // } 
        // // 根据优先级排序后调度
        // else 
        {
            let mut sorted = Vec::new();
            for (fn_id, _) in dag_fns_priority {
                if fns.contains(fn_id) {
                    sorted.push(*fn_id);
                }
            }
            sorted
        };

        for fnid in scheduleable_fns {
            let func = env.func(fnid);

            // scale_sche_joint在调度前已经更新了函数所需容器的数量，获取
            let mut target_cnt = mech.scale_num(fnid);
            if target_cnt == 0 {
                target_cnt = 1;
            }

            // 扩容
            let fn_scale_up_cmds =
                mech.scale_up_exec()
                    .exec_scale_up(target_cnt, fnid, env, cmd_distributor);

            // 含有该函数的容器的节点 = 已经有容器的节点 + 扩容所选的节点
            for cmd in fn_scale_up_cmds.iter() {
                scheduleable_fns_nodes
                    .get_mut(&cmd.fnid)
                    .unwrap()
                    .insert(cmd.nid);
            }

            // 可调度的节点
            let scheduleable_nodes = 
            // // 所有节点
            // if self.mode == "a" {
            //     env.nodes().iter().map(|node| node.node_id()).collect::<Vec<usize>>()
            // } 
            // // 含有该函数的容器的节点
            // else 
            {
                scheduleable_fns_nodes.get(&fnid).unwrap().iter().cloned().collect::<Vec<usize>>()
            };

            // 最适合调度的节点
            let mut best_node_id = 999;
            // 最适合调度的节点的分数
            let mut best_score = -10.0;

            for node_id in scheduleable_nodes {
                let node = env.node(node_id);

                // 函数的前驱列表
                let pred_fnids = env.func(fnid).parent_fns(env);
            
                // 不在当前节点的前驱函数的个数
                let mut not_in_the_same_node = 0;
                
                // 不在当前节点的前驱函数的数据传输时间之和
                let mut transimission_time = 0.0;
                for pred in pred_fnids {
                    // 前驱所在节点
                    let &pred_node_id = req.fn_node.get(&pred).unwrap();

                    // 与前驱不在同一节点，计算数据传输时间
                    if pred_node_id != node_id {
                        not_in_the_same_node += 1;
                        
                        transimission_time += env.func(pred).out_put_size
                            / env.node_get_speed_btwn(pred_node_id, node_id);
                    }
                }

                let node_all_task_count = self.node_task_count.get(&node_id).unwrap().0;

                let node_cpu_left = self.node_resource_left.get(&node_id).unwrap().0;

                let node_mem_left = self.node_resource_left.get(&node_id).unwrap().1;

                // 节点分数
                let score_this_node = 
                // 不考虑节点剩余资源量
                // if self.mode == "a" {
                //     1.0 / (node_all_task_count as f32 + 1.0)
                //     // + node_cpu_left / node.rsc_limit.cpu
                //     // + node_mem_left / node.rsc_limit.mem
                //     - transimission_time                                                                                             
                // } 
                // // 考虑节点剩余资源量
                // else 
                {
                    1.0 / (node_all_task_count as f32 + 1.0)
                    + node_cpu_left / node.rsc_limit.cpu
                    + node_mem_left / node.rsc_limit.mem
                    - transimission_time
                };

                // log::info!("score_this_node {}", score_this_node);
                // log::info!("best_score {}", best_score);

                if score_this_node > best_score {
                    best_score = score_this_node;
                    best_node_id = node_id;
                }
            }

            if best_node_id == 999 {
                best_node_id = rand::thread_rng().gen_range(0..env.nodes().len());
            }

            mech_metric().add_node_task_new_cnt(best_node_id);

            // log::info!("best_node_id {}", best_node_id);

            // best_node任务总数 + 1
            if let Some((all_task_count, _)) = self.node_task_count.get_mut(&best_node_id) {
                *all_task_count += 1;
            }

            // 调度指令
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

impl Scheduler for PriorityScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 清理上一次调度的数据
        self.node_resource_left.clear();
        self.node2node_all_bw.clear();
        self.node_task_count.clear();

        // 获取每个节点的资源剩余、任务数
        self.prepare(env);

        // 获取每一对节点的bandwidth
        self.prepare_node2node_all_bw(env);

        // 调度每一个请求
        for (_, req) in env.core().requests().iter() {
            self.schedule_one_req(env, mech, req, cmd_distributor);
        }

        // 缩容
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