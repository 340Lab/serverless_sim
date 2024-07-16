use core::alloc;
use std::{borrow::Borrow, cell::Ref, collections::{HashMap, HashSet}, env, thread::{self, sleep}, time::Duration, vec};


use crate::{
    fn_dag::{EnvFnExt, FnContainerState, FnId, RunningTask}, mechanism::{DownCmd, MechanismImpl, ScheCmd, SimEnvObserve}, mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, node::{self, EnvNodeExt, NodeId}, request::Request, sim_run::{schedule_helper, Scheduler}, with_env_sub::{WithEnvCore, WithEnvHelp}
};

// 维护 bp 数组用
struct BpListUpdateNodes{
    // 平均cpu利用率
    avg_cpu_starve_degree: f32,

    // 平均mem利用率
    avg_mem_use_rate: f32,

    // binpack内饥饿程度最高的节点
    expel_nodeid_inbp: usize,

    // binpack外资源得分最高的节点
    join_nodeid_outbp: usize,
}

struct NodeRescState {
    mem_used: f32,
    mem_limit: f32,
    cpu_left_calc: f32,
    cpu_limit: f32,

    // 饥饿程度
    cpu_starve_degree: f32,

    // 资源得分
    resource_score: f32,

    // 待处理任务数量
    pending_task_cnt: f32,
}

#[derive(Debug, Clone, Copy)]
struct FnContainerCpuStatus {
    node_id: NodeId,
    alloced_cpu: f32,
    cpu_left_calc: f32,
    cpu_starve_degree: f32,
}

pub struct BpBalanceScheduler {
    // 每个函数的binpack节点集合
    binpack_map: HashMap<FnId, HashSet<NodeId>>,

    // 每个函数的最新节点集合
    latest_nodes: HashMap<FnId, HashSet<NodeId>>,

    // 每个节点的资源使用情况，实时更新---调度时需要实时更新
    nodes_resc_state: HashMap<NodeId, NodeRescState>,

    // 判断函数是否可以开始用bp_balance数组机制
    mech_impl_sign: HashMap<FnId, bool>,
}

impl BpBalanceScheduler {
    pub fn new() -> Self {
        Self {
            binpack_map: HashMap::new(),
            latest_nodes: HashMap::new(),
            nodes_resc_state: HashMap::new(),
            mech_impl_sign: HashMap::new(),
        }
    }

    // 找出binpack数组中 最合适的点(取决于贪婪的指标是什么) 的节点、并且要内存足够才行
    fn find_min_starve_nodeid(&self, fnid: FnId, env: &SimEnvObserve) -> usize{
        let mut schedule_node_id: usize = 9999;
        let mut most_degree_metric = 0.0;
        let bplist = self.binpack_map.get(&fnid).unwrap();

        // 遍历所有容器的资源状态
        for node_id in bplist {
            // 要所在节点的内存足够才行，否则跳过该节点
            let node_resource_status = self.nodes_resc_state.get(node_id).unwrap();
            if node_resource_status.mem_limit - node_resource_status.mem_used < env.func(fnid).mem {
                continue;
            }

            // // 此时修改成对节点的 饥饿程度 贪婪
            // if schedule_node_id == 9999{
            //     schedule_node_id = *node_id;
            //     most_degree_metric = node_resource_status.cpu_starve_degree;
            // }
            // else {
            //     // 比较出 饥饿程度 最小的节点
            //     if node_resource_status.cpu_starve_degree < most_degree_metric {
            //         schedule_node_id = *node_id;
            //         most_degree_metric = node_resource_status.cpu_starve_degree;
            //     }
            // }

            // 此时修改成对节点的 待处理任务数量 贪婪
            if schedule_node_id == 9999{
                schedule_node_id = *node_id;
                most_degree_metric = node_resource_status.pending_task_cnt;
            }
            else {
                // 比较出 待处理任务数量 最小的节点
                if node_resource_status.pending_task_cnt < most_degree_metric {
                    schedule_node_id = *node_id;
                    most_degree_metric = node_resource_status.pending_task_cnt;
                }
            }

            // // 此时对节点的 资源得分 贪婪
            // if schedule_node_id == 9999{
            //     schedule_node_id = *node_id;
            //     most_degree_metric = node_resource_status.resource_score;
            // }
            // else {
            //     // 比较出 待处理任务数量 最小的节点
            //     if node_resource_status.resource_score > most_degree_metric {
            //         schedule_node_id = *node_id;
            //         most_degree_metric = node_resource_status.resource_score;
            //     }
            // }
            
        }

        schedule_node_id
    }
    
    // 获得数据，维护 bp 数组
    fn get_bplist_node_status(&self, fnid: FnId, _env: &SimEnvObserve) -> BpListUpdateNodes{
        
        let binpack = self.binpack_map.get(&fnid).unwrap();

        let mut avg_cpu_starve_degree = 0.0;
        let mut avg_mem_use_rate = 0.0;
        let mut max_starve_nodeid_inbp = 9999;
        let mut max_score_nodeid_outbp = 9999;

        // 遍历该函数的可执行节点集合
        for nodeid in self.latest_nodes.get(&fnid).unwrap().iter() {

            // 取出当前节点的资源使用情况
            let iter_node_resc_state = self.nodes_resc_state.get(&nodeid).unwrap();

            // 找到binpack内饥饿程度最高的节点、binpack外资源得分最高的节点,同时计算bp内平均mem利用率、cpu饥饿程度
            if binpack.contains(nodeid){
                // 统计binpack内节点的平均mem利用率
                avg_mem_use_rate +=
                    iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;

                // 统计bp数组内的节点的平均cpu饥饿程度
                if iter_node_resc_state.cpu_starve_degree != 0.0 {
                    // log::info!("func_id: {}, 目前总cpu饥饿: {}, 加上: {}", fnid, avg_cpu_starve_degree, container_status.cpu_starve_degree);
                    avg_cpu_starve_degree += iter_node_resc_state.cpu_starve_degree;
                }

                // 计算binpack内，针对于该函数容器的饥饿程度最高的节点
                if max_starve_nodeid_inbp == 9999{
                    max_starve_nodeid_inbp = *nodeid;
                }
                else {
                    // 取出目前最饥饿节点的资源状态
                    let max_starve_node_resc_state = self.nodes_resc_state.get(&max_starve_nodeid_inbp).unwrap();

                    if iter_node_resc_state.cpu_starve_degree > max_starve_node_resc_state.cpu_starve_degree {
                        max_starve_nodeid_inbp = *nodeid;
                    }
                }
            }
            // binpack外资源得分最高的节点。资源得分 = (1 - 节点cpu饥饿) * 4 + mem空闲率
            else {
                if max_score_nodeid_outbp == 9999{
                    max_score_nodeid_outbp = *nodeid;
                }
                else {
                    // 取出目前得分最高节点的资源状态
                    let max_node_resc_state = self.nodes_resc_state.get(&max_score_nodeid_outbp).unwrap();
                    
                    if iter_node_resc_state.resource_score > max_node_resc_state.resource_score {
                        max_score_nodeid_outbp = *nodeid
                    }
                }
            }
        }

        // 计算平均
        if binpack.len() != 0 {
            avg_mem_use_rate /= binpack.len() as f32;
            avg_cpu_starve_degree /= binpack.len() as f32;
        }
        else {
            avg_mem_use_rate = 0.0;
            avg_cpu_starve_degree = 0.0;
        }

        // 把平均cpu饥饿程度打在日志上
        log::info!("func_id: {}, avg_cpu_starve_degree: {}, avg_mem_use_rate: {}", fnid, avg_cpu_starve_degree, avg_mem_use_rate);
        // log::info!("func_id: {}, max_starve_nodeid_inbp: {}, max_score_nodeid_outbp: {}", fnid, max_starve_nodeid_inbp, max_score_nodeid_outbp);

        BpListUpdateNodes{
            avg_cpu_starve_degree,
            avg_mem_use_rate,
            expel_nodeid_inbp: max_starve_nodeid_inbp,
            join_nodeid_outbp: max_score_nodeid_outbp,
        }
    }

    // 对 bp数组进行更新，因为对 bp 数组的更新都要伴随着对 fn_container_cpu_status 的更新，所以放在一个函数以便两者同时进行
    fn update_bplist(&mut self, fnid: FnId, node_id: NodeId, operate: &str, _env: &SimEnvObserve){
        let fn_binpack_map = self.binpack_map.get_mut(&fnid).unwrap();
        if operate == "add" {
            // 对 binpack 进行更新
            fn_binpack_map.insert(node_id);
        }
        // 如果是 remove 操作，则移除该节点的状态
        else if operate == "remove" {
            // 对 binpack 进行更新
            fn_binpack_map.remove(&node_id);
        }
        else {
            panic!("operate: {} not supported", operate);
        }
    }

    // TODO 调度之后的资源情况需要更新
    fn schedule_one_req_fns(
        &mut self, 
        env: &SimEnvObserve, 
        req: &mut Request, 
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 收集该请求中所有可以调度的函数
        let mut schedule_able_fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::PreAllSched,
        );

        // 对所有函数进行优先级排序，cpu需求较大的在前面
        schedule_able_fns.sort_by(|&a, &b| {
            env.func(a)
                .cpu
                .partial_cmp(&env.func(b).cpu)
                .unwrap()
                .reverse()
        });

        // 进行调度，每次函数请求到达时，往binpack数组中空闲内存最多的节点上调度，并实时更新节点容量。
        let mech_metric = || env.help().mech_metric_mut();

        for &fnid in &schedule_able_fns {
            // 找出该函数的binpack数组中空闲内存最多的节点
            let sche_nodeid = self.find_min_starve_nodeid(fnid, env);

            if sche_nodeid != 9999 {
                mech_metric().add_node_task_new_cnt(sche_nodeid);

                cmd_distributor
                    .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                        reqid: req.req_id,
                        fnid,
                        nid: sche_nodeid,
                        memlimit: None,
                    }))
                    .unwrap();

                // 更新node_resc_state中的资源使用情况：mem_used, cpu_left_calc, cpu_starve_degree, resource_score
                let sche_node_resc_state = self.nodes_resc_state.get_mut(&sche_nodeid).unwrap();
                sche_node_resc_state.mem_used += env.func(fnid).mem;
                sche_node_resc_state.cpu_left_calc += env.func(fnid).cpu;
                sche_node_resc_state.cpu_starve_degree = cal_cpu_starve_degree(sche_node_resc_state.cpu_limit, sche_node_resc_state.cpu_left_calc);
                sche_node_resc_state.resource_score = (1.0 - sche_node_resc_state.cpu_starve_degree) * 4.0 + sche_node_resc_state.mem_used / sche_node_resc_state.mem_limit;
                sche_node_resc_state.pending_task_cnt += 1.0;

                // 计算该函数binpack数组的资源情况
                let bplist_resource_status = self.get_bplist_node_status(fnid, env);

                // 如果binpack内平均资源利用率大于所设阈值，则将含有该函数对应容器快照的目前空余资源量最多的节点加入该binpack数组
                if (bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9) && bplist_resource_status.join_nodeid_outbp != 9999 {
                    self.update_bplist(fnid, bplist_resource_status.join_nodeid_outbp, "add", env);
                }
            }

        }
    }

}

impl Scheduler for BpBalanceScheduler {

    fn schedule_some(&mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,){

        // 遍历每个节点，更新其资源使用情况
        for node in env.core().nodes().iter() {
            // 内存
            let mem_used = env.node(node.node_id()).last_frame_mem;
            let mem_limit = env.node(node.node_id()).rsc_limit.mem;

            // cpu
            let cpu_limit = env.node(node.node_id()).rsc_limit.cpu;
            let mut cpu_left_calc = 0.0;
            // 遍历节点上的每个容器，累加这些容器的剩余计算量
            for container in node.fn_containers.borrow().values() {
                if container.is_running() {
                    for running_tasks in container.req_fn_state.values() {
                        cpu_left_calc += running_tasks.left_calc;
                    }
                }
            }

            // 计算cpu饥饿程度
            let cpu_starve_degree = cal_cpu_starve_degree(cpu_limit, cpu_left_calc);

            // 资源得分
            let resource_score = (1.0 - cpu_starve_degree) * 4.0 + mem_used / mem_limit;
            self.nodes_resc_state.insert(node.node_id(), 
                NodeRescState
                {
                    mem_used, mem_limit, cpu_left_calc, cpu_limit, cpu_starve_degree, resource_score, pending_task_cnt: node.pending_task_cnt() as f32,
                }
            );
        }

        // 遍历每个函数，为其获取扩缩容命令，维护 binpack 数组
        for func in env.core().fns().iter() {
            // 初始化每个函数的 bp 数组
            if !self.binpack_map.contains_key(&func.fn_id){
                self.binpack_map.insert(func.fn_id, HashSet::new());
            }

            // 初始化每个函数的 latest_nodes 数组
            if !self.latest_nodes.contains_key(&func.fn_id){
                self.latest_nodes.insert(func.fn_id, HashSet::new());
            }

            // 初始化每个函数的 mech_impl_sign
            if !self.mech_impl_sign.contains_key(&func.fn_id){
                self.mech_impl_sign.insert(func.fn_id, false);
            }

            // 进行其他处理之前，先更新最新节点集合
            let mut nodes = HashSet::new();
            env.fn_containers_for_each(func.fn_id, |container| {
                nodes.insert(container.node_id);
            });


            // 根据 scaler 得出的容器数量进行扩缩容--------------------------------------------------------------------
            let target = mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            // 如果需要扩容
            if target > cur {
                let up_cmd = mech.scale_up_exec().exec_scale_up(
                    target, 
                    func.fn_id, env, 
                    cmd_distributor
                );

                // 实时更新函数的节点情况
                for cmd in up_cmd.iter() {
                    nodes.insert(cmd.nid);
                }
            }
            // 如果需要缩容
            else if target < cur {
                // 标记可以开始bp机制
                if self.mech_impl_sign.get(&func.fn_id).unwrap() == &false {
                    log::info!("fn_id: {}, 在第 {} 帧触发机制", func.fn_id, env.core().current_frame());
                }
                // MARK 这里关闭了bp机制的，如果需要开启bp机制，对下行注释取消
                self.mech_impl_sign.insert(func.fn_id, true);
                
                // 进行缩容
                let down_cmd = mech.scale_down_exec().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                    cmd_distributor
                );

                // 实时更新函数的节点情况
                for cmd in down_cmd.iter() {
                    nodes.remove(&cmd.nid);
                }
            }
            // ---------------------------------------------------------------------------------------------------------------------------

            // 机制没有触发，则该函数的bp数组就是nodes
            if !*self.mech_impl_sign.get(&func.fn_id).unwrap(){
                // 该函数的可调度节点和最新节点集合就是nodes
                self.latest_nodes.insert(func.fn_id, nodes.clone());
                self.binpack_map.insert(func.fn_id, nodes.clone());
            }
            // 如果bp机制触发，则开始维护该函数的bp数组
            else {

                // 清理一下binpack数组中被意外缩容的节点，一般不会出现这个情况
                let binpack_nodeids = self.binpack_map.get(&func.fn_id).unwrap().clone();
                for nodeid in binpack_nodeids.iter() {
                    if !nodes.contains(nodeid) {
                        self.update_bplist(func.fn_id, *nodeid, "remove", env);
                    }
                }

                let binpack = self.binpack_map.get(&func.fn_id).unwrap();
                // 该函数没有可调度节点，表示该函数最近一直没有请求，直接跳过
                if nodes.len() == 0 {
                    assert!(binpack.len() == 0);
                    continue;
                }

                // 重新拿一次，避免借用冲突
                let binpack = self.binpack_map.get(&func.fn_id).unwrap();

                // 遍历每个容器，对binpack数组外的容器进行超时缩容------------------------------------------
                env.fn_containers_for_each(func.fn_id, |container| {
                    
                    // 对于不是binpack数组中的节点，进行超时缩容
                    if !binpack.contains(&container.node_id) {
                        
                        // 如果该容器最近20帧都是空闲则缩容
                        if container.recent_frame_is_idle(20) && container.req_fn_state.len() == 0  {
                            
                            // 发送缩容命令
                            cmd_distributor
                                .send(MechScheduleOnceRes::ScaleDownCmd(DownCmd 
                                    {
                                        nid: container.node_id,
                                        fnid: func.fn_id
                                    }
                                ))
                                .unwrap();

                            nodes.remove(&container.node_id);
                        }
                    }

                });
                // 超时缩容完成----------------------------------------------------------------------------------------

                // 更新该函数的最新可调度节点集合
                self.latest_nodes.insert(func.fn_id, nodes.clone());

                // 对 binpack 数组进行维护----------------------------------------------------------------------------------------
                let binpack = self.binpack_map.get(&func.fn_id).unwrap();

                // 当binpack数组为空时，把所有节点都加进去
                if binpack.len() == 0 {
                    self.binpack_map.insert(func.fn_id, nodes.clone());

                }

                // 计算该函数binpack数组内的资源利用率，以及得出其内、其外的空闲资源最多的节点id
                let mut bplist_resource_status = self.get_bplist_node_status(func.fn_id, env);
                
                // 维护binpack数组，直到其数组内  0.5 < 平均cpu饥饿程度 < 0.9 && 平均mem利用率 < 0.8
                while bplist_resource_status.avg_cpu_starve_degree != 0.0 && bplist_resource_status.avg_cpu_starve_degree < 0.5 || bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9 {
                    
                    // 如果需要添加/删除，但是没有可供添加/删除的节点了，就退出循环。不能放在while外面，否则可能会无限循环
                    if bplist_resource_status.avg_cpu_starve_degree < 0.5 && bplist_resource_status.expel_nodeid_inbp == 9999 {
                        break;
                    }
                    if (bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9) && bplist_resource_status.join_nodeid_outbp == 9999 {
                        break;
                    }

                    // 如果binpack内平均cpu饥饿程度小于0.5，则逐出数组中饥饿程度最高的节点
                    if bplist_resource_status.avg_cpu_starve_degree < 0.5 && bplist_resource_status.expel_nodeid_inbp != 9999 {
                        log::info!("fnid:{}, avg_cpu_starve_degree:{}, expel_nodeid_inbp:{}", func.fn_id, bplist_resource_status.avg_cpu_starve_degree, bplist_resource_status.expel_nodeid_inbp);
                        self.update_bplist(func.fn_id, bplist_resource_status.expel_nodeid_inbp, "remove", env);
                    }
                    // 如果binpack内平均mem利用率大于80%或平均饥饿程度大于0.9，则将bp数组外资源得分最高的节点加入该binpack数组
                    else if (bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9) && bplist_resource_status.join_nodeid_outbp != 9999 {
                        self.update_bplist(func.fn_id, bplist_resource_status.join_nodeid_outbp, "add", env);
                    }

                    // log::info!("bplist长度:{}, nodes长度:{}", self.binpack_map.get(&func.fn_id).unwrap().len(), self.latest_nodes.get(&func.fn_id).unwrap().len());

                    // 再次计算
                    bplist_resource_status = self.get_bplist_node_status(func.fn_id, env);
                }
                
                // 为了防止借用冲突
                let binpack = self.binpack_map.get(&func.fn_id).unwrap();

                log::info!("fnid:{}, binpack_len:{}, latest_nodes_len:{}", func.fn_id, binpack.len(), self.latest_nodes.get(&func.fn_id).unwrap().len());

            }

            // 每20帧，等待100ms。看长度情况
            
            // 每帧要更新一次 container_cpu_status_bp
        }

        // 遍历调度每个请求
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            self.schedule_one_req_fns(env, req, cmd_distributor);
        }

    }
}

fn cal_cpu_starve_degree(alloced_cpu: f32, cpu_left_calc: f32,)->f32{
    let mut cpu_starve_degree = 0.0;
    if cpu_left_calc > alloced_cpu {
        cpu_starve_degree = 1.0 - (alloced_cpu / cpu_left_calc);
    }

    return cpu_starve_degree;
}