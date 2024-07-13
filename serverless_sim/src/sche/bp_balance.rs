use core::alloc;
use std::{borrow::Borrow, collections::{HashMap, HashSet}, vec};


use crate::{
    fn_dag::{EnvFnExt, FnContainerState, FnId, RunningTask}, mechanism::{DownCmd, MechanismImpl, ScheCmd, SimEnvObserve}, mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, node::{EnvNodeExt, NodeId}, request::Request, sim_run::{schedule_helper, Scheduler}, with_env_sub::{WithEnvCore, WithEnvHelp}
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

    // 每个函数的binpack节点cpu状态集合---调度时需要实时更新
    container_cpu_status_bp: HashMap<FnId, Vec<FnContainerCpuStatus>>,

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
            container_cpu_status_bp: HashMap::new(),
            latest_nodes: HashMap::new(),
            nodes_resc_state: HashMap::new(),
            mech_impl_sign: HashMap::new(),
        }
    }

    // 找出binpack数组中饥饿程度最小的节点、并且要内存足够才行
    fn find_min_starve_nodeid(&self, fnid: FnId, env: &SimEnvObserve) -> usize{
        let mut min_starve_nodeid: usize = 9999;
        let mut min_starve_degree = 1.0;
        let container_cpu_status_bp = self.container_cpu_status_bp.get(&fnid).unwrap();

        // 遍历所有容器的资源状态
        for status in container_cpu_status_bp {
            // 要所在节点的内存足够才行，否则跳过该节点
            let node_resource_status = self.nodes_resc_state.get(&status.node_id).unwrap();
            if node_resource_status.mem_limit - node_resource_status.mem_used < env.func(fnid).mem {
                continue;
            }

            // 初始化
            if min_starve_nodeid == 9999{
                min_starve_nodeid = status.node_id;
                min_starve_degree = status.cpu_starve_degree;
            }
            else {
                // 比较出饥饿程度最小的节点
                if status.cpu_starve_degree < min_starve_degree {
                    min_starve_nodeid = status.node_id;
                    min_starve_degree = status.cpu_starve_degree;
                }
            }
        }

        min_starve_nodeid
    }

    // 获取函数在bp数组内的节点上的容器的cpu状态
    fn get_container_cpu_status_by_nodeid(&self, fnid: FnId, nodeid: NodeId) -> Option<FnContainerCpuStatus> {
        match self.container_cpu_status_bp.get(&fnid) {
            Some(containers_cpu_status) => {
                for status in containers_cpu_status {
                    if status.node_id == nodeid {
                        return Some(status.clone());
                    }
                }
                panic!("func_id: {}, fn_bpFncontainer_cpu_status != binpack_map", fnid);
            },
            None => {
                panic!("func_id: {}, not found in fn_bpFncontainer_cpu_status", fnid);
            }
        }
    }
    

    // 获得数据，维护 bp 数组
    fn get_bplist_node_status(&self, fnid: FnId, _env: &SimEnvObserve) -> BpListUpdateNodes{
        
        let binpack = self.binpack_map.get(&fnid).unwrap();

        let mut avg_cpu_starve_degree = 0.0;
        let mut avg_mem_use_rate = 0.0;
        let mut max_starve_nodeid_inbp = 9999;
        let mut max_score_nodeid_outbp = 9999;

        let mut running_container_count = 0;

        // 遍历该函数的可执行节点集合
        for nodeid in self.latest_nodes.get(&fnid).unwrap().iter() {

            // 取出当前节点的资源使用情况
            let iter_node_resc_state = self.nodes_resc_state.get(&nodeid).unwrap();

            // 找到binpack内饥饿程度最高的节点、binpack外资源得分最高的节点,同时计算bp内平均mem利用率、cpu饥饿程度
            if binpack.contains(nodeid){
                // 统计binpack内节点的平均mem利用率
                avg_mem_use_rate +=
                    iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;

                // 统计bp数组内的容器的平均cpu饥饿程度，只算运行中的容器。
                let container_status = self.get_container_cpu_status_by_nodeid(fnid, *nodeid).unwrap();
                if container_status.cpu_starve_degree != 0.0 {
                    avg_cpu_starve_degree += container_status.cpu_starve_degree;
                    running_container_count += 1;
                }

                // 计算binpack内，针对于该函数容器的饥饿程度最高的节点
                if max_starve_nodeid_inbp == 9999{
                    max_starve_nodeid_inbp = *nodeid;
                }
                else {
                    // 取出目前最饥饿节点的资源状态
                    let max_starve_node_resc_state = self.get_container_cpu_status_by_nodeid(fnid, max_starve_nodeid_inbp).unwrap();

                    if container_status.cpu_starve_degree > max_starve_node_resc_state.cpu_starve_degree {
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
        avg_mem_use_rate /= binpack.len() as f32;

        if running_container_count != 0 {
            avg_cpu_starve_degree /= running_container_count as f32;
        }

        BpListUpdateNodes{
            avg_cpu_starve_degree,
            avg_mem_use_rate,
            expel_nodeid_inbp: max_starve_nodeid_inbp,
            join_nodeid_outbp: max_score_nodeid_outbp,
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
                sche_node_resc_state.cpu_starve_degree = 1.0 - sche_node_resc_state.cpu_limit / sche_node_resc_state.cpu_left_calc;
                sche_node_resc_state.resource_score = (1.0 - sche_node_resc_state.cpu_starve_degree) * 4.0 + sche_node_resc_state.mem_used / sche_node_resc_state.mem_limit;
                
                // 更新container_cpu_status_bp的内容
                let fncontainer_cpu_status = self.container_cpu_status_bp.get_mut(&fnid).unwrap();
                for status in fncontainer_cpu_status {
                    if status.node_id == sche_nodeid {
                        status.cpu_left_calc += env.func(fnid).cpu;
                        status.cpu_starve_degree = 1.0 - status.alloced_cpu / status.cpu_left_calc;
                    }
                }

                // 计算该函数binpack数组的资源情况
                let bplist_resource_status = self.get_bplist_node_status(fnid, env);

                // 如果binpack内平均资源利用率大于所设阈值，则将含有该函数对应容器快照的目前空余资源量最多的节点加入该binpack数组
                if (bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9) && bplist_resource_status.join_nodeid_outbp != 9999 {
                    self.binpack_map.get_mut(&fnid).unwrap().insert(bplist_resource_status.join_nodeid_outbp);
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
            let cpu_starve_degree = 1.0 - cpu_limit / cpu_left_calc;

            // 资源得分
            let resource_score = (1.0 - cpu_starve_degree) * 4.0 + mem_used / mem_limit;
            self.nodes_resc_state.insert(node.node_id(), 
                NodeRescState
                {
                    mem_used, mem_limit, cpu_left_calc, cpu_limit, cpu_starve_degree, resource_score
                }
            );
        }

        // 遍历每个函数，为其获取扩缩容命令，维护一个binpack节点数组和一个可执行节点数组
        for func in env.core().fns().iter() {
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
                self.mech_impl_sign.insert(func.fn_id, true);
                log::info!("fn_id: {}, start bp mechanism at {} frame", func.fn_id, env.core().current_frame());

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
                // 获得binpack数组
                let binpack = self.binpack_map.get_mut(&func.fn_id).unwrap();

                // 清理一下binpack数组中被意外缩容的节点，一般不会出现这个情况
                let binpack_nodeids = binpack.clone();
                for nodeid in binpack_nodeids.iter() {
                    if !nodes.contains(nodeid) {
                        binpack.remove(nodeid);
                    }
                }

                // 遍历每个容器，对binpack数组外的容器进行超时缩容--------------------------------
                if nodes.len() > binpack.len() + 1 {

                    // MARK 这里可以用数据结构加速，再为每个函数设定一个不在bp数组内的节点集合就行
                    // 遍历所有节点，找出不在bp数组内的节点并进行超时缩容
                    env.fn_containers_for_each(func.fn_id, |container| {
                        
                        // 对于不是binpack数组中的节点，进行超时缩容，但是至少要留一个
                        if nodes.len() > binpack.len() + 1 && !binpack.contains(&container.node_id) {
                            
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
                }
                // 超时缩容完成----------------------------------------------------------------------------------------

                // 更新该函数的最新可调度节点集合
                self.latest_nodes.insert(func.fn_id, nodes.clone());

                // 对 binpack 数组进行维护----------------------------------------------------------------------------------------
                let binpack = self.binpack_map.get_mut(&func.fn_id).unwrap();

                // 当binpack数组为空时（正常运行的话不会是空的），把所有节点都加进去
                if binpack.len() == 0 {
                    self.binpack_map.insert(func.fn_id, nodes.clone());
                }

                // 计算该函数binpack数组内的资源利用率，以及得出其内、其外的空闲资源最多的节点id
                let mut bplist_resource_status = self.get_bplist_node_status(func.fn_id, env);
                
                // 维护binpack数组，直到其数组内  0.5 < 平均cpu饥饿程度 < 0.9 && 平均mem利用率 < 0.8
                while bplist_resource_status.avg_cpu_starve_degree < 0.5 || bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9 {
                    // 如果没有可添加的节点，并且也没有可驱逐的节点了，就退出。不能放在while外面，否则可能会无限循环
                    if bplist_resource_status.expel_nodeid_inbp == 9999 && bplist_resource_status.join_nodeid_outbp == 9999 {
                        break;
                    }

                    let binpack = self.binpack_map.get_mut(&func.fn_id).unwrap();

                    // 如果binpack内平均cpu饥饿程度小于0.5，则逐出数组中饥饿程度最高的节点，但是至少要有一个节点
                    if bplist_resource_status.avg_mem_use_rate < 0.5 && bplist_resource_status.expel_nodeid_inbp != 9999 && binpack.len() > 1{
                        binpack.remove(&bplist_resource_status.expel_nodeid_inbp);
                    }
                    // 如果binpack内平均mem利用率大于80%或平均饥饿程度大于0.9，则将bp数组外资源得分最高的节点加入该binpack数组
                    else if (bplist_resource_status.avg_mem_use_rate > 0.8 || bplist_resource_status.avg_cpu_starve_degree > 0.9) && bplist_resource_status.join_nodeid_outbp != 9999 {
                        binpack.insert(bplist_resource_status.join_nodeid_outbp);
                    }

                    // 再次计算
                    bplist_resource_status = self.get_bplist_node_status(func.fn_id, env);
                }
                
                // 为了防止借用冲突
                let binpack = self.binpack_map.get_mut(&func.fn_id).unwrap(); 

                log::info!("fnid:{}, binpack_len:{}, latest_nodes_len:{}", func.fn_id, binpack.len(), self.latest_nodes.get(&func.fn_id).unwrap().len());

            }

            // // 每20帧，等待100ms。看长度情况
            // if *env.core.current_frame() % 10 == 0 {
            //     let a_millis = 50;
            //     let wait_duration = Duration::from_millis(a_millis);
            //     // 让当前线程暂停指定的持续时间
            //     thread::sleep(wait_duration);
            // }

            // 对每个函数每帧处理一次所有容器的cpu数据
            let binpack = self.binpack_map.get(&func.fn_id).unwrap();
            let mut fncontainers_cpu: Vec<FnContainerCpuStatus> = Vec::new();
            for node_id in binpack.iter() {
                let node = env.node(*node_id);
                let fncontainer = node.container(func.fn_id);

                // 这里只具体计算已经是运行状态的容器，对于只是逻辑上的容器或者正在启动状态的容器则不计算cpu饥饿程度，设置为默认值0
                if fncontainer.is_some() && fncontainer.unwrap().is_running() {
                    let fncontainer = node.container(func.fn_id).unwrap();
                    // 获取上一帧分配的cpu总量
                    let alloced_cpu = fncontainer.last_frame_cpu_used / fncontainer.cpu_use_rate();
    
                    // 计算剩余需要的cpu量
                    let mut left_calc = 0.0;
                    for running_tasks in fncontainer.req_fn_state.values() {
                        left_calc += running_tasks.left_calc;
                    }
    
                    fncontainers_cpu.push(FnContainerCpuStatus{
                        node_id: *node_id,
                        alloced_cpu: alloced_cpu,
                        cpu_left_calc: left_calc,
                        cpu_starve_degree: 1.0 - (alloced_cpu / left_calc)
                    });
                }
                else {
                    // alloced_cpu直接用节点上的每个容器平分cpu来算
                    let container_count = env.node(*node_id).fn_containers.borrow().len() as f32;
                    let alloced_cpu = env.node(*node_id).rsc_limit.cpu / container_count;

                    fncontainers_cpu.push(FnContainerCpuStatus{
                        node_id: *node_id,
                        alloced_cpu: alloced_cpu,
                        cpu_left_calc: 0.0,
                        cpu_starve_degree: 0.0,
                    });
                }
            }
            // 记录该函数bp数组内的每个容器的cpu状态。fncontainers_cpu里只有在bp内的容器
            self.container_cpu_status_bp.insert(func.fn_id, fncontainers_cpu);
        }

        // 遍历调度每个请求
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            self.schedule_one_req_fns(env, req, cmd_distributor);
        }

    }
}