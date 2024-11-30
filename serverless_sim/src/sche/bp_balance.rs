use core::alloc;
use std::{borrow::Borrow, cell::Ref, collections::{HashMap, HashSet}, env, thread::{self, panicking, sleep}, time::Duration, vec};


use crate::{
    fn_dag::{EnvFnExt, FnContainerState, FnId, RunningTask}, mechanism::{DownCmd, MechanismImpl, ScheCmd, SimEnvObserve}, mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, node::{self, EnvNodeExt, NodeId}, request::Request, sim_run::{schedule_helper, Scheduler}, with_env_sub::{WithEnvCore, WithEnvHelp}
};

const CPU_THRESHOLD_TO_ADD: f32 = 1.0;
const CPU_THRESHOLD_TO_REMOVE: f32 = 0.7;
const MEM_THRESHOLD_TO_ADD: f32 = 0.9;

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
    cpu_limit: f32,

    // 资源得分
    resource_score: f32,

    // 所有任务数量
    all_task_cnt: f32,
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

    // 需要调度的函数集合
    need_schedule_fn: HashSet<FnId>,
}

impl BpBalanceScheduler {
    pub fn new() -> Self {
        Self {
            binpack_map: HashMap::new(),
            latest_nodes: HashMap::new(),
            nodes_resc_state: HashMap::new(),
            mech_impl_sign: HashMap::new(),
            need_schedule_fn: HashSet::new(),
        }
    }

    // 找出binpack数组中 最合适的点(取决于贪婪的指标是什么) 的节点、并且要内存足够才行
    fn find_schedule_nodeid(&self, fnid: FnId, env: &SimEnvObserve) -> usize{
        let mut schedule_node_id: usize = 9999;
        let mut best_node_metric = 0.0;
        let bplist = self.binpack_map.get(&fnid).unwrap();

        // 遍历所有容器的资源状态
        for node_id in bplist {
            let iter_node_resource_status = self.nodes_resc_state.get(node_id).unwrap();

            // 找出 饥饿程度 最小的
            let iter_node_starve_degree = self.cal_cpu_starve_degree(iter_node_resource_status, fnid, env);
            if schedule_node_id == 9999{
                schedule_node_id = *node_id;
                best_node_metric = iter_node_starve_degree;
            }
            else {
                // 比较出 饥饿程度 最小的节点
                if iter_node_starve_degree < best_node_metric {
                    schedule_node_id = *node_id;
                    best_node_metric = iter_node_starve_degree;
                }
            }

            // // 找出 待处理任务数量 最小的节点
            // if schedule_node_id == 9999{
            //     schedule_node_id = *node_id;
            //     best_node_metric = iter_node_resource_status.all_task_cnt;
            // }
            // else {
            //     // 比较出 待处理任务数量 最小的节点
            //     if iter_node_resource_status.all_task_cnt < best_node_metric {
            //         schedule_node_id = *node_id;
            //         best_node_metric = iter_node_resource_status.all_task_cnt;
            //     }
            // }
            
        }

        schedule_node_id
    }
    
    // 计算函数分配到节点上后，cpu饥饿程度 = fn_cpu_use / cpu_local
    fn cal_cpu_starve_degree(&self, node_resc_state: &NodeRescState, fnid: FnId, env: &SimEnvObserve)->f32{

        // 先取出该函数所需要的cpu
        let fn_cpu_use = env.func(fnid).cpu;

        let cpu_local = node_resc_state.cpu_limit / (node_resc_state.all_task_cnt + 1.0);
    
        return fn_cpu_use / cpu_local;
    }

    // 获得数据，维护 bp 数组
    fn get_bplist_node_status(&self, fnid: FnId, env: &SimEnvObserve) -> BpListUpdateNodes{
        
        let binpack = self.binpack_map.get(&fnid).unwrap();

        assert!(binpack.len() != 0 && self.latest_nodes.get(&fnid).unwrap().len() != 0);

        let mut avg_cpu_starve_degree = 0.0;
        let mut avg_mem_use_rate = 0.0;
        let mut max_starve_nodeid_inbp = 9999;
        let mut max_score_nodeid_outbp = 9999;

        let mut max_starve_degree = 1.0;
        let mut max_resource_score = 0.0;

        // 遍历该函数的可执行节点集合
        for nodeid in self.latest_nodes.get(&fnid).unwrap().iter() {

            // 取出当前节点的资源使用情况
            let iter_node_resc_state = self.nodes_resc_state.get(&nodeid).unwrap();

            // 找到binpack内饥饿程度最高的节点、binpack外资源得分最高的节点,同时计算bp内平均mem利用率、cpu饥饿程度
            if binpack.contains(nodeid){
                // 统计binpack内节点的平均mem利用率
                avg_mem_use_rate +=
                    iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;

                let cpu_starve_degree = self.cal_cpu_starve_degree(iter_node_resc_state, fnid, env);

                // 统计bp数组内的节点的平均cpu饥饿程度
                avg_cpu_starve_degree += cpu_starve_degree;

                // 计算binpack内，针对于该函数容器的饥饿程度最高的节点
                if max_starve_nodeid_inbp == 9999{
                    max_starve_nodeid_inbp = *nodeid;
                    max_starve_degree = cpu_starve_degree;
                }
                else {
                    // 取出目前最饥饿节点的资源状态
                    if cpu_starve_degree > max_starve_degree {
                        max_starve_nodeid_inbp = *nodeid;
                        max_starve_degree = cpu_starve_degree;
                    }
                }
            }
            // binpack外资源得分最高的节点。资源得分 = 1.0 / (1.0 + 任务数 + 内存使用率)
            else {
                if max_score_nodeid_outbp == 9999{
                    max_score_nodeid_outbp = *nodeid;
                    max_resource_score = iter_node_resc_state.resource_score;
                }
                else {
                    // 取出目前得分最高节点的资源状态
                    if iter_node_resc_state.resource_score > max_resource_score {
                        max_score_nodeid_outbp = *nodeid;
                        max_resource_score = iter_node_resc_state.resource_score;
                    }
                }
            }
        }

        // 计算平均
        avg_mem_use_rate /= binpack.len() as f32;
        avg_cpu_starve_degree /= binpack.len() as f32;

        // 把平均cpu饥饿程度打在日志上
        log::info!("func_id: {}, avg_cpu_starve_degree: {}, avg_mem_use_rate: {}", fnid, avg_cpu_starve_degree, avg_mem_use_rate);
        log::info!("func_id: {}, max_starve_nodeid_inbp: {}, max_score_nodeid_outbp: {}", fnid, max_starve_nodeid_inbp, max_score_nodeid_outbp);

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

    // 调度之后的资源情况需要更新
    fn schedule_one_req_fns(
        &mut self, 
        env: &SimEnvObserve,
        mech: &MechanismImpl, 
        req: &mut Request, 
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 收集该请求中所有可以调度的函数
        let schedule_able_fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::PreAllSched,
        );

        let mech_metric = || env.help().mech_metric_mut();
        let scale_up_exec = mech.scale_up_exec();

        for &fnid in &schedule_able_fns {

            let mut target_cnt = mech.scale_num(fnid);
            if target_cnt == 0 {
                target_cnt = 1;
            }

            let fn_scale_up_cmds = scale_up_exec.exec_scale_up(
                target_cnt,
                fnid,
                env,
                cmd_distributor
            );
            for cmd in fn_scale_up_cmds.iter() {
                self.latest_nodes.get_mut(&fnid).unwrap().insert(cmd.nid);
                self.binpack_map.get_mut(&fnid).unwrap().insert(cmd.nid);
            }            


            if self.binpack_map.get(&fnid).unwrap().len() == 0 && fn_scale_up_cmds.len() != 0 {
                panic!("fnid:{}, last_nodes_len:{}", fnid, self.latest_nodes.get(&fnid).unwrap().len());
            }

            // 找到调度节点
            let sche_nodeid = self.find_schedule_nodeid(fnid, env);

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
                sche_node_resc_state.all_task_cnt += 1.0;
                sche_node_resc_state.resource_score = 1.0 / (1.0 + sche_node_resc_state.all_task_cnt + sche_node_resc_state.mem_used / sche_node_resc_state.mem_limit);

                // 只有当机制触发的时候才扩容binpack数组
                if self.mech_impl_sign.get(&fnid).unwrap() == &true {
                    // 计算该函数binpack数组的资源情况
                    let bplist_resource_status = self.get_bplist_node_status(fnid, env);

                    // 如果binpack内平均资源利用率大于所设阈值，则将含有该函数对应容器快照的目前空余资源量最多的节点加入该binpack数组
                    if (bplist_resource_status.avg_mem_use_rate > MEM_THRESHOLD_TO_ADD || bplist_resource_status.avg_cpu_starve_degree > CPU_THRESHOLD_TO_ADD) && bplist_resource_status.join_nodeid_outbp != 9999 {
                        self.update_bplist(fnid, bplist_resource_status.join_nodeid_outbp, "add", env);
                    }
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

            // 任务数量
            let all_task_cnt = node.all_task_cnt() as f32;

            // 资源得分，资源得分 = 1.0 / (1.0 + 任务数 + 内存使用率)
            let resource_score = 1.0 / (1.0 + all_task_cnt + mem_used / mem_limit);
            self.nodes_resc_state.insert(node.node_id(), 
                NodeRescState
                {
                    mem_used, mem_limit, cpu_limit, resource_score, all_task_cnt,
                }
            );
        }

        self.need_schedule_fn.clear();
        // 找到这一帧需要调度的函数
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            let schedule_able_fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::PreAllSched,
            );
            for fnid in schedule_able_fns.iter() {
                self.need_schedule_fn.insert(*fnid);
            }
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
            // let mut nodes = HashSet::new();
            // env.fn_containers_for_each(func.fn_id, |container| {
            //     nodes.insert(container.node_id);
            // });
            let mut nodes = env
                                            .core().fn_2_nodes()
                                            .get(&func.fn_id)
                                            .map(|v| { v.clone() })
                                            .unwrap_or(HashSet::new());


            // 根据 scaler 得出的容器数量进行扩缩容--------------------------------------------------------------------
            let mut scale_down_sign = false;
            let target = mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target > cur && self.need_schedule_fn.contains(&func.fn_id){
                let up_cmd = mech.scale_up_exec().exec_scale_up(
                    target, 
                    func.fn_id, env, 
                    cmd_distributor
                );

                // 实时更新函数的节点情况
                for cmd in up_cmd.iter() {
                    nodes.insert(cmd.nid);
                    self.binpack_map.get_mut(&func.fn_id).unwrap().insert(cmd.nid);
                }
            }
            // 如果需要缩容
            else if target < cur && (cur > 1 || !self.need_schedule_fn.contains(&func.fn_id)) {
                // 标记可以开始bp机制
                if self.mech_impl_sign.get(&func.fn_id).unwrap() == &false {
                    log::info!("fn_id: {}, 在第 {} 帧触发机制", func.fn_id, env.core().current_frame());
                }
                // MARK 注释下面这行，可以关闭bp机制，此时变成贪婪调度
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

                // 如果有缩容命令，则标记 scale_down_sign 为 true
                if down_cmd.len() != 0 {
                    scale_down_sign = true;
                }
            }
            // ---------------------------------------------------------------------------------------------------------------------------

            // 机制没有触发，则该函数的bp数组就是nodes
            if !*self.mech_impl_sign.get(&func.fn_id).unwrap(){
                // log::info!("fn_id: {}, frame:{}, 机制没有触发", func.fn_id, env.core().current_frame());
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
                        log::info!("fnid:{}, 节点{}被意外缩容, 移除后binpack_len:{}", func.fn_id, nodeid, self.binpack_map.get(&func.fn_id).unwrap().len());
                        log::info!("nodes.len():{}", nodes.len());
                    }
                }

                let binpack = self.binpack_map.get(&func.fn_id).unwrap();
                // 该函数没有可调度节点，表示该函数最近一直没有请求，直接跳过
                if nodes.len() == 0 {
                    self.latest_nodes.insert(func.fn_id, nodes.clone());
                    assert!(binpack.len() == 0);
                    continue;
                }

                // 重新拿一次，避免借用冲突
                let binpack = self.binpack_map.get(&func.fn_id).unwrap();

                // 如果扩缩容器没有缩容，那么遍历每个容器，对binpack数组外的容器进行超时缩容------------------------------------------
                if /* scale_down_sign == false */ true {
                    env.fn_containers_for_each(func.fn_id, |container| {
                    
                        // 对于不是binpack数组中的节点，进行超时缩容
                        if !binpack.contains(&container.node_id) {
                            
                            // 如果该容器最近50帧都是空闲则缩容
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

                // 到这里来的时候nodes集合一定不为空
                assert!(nodes.len() != 0);

                // 更新该函数的最新可调度节点集合
                self.latest_nodes.insert(func.fn_id, nodes.clone());

                // 对 binpack 数组进行维护----------------------------------------------------------------------------------------
                let binpack = self.binpack_map.get(&func.fn_id).unwrap();

                // 当binpack数组为空时，把所有节点都加进去
                if binpack.len() == 0 {
                    self.binpack_map.insert(func.fn_id, nodes.clone());
                    assert!(self.binpack_map.get(&func.fn_id).unwrap().len() != 0);
                }

                // 计算该函数binpack数组内的资源利用率，以及得出其内、其外的空闲资源最多的节点id
                let mut bplist_resource_status = self.get_bplist_node_status(func.fn_id, env);
                
                // 维护binpack数组
                while bplist_resource_status.avg_cpu_starve_degree < CPU_THRESHOLD_TO_REMOVE || bplist_resource_status.avg_mem_use_rate > MEM_THRESHOLD_TO_ADD || bplist_resource_status.avg_cpu_starve_degree > CPU_THRESHOLD_TO_ADD {
                    
                    let binpack = self.binpack_map.get(&func.fn_id).unwrap();

                    assert!(bplist_resource_status.avg_cpu_starve_degree != 0.0);
                    
                    // 退出循环逻辑
                    if bplist_resource_status.avg_cpu_starve_degree < CPU_THRESHOLD_TO_REMOVE && binpack.len() == 1{
                        break;
                    }
                    if (bplist_resource_status.avg_mem_use_rate > MEM_THRESHOLD_TO_ADD || bplist_resource_status.avg_cpu_starve_degree > CPU_THRESHOLD_TO_ADD) && bplist_resource_status.join_nodeid_outbp == 9999 {
                        break;
                    }

                    // 如果binpack内平均cpu饥饿程度小于0.5，则逐出数组中饥饿程度最高的节点
                    if bplist_resource_status.avg_cpu_starve_degree < CPU_THRESHOLD_TO_REMOVE && bplist_resource_status.expel_nodeid_inbp != 9999 {
                        // log::info!("fnid:{}, avg_cpu_starve_degree:{}, expel_nodeid_inbp:{}", func.fn_id, bplist_resource_status.avg_cpu_starve_degree, bplist_resource_status.expel_nodeid_inbp);
                        self.update_bplist(func.fn_id, bplist_resource_status.expel_nodeid_inbp, "remove", env);
                        // log::info!("fnid:{}, 节点{}被逐出binpack, 剩余binpack_len:{}", func.fn_id, bplist_resource_status.expel_nodeid_inbp, self.binpack_map.get(&func.fn_id).unwrap().len());
                        break;
                    }
                    // 如果binpack内平均mem利用率大于80%或平均饥饿程度大于0.9，则将bp数组外资源得分最高的节点加入该binpack数组
                    else if (bplist_resource_status.avg_mem_use_rate > MEM_THRESHOLD_TO_ADD || bplist_resource_status.avg_cpu_starve_degree > CPU_THRESHOLD_TO_ADD) && bplist_resource_status.join_nodeid_outbp != 9999 {
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
            self.schedule_one_req_fns(env, mech, req, cmd_distributor);
        }

    }
}

