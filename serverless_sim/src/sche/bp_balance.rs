use std::{borrow::Borrow, collections::{HashMap, HashSet}, vec};


use crate::{
    fn_dag::{EnvFnExt, FnId}, mechanism::{DownCmd, MechanismImpl, ScheCmd, SimEnvObserve, UpCmd}, mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes}, node::{EnvNodeExt, NodeId}, request::Request, sim_run::{schedule_helper, Scheduler}, with_env_sub::{WithEnvCore, WithEnvHelp}
};

struct bplistStatues{
    avg_cpu_use_rate: f32,
    avg_mem_use_rate: f32,
    max_idle_nodeid_inbp: usize,
    max_idle_nodeid_unbp: usize,
}

impl bplistStatues {
    pub fn new(avg_cpu_use_rate: f32, avg_mem_use_rate: f32, max_idle_nodeid_inbp: usize, max_idle_nodeid_unbp: usize) -> Self {
        Self {
            avg_cpu_use_rate,
            avg_mem_use_rate,
            max_idle_nodeid_inbp,
            max_idle_nodeid_unbp,
        }
    }
}

struct NodeRescState {
    mem_used: f32,
    mem_limit: f32,
}

impl NodeRescState {
    pub fn new(mem_used: f32, mem_limit: f32) -> Self {
        Self {
            mem_used,
            mem_limit,
        }
    }
}

pub struct BpBalanceScheduler {
    // 每个函数的binpack节点集合
    fn_binpack_map: HashMap<FnId, HashSet<NodeId>>,

    // 每个函数的最新节点集合
    fn_latest_nodes: HashMap<FnId, HashSet<NodeId>>,

    // MARK 这个还没初始化的
    // 每个节点的资源使用情况，实时更新
    nodes_resc_state: HashMap<NodeId, NodeRescState>,
}

impl BpBalanceScheduler {
    pub fn new() -> Self {
        Self {
            fn_binpack_map: HashMap::new(),
            fn_latest_nodes: HashMap::new(),
            nodes_resc_state: HashMap::new(),
        }
    }

    // 找出binpack数组中空闲内存率最高的节点
    fn find_max_idle_nodeid(&self, fnid: FnId, env: &SimEnvObserve) -> usize{
        // BUG 当binpack为空时，会返回9999，导致调度失败
        let mut max_idle_nodeid: usize = 9999;
        let binpack = self.fn_binpack_map.get(&fnid).unwrap();

        for nodeid in binpack {

            if max_idle_nodeid == 9999 {
                max_idle_nodeid = *nodeid;
            }
            else {
                // 取出当前遍历节点的资源使用情况
                let iter_node_resc_state = self.nodes_resc_state.get(&nodeid).unwrap();
                // 取出目前最大空闲节点的资源状态
                let max_node_resc_state = self.nodes_resc_state.get(&max_idle_nodeid).unwrap();

                // 计算资源空闲率
                let this_node_idle = 1.0 - iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;
                let max_node_idle = 1.0 - max_node_resc_state.mem_used / max_node_resc_state.mem_limit;
                if this_node_idle > max_node_idle {
                    max_idle_nodeid = *nodeid;
                }
            }
        }
        max_idle_nodeid
    }


    // TODO 也要计算cpu，计算指定函数binpack数组内的资源利用率，以及得出其内、其外的空闲资源最多的节点id
    fn get_bplist_status(&self, fnid: FnId, env: &SimEnvObserve) -> bplistStatues{
        
        let binpack = self.fn_binpack_map.get(&fnid).unwrap();

        // binpack内节点的平均mem、cpu利用率
        let mut avg_mem_use_rate = 0.0;
        let mut avg_cpu_use_rate = 0.0;
        let mut bplist_have_container = 0;

        // binpack内或外最空闲的节点
        let mut max_idle_nodeid_inbp = 9999;
        let mut max_idle_nodeid_unbp = 9999;

        // 遍历该函数的可执行节点集合
        for nodeid in self.fn_latest_nodes.get(&fnid).unwrap().iter() {

            // 取出当前节点的资源使用情况
            let iter_node_resc_state = self.nodes_resc_state.get(&nodeid).unwrap();

            // binpack内最空闲的节点,同时计算平均mem、cpu利用率
            if binpack.contains(nodeid){
                // 统计binpack内节点的平均资源利用率
                avg_mem_use_rate +=
                    iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;

                // BUG 第一帧的时候，实际上还没有容器，这时候取cpu_use_rate会报错
                // 取出这个节点内该函数的容器的cpu利用率
                if env.node(*nodeid).container(fnid).is_some() {
                    avg_cpu_use_rate += env.node(*nodeid).container(fnid).unwrap().borrow().cpu_use_rate();
                    bplist_have_container += 1;
                }


                if max_idle_nodeid_inbp == 9999{
                    max_idle_nodeid_inbp = *nodeid;
                }
                else {
                    // 取出目前最大空闲节点的资源状态
                    let max_node_resc_state = self.nodes_resc_state.get(&max_idle_nodeid_inbp).unwrap();

                    // 计算资源空闲率
                    let iter_nodeid_idle = 1.0 - iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;
                    let max_nodeid_idle = 1.0 - max_node_resc_state.mem_used / max_node_resc_state.mem_limit;

                    if iter_nodeid_idle > max_nodeid_idle {
                        max_idle_nodeid_inbp = *nodeid;
                    }
                }
            }
            // binpack外最空闲的节点
            else {
                if max_idle_nodeid_unbp == 9999{
                    max_idle_nodeid_unbp = *nodeid;
                }
                else {
                    // 取出目前最大空闲节点的资源状态
                    let max_node_resc_state = self.nodes_resc_state.get(&max_idle_nodeid_unbp).unwrap();

                    let iter_nodeid_idle = 1.0 - iter_node_resc_state.mem_used / iter_node_resc_state.mem_limit;
                    let max_nodeid_idle = 1.0 - max_node_resc_state.mem_used / max_node_resc_state.mem_limit;
                    
                    if iter_nodeid_idle > max_nodeid_idle {
                        max_idle_nodeid_unbp = *nodeid
                    }
                }
            }


        }

        // 计算平均
        avg_mem_use_rate /= binpack.len() as f32;

        if bplist_have_container != 0 {
            avg_cpu_use_rate /= bplist_have_container as f32;
        }

        bplistStatues{
            avg_mem_use_rate,
            avg_cpu_use_rate,
            max_idle_nodeid_inbp,
            max_idle_nodeid_unbp
        }
    }
    
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
        // let mut sche_cmds = vec![];
        let mech_metric = || env.help().mech_metric_mut();

        for &fnid in &schedule_able_fns {
            // 找出该函数的binpack数组中空闲内存最多的节点
            let sche_nodeid = self.find_max_idle_nodeid(fnid, env);

            mech_metric().add_node_task_new_cnt(sche_nodeid);

            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    reqid: req.req_id,
                    fnid,
                    nid: sche_nodeid,
                    memlimit: None,
                }))
                .unwrap();

            // 更新node_resc_state中的节点容量
            self.nodes_resc_state.get_mut(&sche_nodeid).unwrap().mem_used += env.func(fnid).mem;

            // 计算该函数binpack数组内的资源利用率，以及得出其外的空闲资源最多的节点id
            let bplist_status = self.get_bplist_status(fnid, env);

            // 如果binpack内平均资源利用率大于所设阈值，则将含有该函数对应容器快照的目前空余资源量最多的节点加入该binpack数组
            if (bplist_status.avg_mem_use_rate > 0.8 || bplist_status.avg_cpu_use_rate > 0.9) && bplist_status.max_idle_nodeid_unbp != 9999{
                self.fn_binpack_map.get_mut(&fnid).unwrap().insert(bplist_status.max_idle_nodeid_unbp);
            }

        }
    }
}

impl Scheduler for BpBalanceScheduler {

    fn schedule_some(&mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,){

        let mut up_cmds = vec![];
        // let mut sche_cmds = vec![];
        let mut down_cmds = vec![];

        // 遍历每个节点，更新其资源使用情况
        for node in env.core().nodes().iter() {
            let mem_used = env.node(node.node_id()).last_frame_mem;
            let mem_limit = env.node(node.node_id()).rsc_limit.mem;
            self.nodes_resc_state.insert(node.node_id(), NodeRescState::new(mem_used, mem_limit));

        }

        // 遍历每个函数，为其获取扩缩容命令，维护一个binpack节点数组和一个可执行节点数组
        for func in env.core().fns().iter() {

            // 进行其他处理之前，先更新最新节点集合
            let mut nodes = HashSet::new();
            env.fn_containers_for_each(func.fn_id, |container| {
                nodes.insert(container.node_id);
            });

            // 根据 scaler 得出的容器数量进行扩容
            let mut target = mech.scale_num(func.fn_id);
            if target == 0 {
                target = 1;
            }
            let cur = env.fn_container_cnt(func.fn_id);
            // 如果当前容器数量少于目标数量，则进行扩容
            if target > cur {
                let up_cmd = mech.scale_up_exec().exec_scale_up(target, func.fn_id, env, cmd_distributor);
                up_cmds.extend(up_cmd.clone());

                // 实时更新函数的节点情况、新扩容的容器数量直接放入binpack数组中
                for cmd in up_cmd.iter() {
                    nodes.insert(cmd.nid);
                }
            }

            // MARK 试一下所有节点都放binpack数组中的情况
            // 如果函数有binpack数组，则再进行超时缩容，并维护该 binpack 数组
            if self.fn_binpack_map.contains_key(&func.fn_id){
                // 获得节点总数和binpack数组
                let binpack = self.fn_binpack_map.get(&func.fn_id).unwrap();

                // 遍历每个容器，对binpack数组外的容器进行超时缩容
                env.fn_containers_for_each(func.fn_id, |container| {
                    
                    // TODO 添加CPU资源监控后这里应该修改逻辑，不需要节点总数大于1.5倍才缩容
                    // 如果当前节点总数比binpack数组多，对于不是binpack数组中的节点，进行超时缩容
                    if nodes.len() > binpack.len() + 1 && !binpack.contains(&container.node_id) {
                    // if nodes.len() > (binpack.len() as f32 * 1.5).ceil() as usize && !binpack.contains(&container.node_id) {
                        
                        // 如果最近20帧都是空闲，且没有请求
                        if container.recent_frame_is_idle(20) && container.req_fn_state.len() == 0  {
                            down_cmds.push(
                                DownCmd {
                                nid: container.node_id,
                                fnid: func.fn_id}
                            );

                            nodes.remove(&container.node_id);
                        }
                    }
                    
                });
                // ----------------------超时缩容完成

                // 更新最新节点集合
                self.fn_latest_nodes.insert(func.fn_id, nodes.clone());

                // ----------------------对 binpack 数组进行维护
                let binpack = self.fn_binpack_map.get_mut(&func.fn_id).unwrap();

                // 清理一下binpack数组中被意外缩容的节点，一般不会出现这个情况
                let binpack_nodeids = binpack.clone();
                for nodeid in binpack_nodeids.iter() {
                    if !nodes.contains(nodeid) {
                        binpack.remove(nodeid);
                    }
                }

                // 当binpack数组为空时（正常运行的话不会是空的），把所有节点都加进去
                if binpack.len() == 0 {
                    self.fn_binpack_map.insert(func.fn_id, nodes.clone());
                }

                // 计算该函数binpack数组内的资源利用率，以及得出其内、其外的空闲资源最多的节点id
                let mut bplist_status = self.get_bplist_status(func.fn_id, env);
                
                // TODO 更改维护数组的逻辑，增加对cpu利用率的调控
                // 维护binpack数组，直到其数组内资源利用率在50%到80%之间
                while bplist_status.avg_mem_use_rate < 0.5 && (bplist_status.avg_mem_use_rate > 0.8 || bplist_status.avg_cpu_use_rate > 0.9) {

                    let binpack = self.fn_binpack_map.get_mut(&func.fn_id).unwrap();

                    // 空数组或者已经没有外部可添加节点了
                    if binpack.len() == 0 || bplist_status.max_idle_nodeid_unbp == 9999 {
                        break;
                    }

                    // 如果binpack内平均资源利用率小于50%，则逐出数组中资源利用率最低的元素，但是至少要有一个节点
                    if bplist_status.avg_mem_use_rate < 0.5 && bplist_status.max_idle_nodeid_inbp != 9999 && binpack.len() > 1{

                        // 预计逐出后的平均资源利用率
                        let mut new_mem_util = 0.0;
                        let mut new_cpu_util = 0.0;
                        for nodeid in binpack.iter() {
                            if *nodeid != bplist_status.max_idle_nodeid_inbp {
                                let node_resc_state = self.nodes_resc_state.get(&nodeid).unwrap();
                                new_mem_util += node_resc_state.mem_used / node_resc_state.mem_limit;
                                new_cpu_util += env.node(*nodeid).container(func.fn_id).unwrap().borrow().cpu_use_rate();
                            }
                        }
                        // 计算逐出后的平均利用率
                        new_mem_util /= (binpack.len() - 1) as f32;
                        new_cpu_util /= (binpack.len() - 1) as f32;

                        // 确保逐出后平均利用率依然在阈值之内
                        if new_mem_util < 0.8 && new_cpu_util < 0.9 {
                            binpack.remove(&bplist_status.max_idle_nodeid_inbp); 
                        }

                    }
                    // 如果binpack内平均资源利用率大于80%，则将含有该函数对应容器快照的目前空余资源量最多的节点加入该binpack数组
                    else if (bplist_status.avg_mem_use_rate > 0.8 || bplist_status.avg_cpu_use_rate > 0.9) && bplist_status.max_idle_nodeid_unbp != 9999 {
                        binpack.insert(bplist_status.max_idle_nodeid_unbp);
                    }

                    // 再次计算
                    bplist_status = self.get_bplist_status(func.fn_id, env);
                }
                
                // 为了防止借用冲突
                let binpack = self.fn_binpack_map.get_mut(&func.fn_id).unwrap(); 

                log::info!("fnid:{}, binpack_len:{}, latest_nodes_len:{}", func.fn_id, binpack.len(), self.fn_latest_nodes.get(&func.fn_id).unwrap().len());

            }
            // 如果函数没有binpack数组，则初始化，把该函数的所有可执行节点加入该数组
            else{
                // 更新最新节点集合
                self.fn_latest_nodes.insert(func.fn_id, nodes.clone());
                self.fn_binpack_map.insert(func.fn_id, nodes.clone());
            }
            
            // self.fn_latest_nodes.insert(func.fn_id, nodes.clone());
            // self.fn_binpack_map.insert(func.fn_id, nodes.clone());

            // // 每20帧，等待100ms。看长度情况
            // if *env.core.current_frame() % 10 == 0 {
            //     let a_millis = 50;
            //     let wait_duration = Duration::from_millis(a_millis);
            //     // 让当前线程暂停指定的持续时间
            //     thread::sleep(wait_duration);
            // }
        }

        // 遍历调度每个请求
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            self.schedule_one_req_fns(env, req, cmd_distributor);
        }

        // (up_cmds, sche_cmds, down_cmds)
    }
}