
/*
算法流程：https://fvd360f8oos.feishu.cn/docx/QbXqdmszVo4lOsxvveacdDOMn6c?from=from_copylink
*/

use std::borrow::Borrow;
use std::cell::{RefCell};
use std::collections::{HashMap, VecDeque};


use crate::fn_dag::EnvFnExt;
use crate::mechanism::SimEnvObserve;
use crate::node::EnvNodeExt;
use crate::with_env_sub::{WithEnvCore, WithEnvHelp};
use crate::{
    actions::ESActionWrapper, fn_dag::FnId,
    CONTAINER_BASIC_MEM,
};

use super::{
    down_filter::{CarefulScaleDownFilter, ScaleFilter},
    ScaleNum,
};

// 定义Hawkes过程参数类型
struct HawkesParams {
    mu: f64,    // 在无历史调用影响下的平均调用率
    alpha: f64, // 单个触发事件的影响力
    beta: f64,  // 衰减率，表示过去调用对当前调用率影响的衰减速度
}
impl HawkesParams {
    fn new() -> HawkesParams {
        // MARK 以下三个参数初始值可以更改
        HawkesParams {
            mu: 0.1,
            alpha: 0.2,
            beta: 1.25,
        }
    }
}

struct FrameCountTemp {
    frame: usize,
    count: usize,
    temp: f64,
}
impl FrameCountTemp {
    fn new(frame: usize, count: usize, temp: f64) -> FrameCountTemp {
        FrameCountTemp { frame, count, temp }
    }
}

// 函数调用温度感知调度器
pub struct TempScaleNum {
    // 指定函数的 Hawkes 过程的的相关参数
    fn_params: HashMap<FnId, RefCell<HawkesParams>>,

    // 函数的历史调用记录，函数 - 帧数_温度 的映射，用于计算温度
    fn_call_history: HashMap<FnId, RefCell<VecDeque<FrameCountTemp>>>,

    // 函数的历史温度记录，函数 - 帧数_温度 的映射，帧数连续，只用于计算阈值和判断扩缩容
    fn_temp_history: HashMap<FnId, RefCell<VecDeque<FrameCountTemp>>>,

    // 函数根据温度决定扩缩容的帧数记录
    fn_temp_scale_sign: HashMap<FnId, usize>,

    // 历史 调用_温度 记录窗口长度
    call_history_window_len: usize,

    // 函数历史温度记录的窗口长度
    temp_history_window_len: usize,

    // 温度感知窗口长度
    temp_care_window_len: usize,

    // 控制缩容时候的容器过滤策略，目前用的是 CarefulScaleDownFilter
    pub scale_down_policy: Box<dyn ScaleFilter + Send>,

    // 记录扩缩容器决策扩容、缩容次数
    decide_to_up_count: usize,
    resource_decide_to_up_count: usize,
    decide_to_down_count: usize,
}


impl TempScaleNum {
    pub fn new() -> Self {
        // log::info!("创建了一个 TempScaleNum 实例");

        Self {
            fn_params: HashMap::new(),
            fn_call_history: HashMap::new(),
            fn_temp_history: HashMap::new(),
            fn_temp_scale_sign: HashMap::new(),

            call_history_window_len: 50,
            temp_history_window_len: 50,
            temp_care_window_len: 10,

            scale_down_policy: Box::new(CarefulScaleDownFilter::new()),

            decide_to_up_count: 0,
            resource_decide_to_up_count: 0,
            decide_to_down_count: 0,
        }
    }

    // 计算指定帧数下，指定函数的温度值
    fn compute_fn_temperature(&self, fnid: FnId, calculate_frame: usize, fn_count: usize) -> f64 {
        // 取出参数
        let alpha = self.fn_params.get(&fnid).unwrap().borrow().alpha;
        let beta = self.fn_params.get(&fnid).unwrap().borrow().beta;
        let mu = self.fn_params.get(&fnid).unwrap().borrow().mu;

        // 温度初始化
        let mut temp = mu;

        // 取出函数的调用记录
        if let Some(call_records) = self.fn_call_history.get(&fnid) {

            // 根据 Hawkes 公式计算温度
            for frame_count_temp in call_records.borrow().iter() {

                // 只能计算指定帧数以前的调用记录
                if calculate_frame < frame_count_temp.frame {
                    break;
                }

                temp += frame_count_temp.count as f64
                    * alpha
                    * (-beta * (calculate_frame - frame_count_temp.frame) as f64).exp();
            }
        }

        // 当前帧的调用还没有被记录，所以另外计算
        temp += fn_count as f64 * alpha * 1.0;

        // 取温度的对数
        temp.ln()
    }

    // 计算指定函数的历史温度平均值以及温度变化感知阈值（历史温度的标准差）
    fn compute_fn_temp_trans_threshold(&self, fnid: FnId) -> (f64, f64) {

        if let Some(history_ref) = self.fn_temp_history.get(&fnid) {
            // 取出温度历史记录的不可变借用
            let history = history_ref.borrow();

            // 取出所有温度值
            let mut samples: VecDeque<f64> = VecDeque::new();
            for frame_count_temp in history.iter() {
                samples.push_back(frame_count_temp.temp);
            }

            // 释放RefCell的borrow
            drop(history);

            // MARK 最近的感知窗口长度的帧不计算在内
            for i in 0..self.temp_care_window_len {
                samples.pop_back();
            }

            // 求平均数
            let mean = samples.iter().sum::<f64>() / samples.len() as f64;

            // 求方差
            let variance =
                samples.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;

            // 求标准差
            let std_dev = variance.sqrt();

            // 将标准差作为临界值，可以根据实验情况更改
            (mean, std_dev)
        } else {
            // 返回合适的默认值或者处理方式
            (0.0, f64::MAX)
        }
    }
}

// 实现核心 trait
impl ScaleNum for TempScaleNum {
    // 设置指定函数的目标容器数量
    fn scale_for_fn(&mut self, env: &SimEnvObserve, fnid: FnId, action: &ESActionWrapper) -> usize {

        // 初始化======================================================================================
        // 获得当前帧数
        let current_frame = env.core().current_frame();

        // 如果该函数是第一次进行扩缩容操作，则初始化参数、调用记录、历史记录
        self.fn_params
            .entry(fnid)
            .or_insert_with(|| RefCell::new(HawkesParams::new()));
        self.fn_call_history
            .entry(fnid)
            .or_insert_with(|| RefCell::new(VecDeque::new()));
        self.fn_temp_history
            .entry(fnid)
            .or_insert_with(|| RefCell::new(VecDeque::new()));
        self.fn_temp_scale_sign.entry(fnid).or_insert_with(|| 0);

        // ============================================================================================

        // 更新函数的历史调用记录------------------------------------------------------------
        // 首先统计当前函数在这一帧的到达数量
        let mut fn_count = 0;

        // 取出所有的请求的不可变借用
        let requests = env.core().requests();

        // 遍历所有请求，只看当前帧到达的请求
        for (_, req) in requests
            .iter()
            .filter(|(_, req)| req.begin_frame == current_frame)
        {
            // 拿到该请求对应的DAG
            let mut walker = env.dag(req.dag_i).new_dag_walker();
            // 遍历DAG里面的所有图节点
            while let Some(fngid) = walker.next(&env.dag(req.dag_i).dag_inner) {
                // 得到该图节点对应的函数
                let fnid_in_dag = env.dag_inner(req.dag_i)[fngid];
                // 累加当前函数到达的次数
                if fnid_in_dag == fnid {
                    fn_count += 1;
                }
            }
        }

        let current_temp = self.compute_fn_temperature(fnid, current_frame, fn_count);
        // 只有fn_count > 0 才更新调用记录
        if fn_count > 0 {
            // 获取当前帧数的调用温度，此时当前帧的调用记录还没有被记录，所以需要额外传输一个参数
            
            // 更新该函数的历史调用记录
            let mut call_history = self.fn_call_history.get(&fnid).unwrap().borrow_mut();
            call_history.push_back(FrameCountTemp::new(current_frame, fn_count, current_temp));

            // 控制滑动窗口长度
            if call_history.len() > self.call_history_window_len {
                call_history.pop_front();
            }
        }
        // ----------------------------------------------------------------------------------------

        // MARK 为了避免借用冲突，必须放在更新历史温度记录前面
        // 标记温度策略是否决定了扩缩容
        let mut scale_sign = false;

        // 当前容器数量
        let cur_container_cnt = env.fn_container_cnt(fnid);

        // 至少要20帧后才用温度计算扩缩容，不然样本数不够
        let temp_history_min_len = 20;

        // 如果记录表长度小于 10，则不进行温度决策，也不需要计算阈值,
        let mut threshold = f64::MAX;
        let mut temp_history_mean = 0.0;
        if self.fn_temp_history.get(&fnid).unwrap().borrow().len() >= temp_history_min_len {
            // 计算以前的温度的正常波动情况
            (temp_history_mean, threshold) = self.compute_fn_temp_trans_threshold(fnid);
        }

        // 更新函数的历史温度记录---------------------------------------------------------------
        // 拿到历史温度记录的可变借用
        let mut temp_recent = self.fn_temp_history.get(&fnid).unwrap().borrow_mut();

        // 插入到函数的历史温度记录
        temp_recent.push_back(FrameCountTemp::new(current_frame, fn_count, current_temp));

        // 控制滑动窗口长度
        if temp_recent.len() > self.temp_history_window_len {
            temp_recent.pop_front();
        }
        // ----------------------------------------------------------------------------------------

        // TODO 根据历史温度记录表来决定是否扩缩容以及计算目标容器数量----------------------------------
        // 初始化目标容器数量
        let mut desired_container_cnt = cur_container_cnt;

        // 如果记录表长度小于 10，或者最近10帧内使用过温度进行扩缩容，则不进行温度决策
        if temp_recent.len() >= temp_history_min_len
            && current_frame - self.fn_temp_scale_sign.get(&fnid).unwrap()
                > self.temp_care_window_len
        {
            // 新建一个扩缩容关心温度变化记录表
            let mut temp_care_records: VecDeque<f64> = VecDeque::new();

            // 从队尾往队头遍历（队尾的记录是最新的）插入temp_care_records
            for frame_count_temp in temp_recent.iter().rev() {
                temp_care_records.push_front(frame_count_temp.temp);

                // 控制记录表长度为窗口长度
                if temp_care_records.len() == self.temp_care_window_len {
                    break;
                }
            }

            // 得到扩缩容关心温度变化记录表的平均值
            let temp_care_mean =
                temp_care_records.iter().sum::<f64>() / temp_care_records.len() as f64;

            // 计算温度增量
            let temp_change = temp_care_mean - temp_history_mean;

            // 如果温度增量的绝对值大于温度变化感知阈值，则进行扩缩容决策
            if temp_change.abs() > threshold {

                // 计算容器数量的增率
                let container_inc_rate = temp_change.abs() / threshold;

                // 统计目前已有的函数实例数量
                let mut fn_instance_cnt = 0;

                // 统计目前可分配的实例数量
                let mut idle_fn_instance_cnt = 0;

                // 取出属于该函数的所有容器快照
                env.fn_containers_for_each(fnid, |container| {
                    // 创建一个该函数的实例需要的内存、该容器所在的节点
                    let fn_mem = env.core().fns().get(fnid).unwrap().mem;
                    let node = env.node(container.node_id);

                    // 累加所有容器上的已有的函数实例数量得到总的函数实例数量
                    fn_instance_cnt +=
                        ((container.last_frame_mem - CONTAINER_BASIC_MEM) / fn_mem) as i32;

                    // 累加容器节点上空闲可分配的实例数量，但是这些可分配的内存是公用的，每个函数平分剩余的空闲内存
                    idle_fn_instance_cnt += ((node.rsc_limit.mem - node.last_frame_mem) / 
                        (fn_mem * node.fn_containers.borrow().len() as f32)).floor() as i32;
                });

                // 决策扩容
                if temp_change > 0.0 {
                    // 根据温度增量计算容器数量的增量
                    let container_change =
                        (fn_instance_cnt as f64 * (container_inc_rate - 1.0)).ceil() as i32;

                    // 如果所需要的实例数量大于空闲的实例数量，则进行扩容
                    if (container_change >= idle_fn_instance_cnt) {
                        // 标记这一帧用温度策略决定扩缩容
                        scale_sign = true;

                        // 更新温度扩缩容记录
                        self.fn_temp_scale_sign.insert(fnid, current_frame);

                        self.decide_to_up_count += 1;

                        // 增加一个容器快照。其实应该严格按照应增加的实例数量来计算具体增加几个快照够，但是又涉及到在哪里进行扩容并计算数量的问题，该系统中实现很麻烦
                        desired_container_cnt += 1;
                    }
                }
                // 决策缩容
                else if desired_container_cnt > 1{
                    // 标记这一帧用温度策略决定扩缩容
                    scale_sign = true;

                    // 更新温度扩缩容记录
                    self.fn_temp_scale_sign.insert(fnid, current_frame);

                    // 记录缩容次数
                    self.decide_to_down_count += 1;

                    // 减少一个容器快照
                    desired_container_cnt -= 1;
                }
            }
        }
        // ----------------------------------------------------------------------------------------

        // 设置机制来处理 温度感知器没反应，但是函数在持续缓慢升温/降温的情况-----------------------------------------------------
        // 获取当前函数的所有容器，计算平均cpu、mem利用率
        if !scale_sign && cur_container_cnt != 0 {
            let mut container_avg_cpu_util = 0.0;
            let mut container_avg_mem_util = 0.0;

            env.fn_containers_for_each(fnid, |container| {
                // 统计cpu、mem情况
                container_avg_cpu_util += container.cpu_use_rate();

                container_avg_mem_util += container.last_frame_mem
                    / (env.node(container.node_id).left_mem() + container.last_frame_mem);

            });
            // 计算平均
            container_avg_mem_util /= cur_container_cnt as f32;
            container_avg_cpu_util /= cur_container_cnt as f32;

            // 如果有一个大于80%，则进行扩容
            if container_avg_mem_util > 0.8 || container_avg_cpu_util > 0.8 {
                self.resource_decide_to_up_count += 1;
                desired_container_cnt += 1;
            }
        }
        // ----------------------------------------------------------------------------------------

        // 每个函数至少要有一个容器
        if desired_container_cnt == 0 {
            desired_container_cnt = 1;
        }

        // log::info!("函数:{}, 在第{}帧的目标容器数量为：{}.scale_for_fn()结束", fnid, current_frame, desired_container_cnt);

        // log::info!("扩缩容器决策升温 {} 次", self.decide_to_up_count);
        // log::info!("扩缩容器决策降温 {} 次", self.decide_to_down_count);
        // log::info!("mem决策升温 {} 次", self.mem_decide_to_up_count);

        desired_container_cnt
    }
}
