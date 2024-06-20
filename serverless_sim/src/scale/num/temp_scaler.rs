/*
算法流程：https://fvd360f8oos.feishu.cn/docx/QbXqdmszVo4lOsxvveacdDOMn6c?from=from_copylink
*/

use crate::fn_dag::EnvFnExt;
use crate::mechanism::SimEnvObserve;
use crate::node::EnvNodeExt;

use crate::with_env_sub::{WithEnvCore, WithEnvHelp};
use crate::{actions::ESActionWrapper, fn_dag::FnId, CONTAINER_BASIC_MEM};

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

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
        }
    }

    // 计算指定帧数下，指定函数的温度值
    fn compute_fn_temperature(&self, fnid: FnId, calculate_frame: usize, fn_count: usize) -> f64 {
        // log::info!("计算函数:{}，在第{}帧的温度值",fnid, calculate_frame);

        // 取出参数
        // log::info!("获取当前Hawkes过程的参数");
        let alpha = self.fn_params.get(&fnid).unwrap().borrow().alpha;
        let beta = self.fn_params.get(&fnid).unwrap().borrow().beta;
        let mu = self.fn_params.get(&fnid).unwrap().borrow().mu;

        // 温度初始化
        let mut temp = mu;
        // log::info!("温度初始化");

        // 取出函数的调用记录
        if let Some(call_records) = self.fn_call_history.get(&fnid) {
            // log::info!("取出函数的调用记录");

            // 根据 Hawkes 公式计算温度
            for frame_count_temp in call_records.borrow().iter() {
                // log::info!("根据 Hawkes 公式计算温度");

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

        // log::info!("温度值计算完成，函数:{}，在第{}帧的温度为: {}",fnid, calculate_frame, temp);

        // 取温度的对数
        temp.ln()
    }

    // 计算指定函数的历史温度平均值以及温度变化感知阈值（历史温度的标准差）
    fn compute_fn_temp_trans_threshold(&self, fnid: FnId, _env: &SimEnvObserve) -> (f64, f64) {
        // log::info!("计算函数:{}，在第{}帧的温度感知变化阈值", fnid, env.core.current_frame());

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
            for _i in 0..self.temp_care_window_len {
                samples.pop_back();
            }

            // 求平均数
            let mean = samples.iter().sum::<f64>() / samples.len() as f64;

            // 求方差
            let variance =
                samples.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;

            // 求标准差
            let std_dev = variance.sqrt();

            // log::info!("阈值计算完成，函数:{}，在第{}帧的阈值为: {}",fnid, env.core.current_frame(), std_dev * 1.2);

            // update 将标准差的1.2倍作为临界值，可以根据实验情况更改
            (mean, std_dev)
        } else {
            // log::error!("未找到函数:{}的历史温度记录", fnid);
            // 返回合适的默认值或者处理方式
            (0.0, f64::MAX)
        }
    }
}

// 实现核心 trait
impl ScaleNum for TempScaleNum {
    // 设置指定函数的目标容器数量
    fn scale_for_fn(
        &mut self,
        env: &SimEnvObserve,
        fnid: FnId,
        _action: &ESActionWrapper,
    ) -> usize {
        // 初始化======================================================================================
        // 获得当前帧数
        let current_frame = env.core().current_frame();

        // log::info!("函数:{}, 在第{}帧的scale_for_fn()初始化", fnid, current_frame);

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

        // let mut params= self.fn_params.get(&fnid).unwrap().borrow_mut();
        // ============================================================================================

        // log::info!("初始化已完成，统计函数:{}, 在第{}帧的请求次数", fnid, current_frame);

        // 统计当前函数在这一帧的到达数量--------------------------------------------------------
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
        // ----------------------------------------------------------------------------------------

        // log::info!("函数:{}, 在第{}帧的到达次数为：{}，准备历史调用记录", fnid, current_frame, fn_count);

        // 更新函数的历史调用记录--------------------------------------------------------------------
        // 只有fn_count > 0 才更新调用记录
        if fn_count > 0 {
            // 获取当前帧数的调用温度，此时当前帧的调用记录还没有被记录，所以需要额外传输一个参数
            let temp = self.compute_fn_temperature(fnid, current_frame, fn_count);

            // 拿到函数的历史调用记录的可变借用
            let mut call_history = self.fn_call_history.get(&fnid).unwrap().borrow_mut();

            call_history.push_back(FrameCountTemp::new(current_frame, fn_count, temp));

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
            // 计算的以前的温度的正常波动情况
            (temp_history_mean, threshold) = self.compute_fn_temp_trans_threshold(fnid, env);
        }

        // log::info!("函数:{}的历史调用记录更新完成，准备更新函数的历史温度记录", fnid);

        // 更新函数的历史温度记录---------------------------------------------------------------
        // 获取当前帧数的调用温度
        let temp = self.compute_fn_temperature(fnid, current_frame, fn_count);

        // 拿到历史温度记录的可变借用
        let mut temp_recent = self.fn_temp_history.get(&fnid).unwrap().borrow_mut();

        // 插入到函数的历史温度记录
        temp_recent.push_back(FrameCountTemp::new(current_frame, fn_count, temp));

        // 控制滑动窗口长度
        if temp_recent.len() > self.temp_history_window_len {
            temp_recent.pop_front();
        }
        // ----------------------------------------------------------------------------------------

        // log::info!("函数:{}的历史温度记录更新完成，计算目标容器数量", fnid);

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

            log::info!("温度增量:{}, 阈值:{}", temp_change.abs(), threshold);

            // 如果温度增量的绝对值大于温度变化感知阈值，则进行扩缩容决策
            if temp_change.abs() > threshold {
                // 更新温度扩缩容记录
                self.fn_temp_scale_sign.insert(fnid, current_frame);

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

                    // 累加所有容器上的已有的函数实例数量
                    fn_instance_cnt +=
                        ((container.last_frame_mem - CONTAINER_BASIC_MEM) / fn_mem) as i32;

                    // 累加容器节点上空闲可分配的实例数量，但是这些可分配的内存是公用的，所以不能全算上该函数的，要按照节点上剩余内存比例来计算
                    idle_fn_instance_cnt += ((node.rsc_limit.mem - node.last_frame_mem) / fn_mem
                        * (1.0 - (node.last_frame_mem / node.rsc_limit.mem)))
                        .floor() as i32;
                });

                // 根据温度增量计算容器数量的增量
                let container_change =
                    (fn_instance_cnt as f64 * (container_inc_rate - 1.0)).ceil() as usize;

                if container_change as f32 >= (idle_fn_instance_cnt as f32) * 0.8 {
                    // 标记这一帧用温度策略决定扩缩容
                    scale_sign = true;

                    // 增加一个容器快照。其实应该严格按照应增加的实例数量来计算具体增加几个快照够，但是又涉及到在哪里进行扩容并计算数量的问题，该系统中实现很麻烦
                    desired_container_cnt += 1;

                    log::info!("函数:{}, 在第{}帧根据temp进行扩缩容", fnid, current_frame);
                }
            }
        }
        // ----------------------------------------------------------------------------------------

        // 设置机制来处理 温度感知器没反应，但是函数在持续缓慢升温/降温的情况-----------------------------------------------------
        // 获取当前函数的所有容器，计算平均cpu、mem利用率
        if !scale_sign && cur_container_cnt != 0 {
            let mut container_avg_cpu_util = 0.0;
            let mut container_avg_mem_remain = 0.0;

            env.fn_containers_for_each(fnid, |container| {
                // 统计cpu、mem情况
                container_avg_cpu_util += container.cpu_use_rate();

                container_avg_mem_remain += container.last_frame_mem
                    / (env.node(container.node_id).left_mem() + container.last_frame_mem);

                // log::info!("mem rate: {}",c.last_frame_mem / (env.node(c.node_id).left_mem() + c.last_frame_mem));
            });
            // 计算平均
            container_avg_mem_remain /= cur_container_cnt as f32;
            container_avg_cpu_util /= cur_container_cnt as f32;

            log::info!(
                "平均mem:{}, 平均cpu使用量:{}",
                container_avg_mem_remain,
                container_avg_cpu_util
            );

            // 如果有一个大于80%，则进行扩容
            if container_avg_mem_remain > 0.8 || container_avg_cpu_util > 0.8 {
                desired_container_cnt += 1;
                log::info!(
                    "函数:{}, 在第{}帧根据cpu/mem机制扩缩容",
                    fnid,
                    current_frame
                );
            }
        }
        // ----------------------------------------------------------------------------------------

        // 如果该函数有未调度的，则至少要有一个容器
        if desired_container_cnt == 0 && env.help().mech_metric().fn_unsche_req_cnt(fnid) > 0 {
            desired_container_cnt = 1;
        }

        // log::info!("函数:{}, 在第{}帧的目标容器数量为：{}.scale_for_fn()结束", fnid, current_frame, desired_container_cnt);

        log::info!(
            "函数:{}, 上次使用温度扩缩容帧数为：{}",
            fnid,
            self.fn_temp_scale_sign.get(&fnid).unwrap()
        );

        desired_container_cnt
    }
}

/*
// MARK 最大似然估计法得出的参数不适合该场景
// 计算似然函数值
fn compute_fn_likelihood(&self, fnid: FnId, env: &SimEnv) -> f64 {
    let mut _likeli = 1.0;  // 前半部分累乘项
    let mut _hood = 0.0;    // 后半部分积分项，由于这里的时间是离散的，所以把积分近似为累加

    // 取出该函数的历史调用的温度记录。
    if let Some(call_history) = self.fn_call_history.get(&fnid) {

        // 计算累乘、累加
        for frame_and_temp in call_history.borrow().iter(){
            _likeli *= frame_and_temp.temp;
            _hood += frame_and_temp.temp;
        }
    }

    // 取累加的负数的对数
    _hood = (-_hood).exp();

    // 获得了似然函数
    let likelihood = _likeli * _hood;

    // 取对数
    likelihood.ln()

}

// MARK 最大似然估计法得出的参数不适合该场景
// 最大似然估计法，估计特定函数的Hawkes过程的参数, mu,alpha,beta
fn estimate_Hawkes_parameter(&mut self, fnid: FnId, env: &SimEnv){

    // 设置学习率
    let learning_rate = 0.2;

    // 迭代次数
    let mut iter_count = 0;

    loop {
    // 取出参数
    let mut mu = self.fn_params.get(&fnid).unwrap().borrow_mut().mu;
    let mut alpha = self.fn_params.get(&fnid).unwrap().borrow_mut().alpha;
    let mut beta = self.fn_params.get(&fnid).unwrap().borrow_mut().beta;


    // 计算偏导数
    let mut der_mu = 0.0;
    let mut der_alpha = 0.0;
    let mut der_beta = 0.0;

    // FIX 取出该函数的历史调用记录。
    // MARK 检查公式是否编码计算正确
    if let Some(call_history) = self.fn_call_history.get(&fnid) {

        // 计算 alpha 的偏导数的第一、二项
        let mut der_alpha_first_item = 0.0;
        let mut der_alpha_second_item = 0.0;

        // 计算 beta 的偏导数的第一、二项
        let mut der_beta_first_item = 0.0;
        let mut der_beta_second_item = 0.0;

        // 计算累乘、累加。只加上调用记录的时刻的温度值，不能把没调用时刻的温度也加上
        for frame_and_temp_outer in call_history.borrow().iter(){
            // 计算 mu 的偏导数的第一项
            der_mu += 1.0 / frame_and_temp_outer.temp;

            // 计算 alpha 的偏导数的第一项的分子、分母
            let mut der_alpha_first_item_numerate = 0.0;
            let der_alpha_first_item_denominate = frame_and_temp_outer.temp;

            // 计算 beta 的偏导数的第二项的分子、分母
            let mut der_beta_second_item_numerate = 0.0;
            let der_beta_second_item_denominate = frame_and_temp_outer.temp;

            for frame_and_temp_inner in call_history.borrow().iter(){

                if frame_and_temp_inner.frame > frame_and_temp_outer.frame{
                    break;
                }
                else{
                    // alpha 的偏导数的第一项的分子累加
                    der_alpha_first_item_numerate += (-beta * (frame_and_temp_outer.frame - frame_and_temp_inner.frame) as f64).exp();

                    // beta 的偏导数的第二项的分子累加
                    der_beta_second_item_numerate += alpha * ((frame_and_temp_outer.frame - frame_and_temp_inner.frame) as f64) * ((-beta * (frame_and_temp_outer.frame - frame_and_temp_inner.frame) as f64).exp());
                }

            }

            // alpha 的偏导数的第一项分子与分母的商的累加
            der_alpha_first_item += der_alpha_first_item_numerate / der_alpha_first_item_denominate;

            // alpha 的偏导数的第二项可以看成是第一项的分子的累加，但是不除以第一项的分母
            der_alpha_second_item += der_alpha_first_item_numerate;


            // beta 的偏导数的第一项的累加，可以看成是第二项的分子的累加，但是不除以第二项的分母
            der_beta_first_item += der_beta_second_item_numerate;

            // beta 的偏导数的第二项的分子与分母的商的累加
            der_beta_second_item += der_beta_second_item_numerate / der_beta_second_item_denominate;

        }

        // 减去偏导数的第二项，mu 的偏导数计算完毕
        der_mu -= self.temp_history_window_len as f64;

        // 两项相减，alpha 的偏导数计算完毕
        der_alpha = der_alpha_first_item - der_alpha_second_item;

        // 两项相减，beta 的偏导数计算完毕
        der_beta = der_beta_first_item - der_beta_second_item;
    }

    // 更新参数
    mu += learning_rate * der_mu;
    alpha += learning_rate * der_alpha;
    beta += learning_rate * der_beta;

    // 参数的非负约束
    if mu <= 0.0 {
        mu = 0.1;
    }
    if alpha <= 0.0 {
        alpha = 0.1;
    }
    if beta <= 0.0 {
        beta = 1.0;
    }

    // log::info!("fn_params: fnid: {:?}, mu: {}, alpha: {}, beta: {}, iter_count: {}", fnid, mu, alpha, beta, iter_count);

    // 迭代次数加 1
    iter_count += 1;

    // 如果迭代次数超过10次或者偏导数小于0.05，则停止迭代
    if iter_count > 10 || (der_mu <= 0.005 && der_alpha <= 0.005 && der_beta <= 0.005) {
        break;
    }
    }

}
*/
