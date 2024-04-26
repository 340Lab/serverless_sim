/* 
算法流程：https://fvd360f8oos.feishu.cn/docx/QbXqdmszVo4lOsxvveacdDOMn6c?from=from_copylink
*/


use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use crate::scale::down_exec::ScaleDownExec;
use crate::{actions::ESActionWrapper, algos::ContainerMetric, fn_dag::FnId, sim_env::SimEnv};

use super::{
    down_filter::{CarefulScaleDownFilter, ScaleFilter},
    ScaleNum,
};

// 定义Hawkes过程参数类型
struct HawkesParams {
    mu: f64,            // 在无历史调用影响下的平均调用率
    alpha: f64,         // 单个触发事件的影响力
    beta: f64,          // 衰减率，表示过去调用对当前调用率影响的衰减速度
}
impl HawkesParams {
    fn new() -> HawkesParams {
        // MARK 以下三个参数初始值可以更改
        HawkesParams {
            mu: 0.1,
            alpha: 0.2,
            beta: 1.0,
        }
    }
}

struct FrameCountTemp {
    frame:usize,
    count:usize,
    temp:f64,
}
impl FrameCountTemp {
    fn new(frame:usize, count:usize, temp:f64) -> FrameCountTemp {
        FrameCountTemp {
            frame,
            count,
            temp,
        }
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

    // 历史 调用_温度 记录窗口长度
    call_history_window_len: usize,

    // 函数近期温度记录的窗口长度
    temp_history_window_len: usize,
 
    // 控制缩容时候的容器过滤策略，目前用的是 CarefulScaleDownFilter
    pub scale_down_policy: Box<dyn ScaleFilter + Send>,
}

impl TempScaleNum {
    fn new() -> Self {
        Self {
            fn_params: HashMap::new(),
            fn_call_history: HashMap::new(),
            fn_temp_history: HashMap::new(),

            // MARK 以下两个参数初始值可以更改
            call_history_window_len: 100,
            temp_history_window_len: 100,
            scale_down_policy: Box::new(CarefulScaleDownFilter::new()),

        }
    }

    // 计算指定帧数下，指定函数的温度值
    fn compute_fn_temperature(&self, fnid: FnId, calculate_frame: usize) -> f64{
        
        // 取出参数
        let mu = self.fn_params.get(&fnid).unwrap().borrow().mu;

        // 温度初始化
        let mut temp = mu;
        
        // 取出函数的调用记录
        if let Some(call_records) = self.fn_call_history.get(&fnid) {

            // 获取当前帧数、参数
            let alpha = self.fn_params.get(&fnid).unwrap().borrow().alpha;
            let beta = self.fn_params.get(&fnid).unwrap().borrow().beta;
            
            // 根据 Hawkes 公式计算温度
            for frame_count_temp in call_records.borrow().iter() {

                // 只能计算指定帧数以前的调用记录
                if calculate_frame < frame_count_temp.frame {
                    break;
                }

                temp += frame_count_temp.count as f64 * alpha * (-beta * (calculate_frame - frame_count_temp.frame) as f64).exp();
            }

        }

        // 取温度的对数
        temp.ln()
    }

    // 计算指定函数的温度变化感知阈值（近期温度的标准差）
    fn compute_fn_temp_trans_threshold(&self, fnid: FnId, env: &SimEnv) -> f64 {
        
        // 先取出指定函数的历史温度记录
        let samples = self.fn_temp_history.get(&fnid).unwrap().borrow().iter().map(|frame_count_temp| frame_count_temp.temp).collect::<Vec<f64>>();

        // 求平均数
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;

        // 求方差
        let variance = samples.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (samples.len() - 1) as f64;

        // 求标准差
        let std_dev = variance.sqrt();

        // update 将标准差的1.2倍作为临界值，可以根据实验情况更改
        std_dev * 1.2

    }

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

        log::info!("fn_params: fnid: {:?}, mu: {}, alpha: {}, beta: {}, iter_count: {}", fnid, mu, alpha, beta, iter_count);

        // 迭代次数加 1
        iter_count += 1;

        // 如果迭代次数超过10次或者偏导数小于0.05，则停止迭代
        if iter_count > 10 || (der_mu <= 0.05 && der_alpha <= 0.05 && der_beta <= 0.05) {
            break;
        }

        }

        // 参数的非负约束
        if self.fn_params.get(&fnid).unwrap().borrow().mu <= 0.0 {
            self.fn_params.get(&fnid).unwrap().borrow_mut().mu = 0.1;
        }
        if self.fn_params.get(&fnid).unwrap().borrow().alpha <= 0.0 {
            self.fn_params.get(&fnid).unwrap().borrow_mut().alpha = 0.1;
        }
        if self.fn_params.get(&fnid).unwrap().borrow().beta <= 0.0 {
            self.fn_params.get(&fnid).unwrap().borrow_mut().beta = 1.0;
        }

    }

}

// 实现核心 trait
impl ScaleNum for TempScaleNum {


    // TODO 完成函数功能
    // FIX  修改函数到达逻辑
    // 设置指定函数的目标容器数量
    fn scale_for_fn(&mut self, env: &SimEnv, fnid: FnId, action: &ESActionWrapper) -> usize {
        // 初始化======================================================================================
        // 获得当前帧数
        let current_frame = env.current_frame();

        // 如果该函数是第一次进行扩缩容操作，则初始化参数、调用记录、历史记录
        self.fn_params.entry(fnid).or_insert_with(|| RefCell::new(HawkesParams::new()));
        self.fn_call_history.entry(fnid).or_insert_with(|| RefCell::new(VecDeque::new()));
        self.fn_temp_history.entry(fnid).or_insert_with(|| RefCell::new(VecDeque::new()));
        
        // 拿到可变借用,不能在初始化时一起拿可变借用
        let mut params= self.fn_params.get(&fnid).unwrap().borrow_mut();
        let mut call_history= self.fn_call_history.get(&fnid).unwrap().borrow_mut();
        let mut temp_recent= self.fn_temp_history.get(&fnid).unwrap().borrow_mut();
        // ============================================================================================


        // 得到当前函数在这一帧的到达数量--------------------------------------------------------
        let mut fn_count = 0;

        // 取出所有的请求的不可变借用
        let requests = env.core.requests();

        // 遍历所有请求，只看当前帧到达的请求
        for (_, req) in requests.iter().filter(|(_, req)| req.begin_frame == current_frame) {
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


        // 更新函数的历史调用记录--------------------------------------------------------------------
        // 只有fn_count > 0 才更新调用记录
        if fn_count > 0 {
            call_history.push_back(FrameCountTemp::new(current_frame, fn_count, 0.0));
            // 控制滑动窗口长度
            if call_history.len() > self.call_history_window_len{
                call_history.pop_front();
            }

            // 获取当前帧数的调用温度
            let temp = self.compute_fn_temperature(fnid, current_frame);
            // 修改 temp
            if let Some(mut last_frame_and_temp) = call_history.pop_back() {
                last_frame_and_temp.temp = temp;
                // 然后将修改后的元素再次压入 VecDeque 中
                call_history.push_back(last_frame_and_temp);
            }
        }
        // ----------------------------------------------------------------------------------------
        

        // 更新函数的历史温度记录---------------------------------------------------------------
        // 更新函数的历史温度记录后（有可能不更新），获取当前帧数的调用温度
        let temp = self.compute_fn_temperature(fnid, current_frame);

        // 插入到函数的历史温度记录
        temp_recent.push_back(FrameCountTemp::new(current_frame, fn_count, temp));

        // 控制滑动窗口长度
        if temp_recent.len() > self.temp_history_window_len{
            temp_recent.pop_front();
        }
        // ----------------------------------------------------------------------------------------


        // TODO 根据近期温度记录表来决定是否扩缩容以及计算目标容器数量----------------------------------
        // 标记温度策略是否决定了扩缩容
        let mut desired_container_cnt = env.fn_container_cnt(fnid);
        let mut scale_sign = false;

        // 如果记录表长度小于 10，则不进行温度决策
        if temp_recent.len() >= 10 {
            // 获得函数的温度变化感知阈值
            let temp_trans_threshold = self.compute_fn_temp_trans_threshold(fnid, env);

            // 新建一个近期温度变化记录表，长度为 temp_care_window_len，暂时定为 20
            let mut temp_care_records : VecDeque<f64> = VecDeque::new();
            let temp_care_window_len = 20;

            // 从队尾往队头遍历（队尾的记录是最新的）插入temp_care_records
            for frame_count_temp in temp_recent.iter().rev() {
                temp_care_records.push_front(frame_count_temp.temp);

                // 控制记录表长度为窗口长度，但是也不一定能装满
                if temp_care_records.len() == temp_care_window_len {
                    break;
                }
            }

            // 计算温度变化量，把近期温度变化分为前后两部分
            let first_half: Vec<f64> = temp_care_records.iter().cloned().take(temp_care_records.len() / 2).collect();
            let second_half: Vec<f64> = temp_care_records.iter().cloned().skip(temp_care_records.len() / 2).collect();

            // 计算平均数
            let sum_first: f64 = first_half.iter().sum();
            let avg_first = sum_first / (first_half.len() as f64);
            let sum_second: f64 = second_half.iter().sum();
            let avg_second = sum_second / (second_half.len() as f64);

            // 计算温度增量，可能为负
            let temp_change = (avg_first - avg_second) / (temp_care_records.len() as f64 / 2.0);

            // MARK 缩容怎么弄？
            // 如果温度增量大于温度变化感知阈值，则进行扩缩容
            if temp_change.abs() > temp_trans_threshold {
                // 标记这一帧用温度策略决定扩缩容
                scale_sign = true;

                // 先获取该函数的现有容器数量
                let container_cnt = env.fn_container_cnt(fnid);

                // 计算容器数量的增率
                let container_inc_rate = temp_change.abs() / temp_trans_threshold;

                // 计算变化的容器数量，向上取整
                let container_change = (container_cnt as f64 * (container_inc_rate - 1.0)).ceil() as usize;

                // 计算目标容器数量
                desired_container_cnt = 
                    if container_inc_rate > 0.0 {
                        container_cnt + container_change
                    }
                    else{
                        container_cnt - container_change
                    };
                
                // 目标容器数量不能小于 0
                if desired_container_cnt < 0 {
                    desired_container_cnt = 0;
                }
            }

        }
        // ----------------------------------------------------------------------------------------


        // TODO 设置机制来持续缓慢升温/降温的情况-----------------------------------------------------
        // ----------------------------------------------------------------------------------------
        
        // 如果该函数有未调度的，则至少要有一个容器
        if desired_container_cnt == 0 && env.help.mech_metric().fn_unsche_req_cnt(fnid) > 0{
            desired_container_cnt = 1;
        }

        desired_container_cnt
    }
}

