// 论文 Reinforcement Learning Applicability for Resource-Based Auto-scaling in Serverless Edge Applications 复现
// 通过 Q-learning 算法优化 HPA 的 CPU 使用率阈值配置，以提高服务质量。

use std::collections::HashMap;
use rand::Rng;
use crate::mechanism::SimEnvObserve;
use crate::with_env_sub::WithEnvCore;
use crate::{ actions::ESActionWrapper, fn_dag::FnId };
use super::ScaleNum;
use super::hpa::{ HpaScaleNum, Target };

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
enum Action {
    Increase, // 增加 CPU 使用率阈值
    Decrease, // 减少 CPU 使用率阈值
    Maintain, // 保持不变
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
struct State {
    cpu_threshold: i32, // 当前的 CPU 使用率阈值
}

// Q-Learning 参数
pub struct QLearningHpaScaleNum {
    q_table: HashMap<(State, Action), f32>, // Q 表
    learning_rate: f32,   // 学习率 α
    discount_factor: f32, // 折扣因子 γ
    exploration_rate: f32, // 探索率 ε
    min_cpu_threshold: i32, // CPU 使用率最小阈值
    max_cpu_threshold: i32, // CPU 使用率最大阈值
    target_latency: f32,    // 目标延迟
    tolerance: f32,    // 容忍度
    hpa_scale_num: HpaScaleNum,
}

impl QLearningHpaScaleNum {
    pub fn new(target_latency: f32) -> Self {
        QLearningHpaScaleNum {
            q_table: HashMap::new(),
            learning_rate: 0.5,
            discount_factor: 0.95,
            exploration_rate: 1.0, // 初始 ε 值
            min_cpu_threshold: 30,
            max_cpu_threshold: 80,
            target_latency,
            tolerance: 0.1,
            hpa_scale_num: HpaScaleNum::new(),
        }
    }

    /// 选择动作：基于 ε-贪婪策略
    fn choose_action(&self, state: &State) -> Action {
        let mut rng = rand::thread_rng();
        if rng.gen_range(0.0..2.0) < self.exploration_rate {
            // 随机选择动作
            let actions = self.get_valid_actions(state.cpu_threshold);
            actions[rng.gen_range(0..actions.len())].clone()
        } else {
            // 选择 Q 值最大的动作
            let actions = self.get_valid_actions(state.cpu_threshold);
            let mut max_q = f32::MIN;
            let mut best_action = Action::Maintain;

            for action in actions {
                let q = *self.q_table.get(&(state.clone(), action.clone())).unwrap_or(&0.0);
                if q > max_q {
                    max_q = q;
                    best_action = action.clone();
                }
            }
            best_action
        }
    }

    /// 在合法范围内随机选择动作
    fn get_valid_actions(&self, cpu_threshold: i32) -> Vec<Action> {
        let mut actions = vec![Action::Maintain];
        if cpu_threshold > self.min_cpu_threshold {
            actions.push(Action::Decrease);
        }
        if cpu_threshold < self.max_cpu_threshold {
            actions.push(Action::Increase);
        }
        actions
    }

    /// 更新 Q 表
    fn update_q_table(&mut self, state: &State, action: &Action, reward: f32, next_state: &State) {
        let current_q = *self.q_table.get(&(state.clone(), action.clone())).unwrap_or(&0.0);
        let max_next_q = self
            .get_valid_actions(next_state.cpu_threshold)
            .iter()
            .map(|a| *self.q_table.get(&(next_state.clone(), a.clone())).unwrap_or(&0.0))
            .fold(f32::MIN, f32::max);

        let new_q = current_q
            + self.learning_rate * (reward + self.discount_factor * max_next_q - current_q);
        self.q_table.insert((state.clone(), action.clone()), new_q);
    }

    /// 奖励函数：基于目标延迟计算奖励
    fn calculate_reward(&self, latency: f32) -> f32 {
        if latency < self.target_latency * (1.0 + self.tolerance) {
            (self.target_latency / latency) * 10.0 // 延迟越低奖励越高
        } else {
            1.0 // 如果超出目标范围，奖励很低
        }
    }
}

impl ScaleNum for QLearningHpaScaleNum{
    // 获得动作，拿到延迟，计算奖励，更新 Q 表
    fn scale_for_fn(
        &mut self,
        env: &SimEnvObserve,
        fnid: FnId,
        _action: &ESActionWrapper
    ) -> usize {
        // 使用 hpa 进行扩缩容
        let desired_container_cnt = self.hpa_scale_num.scale_for_fn(env, fnid, _action);

        // 先拿到当前 cpu 使用率阈值
        let state = State {
            cpu_threshold: (self.hpa_scale_num.get_target() * 100.0) as i32,
        };
        // 选择动作
        let action = self.choose_action(&state);

        // 执行动作并更新状态
        let next_cpu_threshold = match action {
            Action::Increase => (state.cpu_threshold + 1).min(self.max_cpu_threshold),
            Action::Decrease => (state.cpu_threshold - 1).max(self.min_cpu_threshold),
            Action::Maintain => state.cpu_threshold,
        };
        // 获得下一个动作的状态
        let next_state = State {
            cpu_threshold: next_cpu_threshold,
        };
        // 更新 hpa 算法的 cpu 使用率阈值
        self.hpa_scale_num.set_target(Target::MemUseRate(next_cpu_threshold as f32 / 100.0));

        log::info!("当前 cpu 利用率阈值为：{}，动作为：{:?}，下一个状态的 cpu 利用率阈值为: {}", state.cpu_threshold, action, next_state.cpu_threshold);

        // 更新 Q 表
        let avg_latency = env.req_done_time_avg();
        let reward = self.calculate_reward(avg_latency);
        self.update_q_table(&state, &action, reward, &next_state);

        // 探索率衰减
        if env.core().current_frame() > 100 && self.exploration_rate > 0.2 {
            self.exploration_rate *= 0.9977;
        }

        desired_container_cnt
    }
}