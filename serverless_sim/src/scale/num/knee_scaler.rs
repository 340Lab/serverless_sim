use std::collections::{BTreeMap, HashMap};

use crate::fn_dag::{EnvFnExt, FnId};
use crate::mechanism::SimEnvObserve;
use crate::node::EnvNodeExt;
use crate::with_env_sub::{WithEnvCore, WithEnvHelp};
use crate::actions::ESActionWrapper;

use super::ScaleNum;

#[derive(Default)]
struct FnKneeState {
    throughput_by_replica: BTreeMap<usize, f32>,
    knee_replicas: Option<usize>,
    idle_frames: usize,
    last_scale_frame: usize,
}

#[derive(Default)]
struct FnMetrics {
    demand: f32,
    throughput: f32,
    avg_cpu_util: f32,
    avg_mem_util: f32,
}

pub struct KneeScaleNum {
    fn_states: HashMap<FnId, FnKneeState>,
    ema_alpha: f32,
    util_up_threshold: f32,
    util_down_threshold: f32,
    cooldown_frames: usize,
    idle_to_zero_frames: usize,
}

impl KneeScaleNum {
    pub fn new() -> Self {
        Self {
            fn_states: HashMap::new(),
            ema_alpha: 0.35,
            util_up_threshold: 0.72,
            util_down_threshold: 0.38,
            cooldown_frames: 5,
            idle_to_zero_frames: 30,
        }
    }

    fn observe_fn_metrics(env: &SimEnvObserve, fnid: FnId) -> FnMetrics {
        let mech_metric = env.help().mech_metric();
        let unsche = mech_metric.fn_unsche_req_cnt(fnid) as f32;
        let ready = mech_metric
            .fn_ready_sche_tasks(fnid)
            .map(|s| s.len() as f32)
            .unwrap_or(0.0);
        drop(mech_metric);

        let mut throughput = 0.0;
        let mut avg_cpu_util = 0.0;
        let mut avg_mem_util = 0.0;
        let mut cnt = 0;

        env.fn_containers_for_each(fnid, |container| {
            throughput += container.recent_handle_speed();
            avg_cpu_util += container.cpu_use_rate();

            let node = env.node(container.node_id);
            if node.rsc_limit.mem > 0.00001 {
                avg_mem_util += container.last_frame_mem / node.rsc_limit.mem;
            }
            cnt += 1;
        });

        if cnt > 0 {
            let cntf = cnt as f32;
            avg_cpu_util /= cntf;
            avg_mem_util /= cntf;
        }

        FnMetrics {
            demand: unsche.max(ready),
            throughput,
            avg_cpu_util,
            avg_mem_util,
        }
    }

    fn detect_knee_replicas(samples: &BTreeMap<usize, f32>) -> Option<usize> {
        if samples.len() < 3 {
            return None;
        }

        let mut points: Vec<(usize, f32)> = samples
            .iter()
            .map(|(x, y)| (*x, (*y).max(0.0)))
            .collect();

        // Make throughput monotonic to reduce oscillation noise.
        for i in 1..points.len() {
            if points[i].1 < points[i - 1].1 {
                points[i].1 = points[i - 1].1;
            }
        }

        let x_min = points.first().unwrap().0 as f32;
        let x_max = points.last().unwrap().0 as f32;
        let y_min = points
            .iter()
            .map(|(_, y)| *y)
            .fold(f32::MAX, |a, b| a.min(b));
        let y_max = points
            .iter()
            .map(|(_, y)| *y)
            .fold(f32::MIN, |a, b| a.max(b));

        if (x_max - x_min) < 0.00001 || (y_max - y_min) < 0.00001 {
            return None;
        }

        // Kneedle-style score on normalized curve: max(y_norm - x_norm).
        let mut best_x = points[0].0;
        let mut best_score = f32::MIN;
        for (x, y) in points.iter().skip(1).take(points.len().saturating_sub(2)) {
            let x_norm = ((*x as f32) - x_min) / (x_max - x_min);
            let y_norm = (*y - y_min) / (y_max - y_min);
            let score = y_norm - x_norm;
            if score > best_score {
                best_score = score;
                best_x = *x;
            }
        }

        Some(best_x)
    }
}

impl ScaleNum for KneeScaleNum {
    fn scale_for_fn(&mut self, env: &SimEnvObserve, fnid: FnId, _action: &ESActionWrapper) -> usize {
        let current_frame = env.core().current_frame();
        let cur_container_cnt = env.fn_container_cnt(fnid);
        let metrics = Self::observe_fn_metrics(env, fnid);

        let state = self.fn_states.entry(fnid).or_default();

        if cur_container_cnt > 0 {
            let old = state
                .throughput_by_replica
                .get(&cur_container_cnt)
                .copied()
                .unwrap_or(metrics.throughput);
            let new_val = old * (1.0 - self.ema_alpha) + metrics.throughput * self.ema_alpha;
            state.throughput_by_replica.insert(cur_container_cnt, new_val);
            state.knee_replicas = Self::detect_knee_replicas(&state.throughput_by_replica);
        }

        let has_work = metrics.demand > 0.0;
        if !has_work && metrics.throughput < 0.01 {
            state.idle_frames += 1;
        } else {
            state.idle_frames = 0;
        }

        if cur_container_cnt == 0 {
            return if has_work { 1 } else { 0 };
        }

        if state.idle_frames >= self.idle_to_zero_frames {
            state.last_scale_frame = current_frame;
            return 0;
        }

        let pressure = metrics.avg_cpu_util.max(metrics.avg_mem_util);
        let knee_replicas = state.knee_replicas.unwrap_or(cur_container_cnt.max(1));

        let in_cooldown = current_frame.saturating_sub(state.last_scale_frame) < self.cooldown_frames;
        let severe_overload =
            has_work && (pressure > 0.9 || metrics.demand > metrics.throughput * 1.8);
        if in_cooldown && !severe_overload {
            return cur_container_cnt;
        }

        let mut desired = cur_container_cnt;

        // Scale up when pressure/backlog is high.
        if has_work && (pressure > self.util_up_threshold || metrics.demand > metrics.throughput * 1.2) {
            if cur_container_cnt < knee_replicas {
                desired = cur_container_cnt + 1;
            } else if metrics.demand > metrics.throughput * 1.5 {
                // Allow crossing knee under sustained overload.
                desired = cur_container_cnt + 1;
            }
        }

        // Scale down conservatively when over-knee and lightly loaded.
        if desired == cur_container_cnt &&
            cur_container_cnt > 1 &&
            cur_container_cnt > knee_replicas &&
            pressure < self.util_down_threshold &&
            metrics.demand <= metrics.throughput
        {
            desired = cur_container_cnt - 1;
        }

        if has_work && desired == 0 {
            desired = 1;
        }

        if desired != cur_container_cnt {
            state.last_scale_frame = current_frame;
        }

        desired
    }
}

