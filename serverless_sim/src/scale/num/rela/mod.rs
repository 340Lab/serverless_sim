// Policy in paper Cejoss
// by ActivePeter

use std::{ collections::HashMap, sync::Arc };

use parking_lot::{ Mutex };

use crate::{
    mechanism::SimEnvObserve,
    fn_dag::{ FnId, EnvFnExt },
    actions::ESActionWrapper,
    rl_target::{ self, RlTarget },
    util::{ self, Window },
    with_env_sub::{ WithEnvCore },
    score::EnvMetricExt,
};
use super::{ ScaleNum, hpa::{ HpaScaleNum, Target } };

const SCALE_WINDOW_SIZE: usize = 10;

pub struct RelaScaleNum {
    rl: Arc<RelaRlTargetInner>,
    last_env_frame: Option<usize>,
}

struct EachFnState {
    fn_mem: f32,
    fn_cpu: f32,
    fn_total_mem: f32,
    fn_total_cpu: f32,
    history_target: Window,
}
impl Default for EachFnState {
    fn default() -> Self {
        Self {
            fn_mem: 0.0,
            fn_cpu: 0.0,
            fn_total_mem: 0.0,
            fn_total_cpu: 0.0,
            history_target: Window::new(SCALE_WINDOW_SIZE),
        }
    }
}

enum ScalePolicy {
    Hpa(HpaScaleNum),
    Direct(usize),
}

impl ScalePolicy {
    fn update_with_action(&mut self, action: usize) {
        match self {
            ScalePolicy::Hpa(hpa) => {
                let v = 0.1 + 0.8 * ((action as f32) / 10.0);
                hpa.set_target(Target::MemUseRate(v));
            }
            ScalePolicy::Direct(d) => {
                *d = action;
            }
        }
    }
}

struct RelaRlTargetInner {
    pub state_score: Mutex<(Vec<f64>, f32, bool)>,
    each_fn: HashMap<FnId, (ScalePolicy, EachFnState)>,
    fncnt: usize,
    curfn: FnId,
}

pub struct RelaRlTarget {
    inner: Arc<RelaRlTargetInner>,
}

unsafe impl Send for RelaRlTargetInner {}

unsafe impl Sync for RelaRlTargetInner {}

fn new_state(
    cur_frame: usize,
    total_mem: f32,
    total_cpu: f32,
    used_mem: f32,
    used_cpu: f32,
    time_per_req: f32,
    cost_per_req: f32
) -> Vec<f64> {
    let mut ret = vec![
        cur_frame as f64,
        total_mem as f64,
        total_cpu as f64,
        used_mem as f64,
        used_cpu as f64,
        time_per_req as f64,
        cost_per_req as f64,
        0.0, //7 fnid
        0.0, //8 fnbusyness
        0.0, //9 fncpu
        0.0, //10 fnmem
        0.0, //11 fncpu_use
        0.0 //12 fnmem_use
    ];
    let baselen = ret.len();
    ret.resize(baselen + SCALE_WINDOW_SIZE, 0.0);
    ret
}
fn state_set_for_fn(
    state: &mut Vec<f64>,
    fnid: FnId,
    fn_busyness: f32,
    fn_cpu: f32,
    fn_mem: f32,
    fn_cpu_use: f32,
    fn_mem_use: f32,
    history_target: &Window
) {
    let beginidx = 7;
    state[beginidx + 0] = fnid as f64;
    state[beginidx + 1] = fn_busyness as f64;
    state[beginidx + 2] = fn_cpu as f64;
    state[beginidx + 3] = fn_mem as f64;
    state[beginidx + 4] = fn_cpu_use as f64;
    state[beginidx + 5] = fn_mem_use as f64;
    for (i, v) in history_target.queue.iter().enumerate() {
        state[beginidx + 6 + i] = *v as f64;
    }
}

impl RlTarget for RelaRlTarget {
    fn step(&self, action: usize) -> (Vec<f64>, f32, bool) {
        let next_fn = unsafe {
            let mut inner = util::non_null(&*self.inner);
            // env maybe reset, but not inited
            if inner.0.as_mut().fncnt == 0 {
                return (vec![], 0.0, true);
            }
            let cur_fn = inner.0.as_mut().curfn;
            // update hpa
            // let v = 0.1 + 0.8 * ((action as f32) / 10.0);
            inner.0.as_mut().each_fn.get_mut(&cur_fn).unwrap().0.update_with_action(action);

            inner.0.as_mut().curfn += 1;
            inner.0.as_mut().curfn %= inner.0.as_mut().fncnt;

            inner.0.as_mut().curfn
        };

        let mut ret = self.inner.state_score.lock().clone();
        let f = self.inner.each_fn.get(&next_fn).unwrap();
        state_set_for_fn(
            &mut ret.0,
            next_fn,
            0.0,
            f.1.fn_cpu,
            f.1.fn_mem,
            f.1.fn_total_cpu,
            f.1.fn_total_mem,
            &f.1.history_target
        );
        ret
    }
    fn set_stop(&self) {
        self.inner.state_score.lock().2 = true;
    }
}

impl RelaScaleNum {
    pub fn new() -> RelaScaleNum {
        let rl = Arc::new(RelaRlTargetInner {
            state_score: Mutex::new((vec![], 0.0, false)),
            each_fn: HashMap::new(),
            fncnt: 0,
            curfn: 0,
        });
        rl_target::register_rl_target(
            Box::new(RelaRlTarget {
                inner: rl.clone(),
            })
        );

        RelaScaleNum {
            rl,
            last_env_frame: None,
        }
    }
}

// # state

// cur_frame

// 计算总量
// 总内存占用量
// 总cpu占用量
// 函数计算量
// 函数内存量
// 函数总内存占用
// 函数上一帧使用计算量
// 平均请求延迟
// 平均请求成本
impl ScaleNum for RelaScaleNum {
    fn scale_for_fn(&mut self, env: &SimEnvObserve, fnid: FnId, action: &ESActionWrapper) -> usize {
        if self.last_env_frame.is_none() {
            unsafe {
                let mut rl = util::non_null(&*self.rl);
                let fncnt = env.core().fns().len();
                rl.0.as_mut().fncnt = fncnt;

                rl.0.as_mut().each_fn = (0..fncnt)
                    .map(|v| {
                        (
                            v,
                            (
                                // if env.dag(env.func(v).dag_id).len() > 1 {
                                ScalePolicy::Hpa(HpaScaleNum::new()),
                                // } else
                                // {
                                //     ScalePolicy::Direct(0)
                                // }
                                EachFnState::default(),
                            ),
                        )
                    })
                    .collect();
            }
        }
        if
            self.last_env_frame.is_none() ||
            *self.last_env_frame.as_ref().unwrap() != env.core().current_frame()
        {
            // new env came, update state & score
            {
                let mut state_score = self.rl.state_score.lock();

                // # cur frame
                let cur_frame = env.core().current_frame();
                self.last_env_frame = Some(cur_frame);

                // # 内存总量
                let total_mem = env
                    .core()
                    .nodes()
                    .iter()
                    .map(|n| { n.rsc_limit.mem })
                    .sum::<f32>();

                // # cpu 总量
                let total_cpu = env
                    .core()
                    .nodes()
                    .iter()
                    .map(|n| { n.rsc_limit.cpu })
                    .sum::<f32>();

                let used_mem = env
                    .core()
                    .nodes()
                    .iter()
                    .map(|n| n.last_frame_mem)
                    .sum::<f32>();
                let used_cpu = env
                    .core()
                    .nodes()
                    .iter()
                    .map(|n| n.last_frame_cpu)
                    .sum::<f32>();

                state_score.0 = new_state(
                    cur_frame,
                    total_mem,
                    total_cpu,
                    used_mem,
                    used_cpu,
                    env.req_done_time_avg(),
                    env.cost_each_req()
                );
                state_score.1 = env.quality_price_ratio();
            }

            unsafe {
                let mut each_fn = util::non_null(&self.rl.each_fn);

                for f in env.core().fns().iter() {
                    let each_fn = each_fn.0.as_mut().get_mut(&f.fn_id).unwrap();
                    each_fn.1.fn_mem = f.mem;
                    each_fn.1.fn_cpu = f.cpu;
                    let mut fn_mem_use = 0.0;
                    let mut fn_cpu_use = 0.0;
                    env.fn_containers_for_each(f.fn_id, |c| {
                        fn_mem_use += c.last_frame_mem;
                        fn_cpu_use += c.last_frame_cpu_used;
                    });
                    each_fn.1.fn_total_mem = fn_mem_use;
                    each_fn.1.fn_total_cpu = fn_cpu_use;
                }
            }
        }
        unsafe {
            let mut each_fn = util::non_null(&self.rl.each_fn);
            let f = each_fn.0.as_mut().get_mut(&fnid).unwrap();
            let scale = match &mut f.0 {
                ScalePolicy::Hpa(hpa) => { hpa.scale_for_fn(env, fnid, action) }
                ScalePolicy::Direct(d) => *d,
            };
            // record scale
            f.1.history_target.push(scale as f32);
            scale
        }
    }
}
