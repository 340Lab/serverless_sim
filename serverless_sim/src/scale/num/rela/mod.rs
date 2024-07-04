// Policy in paper Cejoss
// by ActivePeter

use std::{ collections::HashMap, sync::Arc, ptr::NonNull };

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

struct RelaRlTargetInner {
    pub state_score: Mutex<(Vec<f64>, f32, bool)>,
    each_fn: HashMap<FnId, (HpaScaleNum, EachFnState)>,
    fncnt: usize,
    curfn: FnId,
}

pub struct RelaRlTarget {
    inner: Arc<RelaRlTargetInner>,
}

unsafe impl Send for RelaRlTargetInner {}

unsafe impl Sync for RelaRlTargetInner {}

impl RlTarget for RelaRlTarget {
    fn step(&self, action: usize) -> (Vec<f64>, f32, bool) {
        let next_fn = unsafe {
            let mut inner: NonNull<RelaRlTargetInner> = util::non_null(&*self.inner);
            // env maybe reset, but not inited
            if inner.as_mut().fncnt == 0 {
                return (vec![], 0.0, true);
            }
            let cur_fn = inner.as_mut().curfn;
            // update hpa
            let v = 0.1 + 0.8 * ((action as f32) / 10.0);
            inner.as_mut().each_fn.get_mut(&cur_fn).unwrap().0.set_target(Target::MemUseRate(v));

            inner.as_mut().curfn += 1;
            inner.as_mut().curfn %= inner.as_mut().fncnt;

            inner.as_mut().curfn
        };

        let mut ret = self.inner.state_score.lock().clone();
        let eachfn = self.inner.each_fn.get(&next_fn).unwrap();
        ret.0[5] = eachfn.1.fn_mem as f64;
        ret.0[6] = eachfn.1.fn_cpu as f64;
        ret.0[7] = eachfn.1.fn_total_mem as f64;
        ret.0[8] = eachfn.1.fn_total_cpu as f64;
        for (i, v) in eachfn.1.history_target.queue.iter().enumerate() {
            ret.0[9 + i] = *v as f64;
        }
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
                let mut rl: NonNull<RelaRlTargetInner> = util::non_null(&*self.rl);
                let fncnt = env.core().fns().len();
                rl.as_mut().fncnt = fncnt;
                rl.as_mut().each_fn = (0..fncnt)
                    .map(|v| { (v, (HpaScaleNum::new(), EachFnState::default())) })
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

                state_score.0 = vec![
                    cur_frame as f64,
                    total_mem as f64,
                    total_cpu as f64,
                    used_mem as f64,
                    used_cpu as f64,
                    0.0, //5 fncpu
                    0.0, //6 fnmem
                    0.0, //7 fncpu_use
                    0.0 //8 fnmem_use
                ];
                state_score.0.resize(9 + SCALE_WINDOW_SIZE, 0.0);
                state_score.1 = env.quality_price_ratio();
            }

            unsafe {
                let mut each_fn = util::non_null(&self.rl.each_fn);

                for f in env.core().fns().iter() {
                    let each_fn = each_fn.as_mut().get_mut(&f.fn_id).unwrap();
                    each_fn.1.fn_mem = f.mem;
                    each_fn.1.fn_cpu = f.cpu;
                    let mut fn_mem_use = 0.0;
                    let mut fn_cpu_use = 0.0;
                    env.fn_containers_for_each(f.fn_id, |c| {
                        fn_mem_use += c.last_frame_mem;
                        fn_cpu_use += c.last_frame_cpu_used;
                    });
                }
            }
        }
        unsafe {
            let mut each_fn = util::non_null(&self.rl.each_fn);
            each_fn.as_mut().get_mut(&fnid).unwrap().0.scale_for_fn(env, fnid, action)
        }
    }
}
