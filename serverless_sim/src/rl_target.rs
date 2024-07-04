// 注册rl 调用目标。rl step 到来时，http调用注册的对象的step，
// 传入action，传出score和state

use std::ptr::NonNull;

lazy_static! {
    pub static ref RL_TARGET: Option<Box<dyn RlTarget>> = None;
}

pub trait RlTarget: 'static + Send + Sync {
    // state, score, done
    fn step(&self, action: usize) -> (Vec<f64>, f32, bool);
    fn set_stop(&self);
}

pub fn register_rl_target(rl_target: Box<dyn RlTarget>) {
    unsafe {
        let mut replace = NonNull::new(
            &*RL_TARGET as *const _ as *mut Option<Box<dyn RlTarget>>
        ).unwrap();

        let _ = replace.as_mut().replace(rl_target);
    }
}
