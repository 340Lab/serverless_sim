use std::sync::{ Arc };

use parking_lot::Mutex;

use crate::sim_env::SimEnv;

/// SimEnv 的 timers 字段维护了一个映射，记录了每个未来帧数应执行的回调函数列表。
/// 当到达指定帧数时，SimEnv 可以遍历对应帧数的回调函数列表并逐一执行
impl SimEnv {
    pub fn start_timer<F: FnMut(&SimEnv) + Send + 'static>(&self, timeout: usize, f: F) {
        let end_frame = self.current_frame() + timeout;
        let shared = Arc::new(Mutex::new(Some(f)));
        // timers: RefCell<HashMap<usize, Vec<Box<dyn FnMut(&SimEnv) + Send>>>>
        self.timers
            .borrow_mut()
            .entry(end_frame)
            // 如果键已存在, 则对相应的值进行修改
            .and_modify(|v| {
                let shared = shared.clone();
                v.push(Box::new(shared.lock().take().unwrap()));
            })
            // 如果键不存在, 则插入一个新的键值对
            .or_insert_with(|| { vec![Box::new(shared.lock().take().unwrap())] });
    }
}
