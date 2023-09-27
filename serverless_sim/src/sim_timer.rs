use std::sync::{ Arc, Mutex };

use crate::sim_env::SimEnv;

impl SimEnv {
    pub fn start_timer<F: FnMut(&SimEnv) + Send + 'static>(&self, timeout: usize, f: F) {
        let end_frame = self.current_frame() + timeout;
        let shared = Arc::new(Mutex::new(Some(f)));
        self.timers
            .borrow_mut()
            .entry(end_frame)
            .and_modify(|v| {
                let shared = shared.clone();
                v.push(Box::new(shared.lock().unwrap().take().unwrap()));
            })
            .or_insert_with(|| { vec![Box::new(shared.lock().unwrap().take().unwrap())] });
    }
}
