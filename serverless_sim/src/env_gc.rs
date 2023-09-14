use std::{ thread, time::{ SystemTime, UNIX_EPOCH, Duration } };

use crate::network::SIM_ENVS;

pub fn start_gc() {
    thread::spawn(|| {
        loop {
            // println!("hi number {} from the spawned thread!", i);
            {
                let start = SystemTime::now();
                let now = start.duration_since(UNIX_EPOCH).unwrap();

                let mut sim_envs = SIM_ENVS.write().unwrap();
                let mut to_remove = vec![];

                for e in sim_envs.iter() {
                    let env = e.1.lock().unwrap();
                    if now > Duration::from_secs(60) + env.recent_use_time {
                        let key = env.config.str();
                        log::warn!("gc env {}", key);
                        env.metric_record.borrow().flush();
                        to_remove.push(key);
                    }
                }
                for key in to_remove {
                    sim_envs.remove(&key);
                }
            }
            thread::sleep(Duration::from_millis(30));
        }
    });
}
