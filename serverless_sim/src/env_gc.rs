use std::{
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::network::SIM_ENVS;

/// 函数启动一个线程，该线程以大约每 30 毫秒的间隔检查 SIM_ENVS 中的模拟环境。
/// 如果某个环境在最近 60 秒内未被使用，它将被移除，并清理相关资源。同时，会记录警告日志和度量数据。
/// 这个函数旨在释放不再使用的资源，减少内存占用，保持系统健康。
pub fn start_gc() {
    // 创建新线程
    thread::spawn(|| {
        // GC 线程将持续运行，定期执行垃圾回收任务
        loop {
            // println!("hi number {} from the spawned thread!", i);
            {
                let start = SystemTime::now();
                let now = start.duration_since(UNIX_EPOCH).unwrap();

                // 获取 SIM_ENVS 中的所有模拟环境
                let mut sim_envs = SIM_ENVS.write();
                // 用于存储待移除的模拟环境的键
                let mut to_remove = vec![];

                for e in sim_envs.iter() {
                    let env = e.1.lock();
                    // 检查当前时间是否已经超过环境最近使用时间加上 60 秒
                    if now > Duration::from_secs(60) + env.recent_use_time {
                        let key = env.help.config().str();
                        log::warn!("gc env {}", key);
                        env.help.metric_record().flush(&env);
                        to_remove.push(key);
                    }
                }
                for key in to_remove {
                    sim_envs.remove(&key);
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
}
