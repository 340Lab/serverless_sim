use std::sync::mpsc;

use thread_priority::{ set_current_thread_priority, ThreadPriority, WinAPIThreadPriority };
// use windows::Win32::System::Threading::{ SetThreadPriority, GetCurrentThread, THREAD_PRIORITY };

use crate::actions::ESActionWrapper;
use crate::mechanism::{ DownCmd, Mechanism, MechanismImpl, ScheCmd, SimEnvObserve, UpCmd };

use crate::util;

pub type MechCmdDistributor = mpsc::Sender<MechScheduleOnceRes>;

pub struct MechScheduleOnce {
    pub sim_env: SimEnvObserve,
    pub responser: MechCmdDistributor,
    pub action: ESActionWrapper,
}

pub enum MechScheduleOnceRes {
    ScheCmd(ScheCmd),
    ScaleUpCmd(UpCmd),
    ScaleDownCmd(DownCmd),
    Cmds {
        sche_cmds: Vec<ScheCmd>,
        scale_up_cmds: Vec<UpCmd>,
        scale_down_cmds: Vec<DownCmd>,
    },
    End {
        mech_run_ms: u64,
    },
}

pub fn spawn(mech: MechanismImpl) -> mpsc::Sender<MechScheduleOnce> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        // 尝试设置当前线程的优先级
        // unsafe {
        //     SetThreadPriority(
        //         GetCurrentThread(),
        //         THREAD_PRIORITY(WinAPIThreadPriority::TimeCritical as i32)
        //     ).unwrap();
        // }
        if let Err(e) = set_current_thread_priority(ThreadPriority::Max) {
            eprintln!("设置线程优先级失败: {:?}", e);
        }

        mechanism_loop(rx, mech);
    });
    tx
}

fn mechanism_loop(rx: mpsc::Receiver<MechScheduleOnce>, mech: MechanismImpl) {
    loop {
        let res = match rx.recv() {
            Ok(res) => res,
            Err(_res) => {
                log::info!("mechanism_loop end");
                return;
            }
        };

        std::thread::sleep(std::time::Duration::from_millis(10));

        let begin_ms = util::now_ms();
        // let measure = util::MeasureThreadTime::new();
        // let begin_cpu = cpu_time::ThreadTime::now();
        mech.step(&res.sim_env, res.action, &res.responser);
        // let passed_ms = measure.passed_100ns();
        let end_ms = util::now_ms();
        // log::info!("master mech run cpu:{:?}, total:{} ms", begin_cpu.elapsed(), end_ms - begin_ms);

        res.responser
            .send(MechScheduleOnceRes::End {
                mech_run_ms: end_ms - begin_ms,
                //  (passed_ms.0 + passed_ms.1) / 10000,
            })
            .unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::mpsc;

    use crate::{ actions::ESActionWrapper, mechanism_thread::MechScheduleOnceRes, sim_env::SimEnv };

    #[test]
    pub fn test_algo_latency() {
        use std::{ cell::RefCell, rc::Rc, sync::{ atomic::AtomicU64, Arc } };

        use crate::config::Config;
        let _ = env_logger::try_init();
        let mut conf = Config::new_test();
        conf.total_frame = 50;
        let mut env = SimEnv::new(conf);
        let (tx, rx) = mpsc::channel();
        env.mech_caller = tx;
        // let algo_latencys=vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90];
        let calltime = Arc::new(AtomicU64::new(1));
        {
            let calltime = calltime.clone();
            std::thread::spawn(move || {
                while let Ok(once) = rx.recv() {
                    once.responser
                        .send(MechScheduleOnceRes::End {
                            mech_run_ms: calltime.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                        })
                        .unwrap();
                }
            });
        }
        let mut calltime = 1;
        let begin_frame = Rc::new(RefCell::new(0));
        let begin_frame2 = begin_frame.clone();

        env.step_es(
            ESActionWrapper::Int(0),
            None,
            None,
            Some(
                Box::new(move |env: &SimEnv| {
                    *begin_frame.borrow_mut() = env.current_frame();
                })
            ),
            Some(
                Box::new(move |env: &SimEnv| {
                    // calltime = env.current_frame() - begin_frame;
                    assert!(
                        env.current_frame() - *begin_frame2.borrow() == calltime,
                        "begin_frame:{} current_frame:{} calltime:{}",
                        begin_frame2.borrow(),
                        env.current_frame(),
                        calltime
                    );
                    calltime += 1;
                })
            )
        );
    }
}
