use std::{ cell::{ Ref, RefMut }, collections::{ BTreeMap, HashMap, HashSet }, thread::sleep, time::Duration };

use daggy::petgraph::visit::Topo;

use rand_distr::{ Distribution, Normal };

use crate::{
    fn_dag::{ DagId, EnvFnExt, FnId },
    node::NodeId,
    sim_env::SimEnv,
    with_env_sub::WithEnvCore,
    REQUEST_GEN_FRAME_INTERVAL,
};

pub type ReqId = usize;

// pub struct RequestPlan {
//     /// 请求id
//     pub req_id: ReqId,

//     /// 对应请求处理的应用
//     pub dag_i: usize,

//     /// 第一次walk是用来扩容，后面的walk是用来扩容
//     pub fn_dag_walker: Topo<NodeIndex, <Dag<Func, f32> as Visitable>::Map>,

//     /// 函数节点被调度到的机器节点
//     pub fn_node: HashMap<FnId, NodeId>,
// }

#[derive(Clone, Debug)]
pub struct ReqFnMetric {
    pub ready_sche_time: Option<usize>,
    pub sche_time: Option<usize>, // sche_time maybe ahead of ready_sche_time
    pub data_recv_done_time: Option<usize>, // data begin when scheduled & ready_sche
    pub cold_start_done_time: Option<usize>, // cold begin when scheduled
    pub fn_done_time: Option<usize>, // exec begin when data and cold start done
}

#[derive(Clone)]
pub struct Request {
    /// 请求id
    pub req_id: ReqId,

    /// 对应请求处理的DAG，由函数组成
    pub dag_i: DagId,

    /// 函数节点被调度到的机器节点
    pub fn_node: HashMap<FnId, NodeId>,

    pub fn_metric: HashMap<FnId, ReqFnMetric>,

    /// 完成执行的函数节点，时间
    pub done_fns: HashMap<FnId, usize>,

    // 当前帧已完成的函数
    pub cur_frame_done: HashSet<FnId>,

    // 请求到达的时刻，这个DAG中包含的所有函数到达时刻也可以看成这个时刻
    pub begin_frame: usize,

    // 请求完成的时刻
    pub end_frame: usize,

    // fn_dag_walker: Topo<NodeIndex, <FnDagInner as Visitable>::Map>,

    // current_fn: Option<(FnId, NodeIndex)>,
    pub walk_cnt: usize,

    // fnid-(predict_time, scheduled_prev_fns_cnt, prev_fns_cnt)
    pub fn_predict_prevs_done_time: HashMap<FnId, (f32, usize, usize)>,

    // initalize when fisrt time get metric
    wait_cold_start_time: Option<usize>,

    wait_sche_time: Option<usize>,

    data_recv_time: Option<usize>,

    exe_time: Option<usize>,
}

impl Request {
    fn fn_latency_unwrap(&self, f: FnId) -> usize {
        let fnmetric = self.fn_metric.get(&f).unwrap();
        fnmetric.fn_done_time.unwrap() - fnmetric.ready_sche_time.unwrap()
    }
    fn init_metrics(&mut self, env: &SimEnv) {
        // find the final node
        // // construct latency dag
        let dag = env.dag(self.dag_i);
        let mut walker = dag.new_dag_walker();
        let mut endtime_fn = BTreeMap::new();
        while let Some(fngi) = walker.next(&dag.dag_inner) {
            let fnid = dag.dag_inner[fngi];
            endtime_fn.insert(self.fn_metric.get(&fnid).unwrap().fn_done_time.unwrap(), (
                self.fn_metric.get(&fnid).unwrap().ready_sche_time.unwrap(),
                fnid,
            ));
        }
        let first = endtime_fn.iter().next().unwrap().1.clone().1;
        let mut cur: (usize, FnId) = endtime_fn.iter().next_back().unwrap().1.clone();
        let mut recur_path = vec![cur.1];
        loop {
            if cur.1 == first {
                break;
            }
            // use cur fn's begin time to get prev fn's end time
            let prev: (usize, FnId) = endtime_fn.get(&cur.0).unwrap().clone();
            recur_path.push(prev.1);
            if prev.1 == first {
                break;
            }
            cur = prev;
        }
        // let mut latency_dag: Dag<(FnId, bool), f32> = Dag::new();
        // {
        //     let mut latency_dag_map = HashMap::new();
        //     while let Some(g_i) = walker.next(&dag.dag_inner) {
        //         let fnid = dag.dag_inner[g_i];
        //         let n = latency_dag.add_node((fnid, true));
        //         let c =
        //             latency_dag.add_child(n, self.fn_latency_unwrap(fnid) as f32, (fnid, false));
        //         latency_dag_map.insert(fnid, c.1);
        //         // connect n to parents
        //         for p in env.func(fnid).parent_fns(env) {
        //             let p_graph_i = latency_dag_map.get(&p).unwrap();
        //             latency_dag.add_edge(*p_graph_i, n, 0.0).unwrap();
        //         }
        //     }
        // }

        // let critical_path = util::graph::aoe_critical_path(&latency_dag);
        // assert!(critical_path.len() % 2 == 0);
        let mut wait_cold_start_time = 0;
        let mut wait_sche_time = 0;
        let mut data_recv_time = 0;
        let mut exe_time = 0;

        for fnid in recur_path {
            // let fnid = *self.done_fns.iter().next().unwrap().0;
            let metric = self.fn_metric.get(&fnid).unwrap();
            // for n in critical_path.iter().step_by(2) {
            // let (fnid, begin) = latency_dag[*n];
            // assert!(begin);
            let sche_time = metric.sche_time.unwrap();
            let ready_sche_time = metric.ready_sche_time.unwrap();
            let cold_start_done_time = if
                let Some(cold_start_done_time) = metric.cold_start_done_time
            {
                cold_start_done_time.max(sche_time.max(ready_sche_time))
            } else {
                sche_time.max(ready_sche_time)
            };
            let data_done_time = if let Some(data_recv_done_time) = metric.data_recv_done_time {
                data_recv_done_time
            } else {
                cold_start_done_time
            };
            let fn_done_time = metric.fn_done_time.unwrap();

            // println!(
            //     "accumulated {} {} {} {}",
            //     if sche_time > ready_sche_time {
            //         sche_time - ready_sche_time
            //     } else {
            //         0
            //     },
            //     cold_start_done_time - sche_time.max(ready_sche_time),
            //     data_done_time - cold_start_done_time,
            //     fn_done_time - data_done_time
            // );
            wait_sche_time += if sche_time > ready_sche_time {
                sche_time - ready_sche_time
            } else {
                0
            };
            // log::info!("cold_start_done_time {}. sche_time.max(ready_sche_time) {}", cold_start_done_time, sche_time.max(ready_sche_time));
            wait_cold_start_time += cold_start_done_time - sche_time.max(ready_sche_time);

            // log::info!("data_done_time {} cold_start_done_time {}", data_done_time, cold_start_done_time);
            data_recv_time += data_done_time - cold_start_done_time;
            exe_time += fn_done_time - data_done_time;
        }

        // }
        self.wait_cold_start_time = Some(wait_cold_start_time);
        self.wait_sche_time = Some(wait_sche_time);
        self.data_recv_time = Some(data_recv_time);
        self.exe_time = Some(exe_time);
    }
    pub fn wait_cold_start_time(&mut self, env: &SimEnv) -> usize {
        self.init_metrics(env);
        self.wait_cold_start_time.unwrap()
    }
    pub fn wait_sche_time(&mut self, env: &SimEnv) -> usize {
        self.init_metrics(env);
        self.wait_sche_time.unwrap()
    }
    pub fn data_recv_time(&mut self, env: &SimEnv) -> usize {
        self.init_metrics(env);
        self.data_recv_time.unwrap()
    }
    pub fn exe_time(&mut self, env: &SimEnv) -> usize {
        self.init_metrics(env);
        self.exe_time.unwrap()
    }
    // pub fn new_from_plan(env: &SimEnv, plan: RequestPlan) -> Self {
    //     Self {
    //         /// 请求id
    //         req_id: plan.req_id,
    //         dag_i: plan.dag_i,
    //         fn_node: plan.fn_node,
    //         done_fns: HashSet::new(),
    //     }
    // }
    pub fn new(env: &SimEnv, dag_i: DagId, begin_frame: usize) -> Self {
        let new = Self {
            req_id: env.help.req_next_id(),
            dag_i,
            fn_node: HashMap::new(),
            done_fns: HashMap::new(),
            // fn_dag_walker: Topo::new(&env.dags.borrow()[dag_i].dag),
            // current_fn: None,
            begin_frame,
            end_frame: 0,
            cur_frame_done: HashSet::new(),
            walk_cnt: 0,
            fn_predict_prevs_done_time: HashMap::new(),
            fn_metric: {
                let dag = env.dag(dag_i);
                let mut walker = dag.new_dag_walker();
                let mut map = HashMap::new();
                while let Some(fngi) = walker.next(&dag.dag_inner) {
                    let ready_sche_time = if
                        env.func(dag.dag_inner[fngi]).parent_fns(env).is_empty()
                    {
                        Some(begin_frame)
                    } else {
                        None
                    };
                    map.insert(dag.dag_inner[fngi], ReqFnMetric {
                        ready_sche_time,
                        sche_time: None,
                        data_recv_done_time: None,
                        cold_start_done_time: None,
                        fn_done_time: None,
                    });
                }
                map
            },
            wait_cold_start_time: None,
            wait_sche_time: None,
            data_recv_time: None,
            exe_time: None,
        };
        // new.prepare_next_fn_2_bind_node(&env.dags.borrow()[dag_i].dag);
        // {
        //     log::info!("req{} dag{dag_i} has following fns", new.req_id);
        //     print!("   ");
        //     new.print_fns(env);
        // }
        new
    }

    // 判断某函数的前驱函数是否已经全部执行完毕
    pub fn parents_all_done(&self, env: &SimEnv, fnid: FnId) -> bool {
        let ps = env.func(fnid).parent_fns(env);
        for p in &ps {
            if !self.done_fns.contains_key(p) {
                return false;
            }
        }
        true
    }

    pub fn print_fns(&self, env: &SimEnv) {
        let dag_i = self.dag_i;
        let mut iter = Topo::new(&env.core.dags()[dag_i].dag_inner);

        while let Some(next) = iter.next(&env.core.dags()[dag_i].dag_inner) {
            let fnid = &env.core.dags()[dag_i].dag_inner[next];
            print!("{} ", fnid);
        }
        println!();
    }

    // 获取指定函数被调度到的节点
    pub fn get_fn_node(&self, fnid: FnId) -> Option<NodeId> {
        self.fn_node.get(&fnid).map(|v| *v)
    }

    // pub fn prepare_next_fn_2_bind_node(&mut self, g: &FnDagInner) -> Option<(FnId, NodeIndex)> {
    //     let res = self.fn_dag_walker.next(g);
    //     let res = res.map(|i| (g[i], i));
    //     self.current_fn = res;
    //     self.walk_cnt += 1;
    //     res
    // }

    // #[allow(dead_code)]
    // pub fn fn_2_bind_node(&self) -> Option<(FnId, NodeIndex)> {
    //     self.current_fn
    // }

    // MARK 减少容器实时使用mem的地方
    // 标记指定函数为已完成，更新当前帧数已完成函数，并检查请求是否已完成
    pub fn fn_done(&mut self, env: &SimEnv, fnid: FnId, current_frame: usize) {
        // log::info!("request {} fn {} done", self.req_id, fnid);
        self.done_fns.insert(fnid, current_frame);
        self.cur_frame_done.insert(fnid);
        if self.is_done(env) {
            self.end_frame = current_frame;
        }

        env.on_task_done(self, fnid);
    }
    // 返回请求对应DAG中节点（函数）的数量
    pub fn fn_count(&self, env: &impl WithEnvCore) -> usize {
        env.core().dags()[self.dag_i].dag_inner.node_count()
    }
    // 判断请求是否已完成
    pub fn is_done(&self, env: &SimEnv) -> bool {
        self.done_fns.len() == self.fn_count(env)
    }
}

impl SimEnv {
    // 获取随机的 IAT 频率，用于模拟真实负载
    fn get_random_frequency(&self, avg_freq: f64, cv: f64) -> f64 {
        // Calculate the standard deviation in terms of IAT
        let standard_deviation = avg_freq * cv;
        // Create a normal distribution with the given mean and standard deviation
        let normal = Normal::new(avg_freq, standard_deviation).unwrap();

        // Generate a normally distributed IAT
        for _ in 0..100 {
            let freq = normal.sample(&mut *self.rander.borrow_mut());
            if freq > 0.0 {
                return freq;
            }
        }
        panic!("Too many tries to generate a random frequency")
    }
    // 生成请求
    pub fn req_sim_gen_requests(&self) {
        let env = self;

        if env.core.current_frame() % REQUEST_GEN_FRAME_INTERVAL == 0 {
            let mut total_req_cnt = 0;

            for (dag_i, &(mut avg_frequency, cv)) in env.help.fn_call_frequency().iter() {
                if env.help.config().request_freq_low() {
                    avg_frequency *= 0.1;
                }
                else if env.help.config().request_freq_middle() {
                    avg_frequency *= 0.2;
                }
                else {
                    avg_frequency *= 0.3;
                }
                // avg_frequency *= 100.0;
                // avg_frequency *= 10.0;
                let mut bind = self.help.dag_accumulate_call_frequency.borrow_mut();
                let accum_freq = bind.entry(*dag_i).or_insert(0.0);
                let random_frequency = self.get_random_frequency(avg_frequency, cv) + *accum_freq;
                let req_cnt = random_frequency as usize;
                *accum_freq = random_frequency - (req_cnt as f64);

                total_req_cnt += req_cnt;


                for _ in 0..req_cnt {
                    let request = Request::new(env, *dag_i, env.core.current_frame());
                    let req_id = request.req_id;
                    env.core.requests_mut().insert(req_id, request);
                }
            }

            log::info!("Gen requests {total_req_cnt} at frame {}", env.current_frame());
        }

        //let env = self;

        // //每 REQUEST_GEN_FRAME_INTERVAL 帧生成一次请求
        // if *env.core.current_frame() % REQUEST_GEN_FRAME_INTERVAL == 0 {

        //     let scale =
        //     if env.help.config().dag_type_dag() {
        //         // log::info!("DAG类型");
        //         if env.help.config().request_freq_high() {
        //             30
        //         } else if env.help.config().request_freq_middle() {
        //             20
        //         } else {
        //             10
        //         }
        //     }
        //     else {
        //         // log::info!("其他类型");
        //         if env.help.config().request_freq_high() {
        //             120
        //         } else if env.help.config().request_freq_middle() {
        //             75
        //         } else {
        //             30
        //         }
        //     };

        //     // 根据 scale 的值和环境中存在的应用数量来生成一个随机数，用来确定要生成的请求的数量
        //     let req_cnt = env.env_rand_i(scale, scale * env.core.dags().len());

        //     // 对每一个请求：随机选择一个应用（dag_i）、创建一个新的请求对象、将新请求添加到环境对象的请求映射中
        //     for _ in 0..req_cnt {
        //         let dag_i = env.env_rand_i(0, env.core.dags().len() - 1);

        //         // 创建一个请求实例，一个请求相当于就是一个DAG
        //         let request = Request::new(env, dag_i, *env.core.current_frame());

        //         let req_id = request.req_id;
        //         env.core.requests_mut().insert(req_id, request);
        //     }

        //     log::info!("Gen requests {req_cnt} at frame {}", env.current_frame());
        // }
    }

    pub fn on_request_done(&self, req_id: ReqId) {
        let req = self.core.requests_mut().remove(&req_id).unwrap();
        self.help.metric_mut().add_done_request();
        self.core.done_requests_mut().push(req);
    }

    // 返回指定请求ID的不可变引用
    pub fn request<'a>(&'a self, i: ReqId) -> Ref<'a, Request> {
        let b = self.core.requests();

        Ref::map(b, |map| { map.get(&i).unwrap_or_else(|| panic!("request {} not found", i)) })
    }

    // 返回指定请求ID的可变引用
    pub fn request_mut<'a>(&'a self, i: ReqId) -> RefMut<'a, Request> {
        let b = self.core.requests_mut();

        RefMut::map(b, |map| {
            map.get_mut(&i).unwrap_or_else(|| panic!("request {} not found", i))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::env::set_var;

    use rand_seeder::rand_core::le;

    use crate::{
        actions::ESActionWrapper,
        config::Config,
        fn_dag::{ EnvFnExt, FnDAG },
        request::Request,
        sim_env::SimEnv,
        util,
    };

    #[test]
    fn test_request_metric() {
        let config = Config::new_test();
        // config.dag_type = "dag".to_owned();
        let sim_env = SimEnv::new(config);
        sim_env.core.dags_mut()[0] = FnDAG::instance_map_reduce(0, &sim_env, 4);
        let dag = sim_env.dag(0);

        sim_env.core.requests_mut().insert(0, Request::new(&sim_env, 0, 0));
        let mut req = sim_env.request_mut(0);

        let mut walker = dag.new_dag_walker();
        let mut walk_step = 0;
        while let Some(fngid) = walker.next(&dag.dag_inner) {
            let fnid = dag.dag_inner[fngid];
            let fnmetric = req.fn_metric.get_mut(&fnid).unwrap();
            if walk_step == 0 {
                // the first
                fnmetric.ready_sche_time = Some(0);
                fnmetric.sche_time = Some(0);
                fnmetric.fn_done_time = Some(5);
            } else if walk_step == 5 {
                // the last
                fnmetric.ready_sche_time = Some(10);
                fnmetric.sche_time = Some(11);
                fnmetric.fn_done_time = Some(15);
            } else {
                if walk_step == 2 {
                    fnmetric.ready_sche_time = Some(5);
                    fnmetric.sche_time = Some(5);
                    fnmetric.fn_done_time = Some(10);
                } else {
                    fnmetric.ready_sche_time = Some(5);
                    fnmetric.sche_time = Some(4);
                    fnmetric.fn_done_time = Some(8);
                }
            }
            walk_step += 1;
        }
        // println!("fn meteic:{:?}", req.fn_metric);
        req.init_metrics(&sim_env);

        assert_eq!(req.exe_time.unwrap(), 14);
        assert_eq!(req.wait_sche_time.unwrap(), 1);
        // let dag = sim_env.dag(0);
        // println!("dag node cnt: {}", dag.dag_inner.node_count());
    }
    // run dag for 100 frames
    #[test]
    fn test_req_30_frame() {
        set_var("RUST_LOG", "debug,error,warn,info");
        let _ = env_logger::builder().is_test(true).try_init();

        let mut runs = vec![];
        for i in 0..10 {
            println!("new run {}", i);
            let mut run = vec![];
            let mut config = Config::new_test();
            // config.dag_type = "dag".to_owned();
            config.total_frame = 30;
            let mut sim_env = SimEnv::new(config);

            let begin_req_cnt = 0;
            let mut begin_req_cnt1 = unsafe { util::non_null(&begin_req_cnt) };
            let begin_req_cnt2 = unsafe { util::non_null(&begin_req_cnt) };
            let frame_begin = move |env: &SimEnv| {
                log::info!("hook frame begin {}", env.current_frame());
                unsafe {
                    *begin_req_cnt1.0.as_mut() = env.core.requests().len();
                }
            };
            let mut run1 = unsafe { util::non_null(&run) };
            let req_gen = move |env: &SimEnv| {
                let aft_gen_req_cnt = env.core.requests().len();

                unsafe {
                    let cnt = aft_gen_req_cnt - *begin_req_cnt2.0.as_ref();
                    log::info!("hook req gen {} at {}", cnt, env.current_frame());
                    run1.0.as_mut().push(cnt);
                }
            };
            sim_env.step_es(
                ESActionWrapper::Int(0),
                Some(Box::new(frame_begin)),
                Some(Box::new(req_gen)),
                None,
                None
            );
            runs.push(run);
        }
        for r in &runs {
            log::info!("req gen seq {:?}", r);
        }
        for r in &runs[1..] {
            assert_eq!(r.len(), runs[0].len());
            for (i, frame_new) in r.iter().enumerate() {
                assert_eq!(runs[0][i], *frame_new);
            }
        }
    }
}
