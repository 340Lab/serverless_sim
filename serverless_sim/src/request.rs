use std::{
    borrow::Borrow, cell::{Ref, RefMut}, collections::{HashMap, HashSet}
};

use daggy::petgraph::visit::Topo;
use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Normal};


use crate::{
    fn_dag::{DagId, FnId},
    node::NodeId,
    sim_env::SimEnv,
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

pub struct Request {
    /// 请求id
    pub req_id: ReqId,

    /// 对应请求处理的DAG，由函数组成
    pub dag_i: DagId,

    /// 函数节点被调度到的机器节点
    pub fn_node: HashMap<FnId, NodeId>,

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
}

impl Request {
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
    }
    // 返回请求对应DAG中节点（函数）的数量
    pub fn fn_count(&self, env: &SimEnv) -> usize {
        env.core.dags()[self.dag_i].dag_inner.node_count()
    }
    // 判断请求是否已完成
    pub fn is_done(&self, env: &SimEnv) -> bool {
        self.done_fns.len() == self.fn_count(env)
    }
}

impl SimEnv {
    // 获取随机的 IAT 频率，用于模拟真实负载
    fn get_random_frequency(avg_freq: f64, cv: f64) -> f64 {
        // Calculate the standard deviation in terms of IAT
        let standard_deviation = avg_freq * cv;
        // Create a normal distribution with the given mean and standard deviation
        let normal = Normal::new(avg_freq, standard_deviation).unwrap();
    
        loop {
            // Generate a normally distributed IAT
            let freq = normal.sample(&mut thread_rng());
            // Ensure the IAT is greater than zero
            if freq > 0.0 {
                // Return the inverse of IAT to get the frequency
                return freq;
            }
        }
    }
    // 生成请求
    pub fn req_sim_gen_requests(&self) {
        
        let env = self;

        if *env.core.current_frame() % REQUEST_GEN_FRAME_INTERVAL == 0 {
            let mut total_req_cnt = 0;

            for (dag_i, &(avg_frequency, cv)) in env.help.fn_call_frequency().borrow().iter() {
                let random_frequency = Self::get_random_frequency(avg_frequency, cv);
                let req_cnt = random_frequency.round() as usize;

                total_req_cnt += req_cnt;

                println!("DAG Index: {}, Avg Frequency: {}, CV: {}, Random Frequency: {}, Request Count: {}", dag_i, avg_frequency, cv, random_frequency, req_cnt);

                for _ in 0..req_cnt {
                    let request = Request::new(env, *dag_i, *env.core.current_frame());
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

        Ref::map(b, |map| {
            map.get(&i)
                .unwrap_or_else(|| panic!("request {} not found", i))
        })
    }

    // 返回指定请求ID的可变引用
    pub fn request_mut<'a>(&'a self, i: ReqId) -> RefMut<'a, Request> {
        let b = self.core.requests_mut();

        RefMut::map(b, |map| {
            map.get_mut(&i)
                .unwrap_or_else(|| panic!("request {} not found", i))
        })
    }
}
