use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    rc::Rc,
};

use daggy::{
    petgraph::visit::{Topo, Visitable},
    Dag, NodeIndex, Walker,
};
use rand::Rng;

fn rand_f(begin: f32, end: f32) -> f32 {
    let a = rand::thread_rng().gen_range(begin..end);
    a
}
fn rand_i(begin: usize, end: usize) -> usize {
    let a = rand::thread_rng().gen_range(begin..end);
    a
}

fn main() {
    println!("Hello, world!");
}

type NodeId = usize;
type FnId = usize;

struct FnDAG {
    begin: NodeIndex,
    dag: Dag<FnId, f32>,
}

impl FnDAG {
    fn new(begin_fn: FnId) -> Self {
        let mut dag = Dag::new();
        let begin = dag.add_node(begin_fn);
        Self { begin, dag }
    }

    fn instance_single_fn(env: &mut SimEnv) -> FnDAG {
        let begin_fn = Fn::rand_fn(env);
        let mut dag = FnDAG::new(begin_fn);
        dag
    }

    fn instance_map_reduce(env: &mut SimEnv, map_cnt: usize) -> FnDAG {
        let begin_fn = Fn::rand_fn(env);
        let mut dag = FnDAG::new(begin_fn);
        let end_fn = Fn::rand_fn(env);
        for i in 0..map_cnt {
            let mut next = Fn::rand_fn(env);
            let (_, next_i) = dag.dag.add_child(
                dag.begin,
                env.fns[begin_fn].out_put_size,
                next,
            );
            dag.dag.add_child(
                next_i,
                env.fns[next].out_put_size,
                end_fn,
            );
        }

        dag
    }
}

struct Fn {
    unique_i: usize,
    // #  #运算量/s 一个普通请求处理函数请求的运算量为1，
    cpu: f32, // 1
    // # 平均时间占用内存资源 mb
    mem: f32, // = 300
    // // # 依赖的数据库-整个过程中数据传输量
    // databases_2_throughput={}
    // # 输出数据量 mb
    out_put_size: f32, //=100,

    // 当前函数有实例的节点
    nodes: HashSet<usize>,
}

struct FnContainer{
    fn_id:FnId
}

impl Fn {
    fn rand_fn(env:&mut SimEnv) -> FnId {
        let id=env.alloc_fn_id();
        env.fns.push(Self {
            unique_i:id,
            cpu: rand_f(0.3, 10.0),
            mem: rand_f(100.0, 1000.0),
            out_put_size: rand_f(0.1, 20.0),
            nodes: HashSet::new(),
        });
        id
    }
}

struct NodeRscLimit {
    cpu: f32,
    mem: f32,
}

struct RequestPlan {
    dag_i: usize,

    fn_dag_walker: Topo<NodeIndex, <Dag<Fn, f32> as Visitable>::Map>,

    request_route_dag: Dag<Node, f32>,
}



struct Node {
    node_id: NodeId,
    // #数据库容器
    // # databases

    // # #函数容器
    // # functions

    // # #serverless总控节点
    // # serverless_controller

    // #资源限制：cpu, mem
    rsc_limit: NodeRscLimit,

    // 当前资源使用情况
    cpu: f32,
    mem: f32,

    fn_containers:Vec<FnContainer>
}

impl Node {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            rsc_limit: NodeRscLimit {
                cpu: 1000.0,
                mem: 1000.0,
            },
            cpu: 0.0,
            mem: 0.0,
            fn_containers:HashMap::default()
        }
    }
}

enum Action {
    /// 选择一个节点放当前函数节点可以延迟最小
    ExpandGreedy,
    /// 随机选择一个节点放当前函数节点
    ExpandRandom,
    /// 随机选择一个节点缩容
    ShrinkRandom,
    /// 选择一个调用频率较小，函数实例最多的缩容
    ShrinkRuleBased,
    /// 不做操作
    DoNothing,
}

struct SimEnv {
    nodes: Vec<Node>,

    // 节点间网速图
    node2node_graph: Vec<Vec<f32>>,

    // databases=[]

    // # dag应用
    dags: Vec<FnDAG>,

    fn_next_id: usize,


    fn_2_nodes: HashMap<FnId, HashSet<NodeId>>,

    fns: Vec<Fn>,

    current_frame: usize,

    scheduling_request: Option<RequestPlan>,

    allocing_requsts: Vec<usize>,

    executing_requsts: Vec<RequestPlan>,

}

impl SimEnv {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            node2node_graph: Vec::new(),
            dags: Vec::new(),
            fn_next_id: 0,
            current_frame: 0,
            scheduling_request: None,
            allocing_requsts: Vec::new(),
            executing_requsts: Vec::new(),
            fn_2_nodes: HashMap::new(),
        }
    }

    fn alloc_fn_id(&mut self) -> usize {
        let ret = self.fn_next_id;
        self.fn_next_id += 1;
        ret
    }

    /// 设置节点间网速
    /// - speed: MB/s
    fn set_speed_btwn(&mut self, n1: usize, n2: usize, speed: f32) {
        assert!(n1 != n2);
        fn _set_speed_btwn(env: &mut SimEnv, nbig: usize, nsmall: usize, speed: f32) {
            env.node2node_graph[nbig][nsmall] = speed;
        }
        if n1 > n2 {
            _set_speed_btwn(self, n1, n2, speed);
        } else {
            _set_speed_btwn(self, n2, n1, speed);
        }
    }

    /// 获取节点间网速
    /// - speed: MB/s
    fn get_speed_btwn(&mut self, n1: usize, n2: usize) -> f32 {
        let _get_speed_btwn = |nbig: usize, nsmall: usize| self.node2node_graph[nbig][nsmall];
        if n1 > n2 {
            _get_speed_btwn(n1, n2)
        } else {
            _get_speed_btwn(n2, n1)
        }
    }

    fn init(&mut self) {
        fn _init_one_node(env: &mut SimEnv, node_id: NodeId) {
            let nodecnt: usize = env.nodes.len();
            let node = Node::new(node_id);
            // let node_i = nodecnt;
            env.nodes.push(node);

            let nodecnt: usize = env.nodes.len();
            println!("nodecnt {}", nodecnt);

            for i in 0..nodecnt - 1 {
                let randspeed = rand_f(8000.0, 10000.0);
                env.set_speed_btwn(i, nodecnt - 1, randspeed);
            }
        }

        // # init nodes graph
        let dim = 10;
        self.node2node_graph = vec![vec![0.0; 10]; 10];
        for i in 0..dim {
            _init_one_node(self, i);
        }

        // # # init databases
        // # databases_cnt=5
        // # for i in range(databases_cnt):
        // #     db=DataBase()
        // #     # bind a database to node
        // #     while True:
        // #         rand_node_i=random.randint(0,dim-1)
        // #         if self.nodes[rand_node_i].database==None:
        // #             self.nodes[rand_node_i].database=db
        // #             db.node=self.nodes[rand_node_i]
        // #             break
        // #     self.databases.append(db)

        // # init dags
        for _ in 0..50 {
            let dag = FnDAG::instance_map_reduce(self, rand_i(2, 10));
            self.dags.push(dag);
        }

        for _ in 0..50 {
            let dag = FnDAG::instance_single_fn(self);
            self.dags.push(dag);
        }
    }

    fn find_next_fn_to_schedule(&self, req_plan: &RequestPlan) {}

    fn set_expand_result(&mut self, fn_id: FnId,node_id:NodeId) {
        // 1. 更新 fn 到nodes的map，用于查询fn 对应哪些节点有部署
        self.fn_2_nodes
            .entry(node_id)
            .and_modify(|v| {
                v.insert(fn_id);
            })
            .or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(fn_id);
                set
            });

        self.nodes[node_id].fn_containers.push(FnContainer{
            fn_id
        });
    }

    fn find_the_most_idle_node(&self) -> NodeId {
        const CPU_SCORE_WEIGHT: f32 = 0.5;
        const MEM_SCORE_WEIGHT: f32 = 0.5;
        self.nodes
            .iter()
            .min_by(|a, b| {
                if a.cpu * CPU_SCORE_WEIGHT + a.mem * MEM_SCORE_WEIGHT
                    > b.cpu * CPU_SCORE_WEIGHT + b.mem * MEM_SCORE_WEIGHT
                {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            })
            .unwrap()
            .node_id
    }

    fn expand_greedy(&mut self, cur_req_dag_i: usize, fn_node_id: NodeIndex) -> NodeId {
        let parents = self.dags[cur_req_dag_i].dag.parents(fn_node_id);
        let children = self.dags[cur_req_dag_i].dag.children(fn_node_id);
        let mut parent_fns: Vec<FnId> = vec![];
        let mut child_fns: Vec<FnId> = vec![];
        for (_edge_i, node_i) in parents.iter(&self.dags[cur_req_dag_i].dag) {
            parent_fns.push(self.dags[cur_req_dag_i].dag[node_i].unique_i);
        }
        for (_edge_i, node_i) in children.iter(&self.dags[cur_req_dag_i].dag) {
            child_fns.push(self.dags[cur_req_dag_i].dag[node_i].unique_i);
        }
        if parent_fns.len() == 0 && child_fns.len() == 0 {
            // 扩容fn到资源最多的节点
            let most_idle_node: NodeId = self.find_the_most_idle_node();
            self.set_expand_result(,most_idle_node)
        } else {
            // 找到所有关联 fn 到放置位置最短 的节点扩容fn
        }
    }
    fn schedule_req_plan_after_expand(&mut self, mut req_plan: RequestPlan) {}
    /// 继续确定当前请求应该放到哪些节点上
    fn schedule(&mut self, action: Action, mut req_plan: RequestPlan) {
        if let Some(next) = req_plan.fn_dag_walker.next(&self.dags[req_plan.dag_i].dag) {
            let (score, state, done) = match action {
                Action::ExpandGreedy => {
                    self.expand_greedy(next);
                    self.schedule_req_plan_after_expand(req_plan);
                }
                Action::ExpandRandom => {
                    // 随机选择一个节点,随机选择一个函数放置或者扩容
                    self.schedule_req_plan_after_expand(req_plan);
                }
                Action::ShrinkRandom => {
                    // 随机选择一个节点,随机选择一个函数实例缩容
                }
                Action::ShrinkRuleBased => {}
                Action::DoNothing => {}
            };
        } else {
            // # 说明已经完成了
            self.executing_requsts.push(req_plan);
        }
    }

    fn step(&mut self, action: Action) {
        if self.allocing_requsts.len() == 0 {
            // # 生成新的请求
            let req_cnt = rand_i(2, self.dags.len());
            let mut reqs = vec![];
            for _ in 0..req_cnt {
                reqs.push(rand_i(0, self.dags.len() - 1));
            }
            self.allocing_requsts = reqs;
        }
        //没有正在调度的请求了，分配一个正在调度的请求
        if self.scheduling_request == None {
            let dag_i = self.allocing_requsts.pop().unwrap();
            let topo = Topo::new(&self.dags[dag_i].dag);
            self.scheduling_request = Some(RequestPlan {
                dag_i,
                fn_dag_walker: topo,
                request_route_dag: Dag::new(),
            });
        }
        if let Some(scheduling_request) = self.scheduling_request.take() {
            self.schedule(action, scheduling_request);
        }

        // 一个帧结束
        if self.allocing_requsts.len() == 0 {
            self.current_frame += 1;

            // 仿真执行一个帧
            self.execute();

            // 统计完成的请求
            let done_reqs = self.collect_execute_done_requests();
        }
    }
}
