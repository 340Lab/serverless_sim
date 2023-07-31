use std::collections::HashMap;

use crate::{
    fn_dag::{FnId, FnRunningState},
    node::NodeId,
    request::ReqId,
    sim_env::SimEnv,
};

#[derive(Clone)]
struct TransPath {
    // from_node_id: NodeId,
    // to_node_id: NodeId,
    req_id: ReqId,
    fn_id: FnId,
}

struct NodeTrans {
    send_paths: Vec<TransPath>,
    recv_paths: Vec<TransPath>,
}

impl NodeTrans {
    fn path_cnt(&self) -> usize {
        self.send_paths.len() + self.recv_paths.len()
    }
}

type NodeTransMap = HashMap<(NodeId, NodeId), NodeTrans>;

impl SimEnv {
    fn try_put_fn(&mut self) {
        //针对所有请求，将请求的fn放到可以放的fn容器中，

        // 要求进入的fn容器需要离前驱fn所进入的容器所在节点尽可能近，

        // 能进入的前提是，
        //  1.前驱fn已经执行完了
        //  2.fn instance 存在
        for (_req_id, req) in &mut self.requests {
            if let Some((current_f, fn_g_i)) = req.dag_current_fn() {
                //对应请求还有未调度的fn
                let parents = self
                    .fn_ops()
                    .get_fn_dag_mut(req.dag_i)
                    .get_parent_fns(fn_g_i);
                let mut all_done = true;
                for p in &parents {
                    if !req.done_fns.contains(p) {
                        //前驱fn还没执行完
                        all_done = false;
                        break;
                    }
                }
                let fn_nodes = self.node_ops().get_fn_relate_nodes(&current_f);
                if all_done && fn_nodes.is_some() {
                    // 将请求对应fn扔到node上进行执行
                    let node_to_run_req_fn: FnId =
                        if self.fn_ops().is_fn_dag_begin(req.dag_i, current_f) {
                            // 若为dag 第一个f，采用负载均衡原则来选择位置
                            self.algo_find_the_most_idle_node_for_fn(current_f)
                        } else {
                            // 若为dag 其他f，与前置f所在node越近越好
                            self.algo_find_the_most_fast_node_for_fn(parent_fns, child_fns)
                        };

                    let new_fn_running = self.fn_ops().new_fn_running_state(req, fn_g_i, current_f);
                    self.nodes[node_to_run_req_fn]
                        .fn_containers
                        .get(&current_f)
                        .unwrap()
                        .req_fn_state
                        .insert(req.req_id, new_fn_running);
                    req.fn_node.insert(current_f, node_to_run_req_fn);
                } else {
                    req.topo_walk_dag(&self.dags[req.dag_i].dag);
                }
            }
        }
    }

    fn sim_transfer_btwn_nodes(
        &mut self,
        node_a: NodeId,
        node_b: NodeId,
        transmap: &mut NodeTransMap,
    ) {
        assert_ne!(node_a, node_b);
        // 两个node之间的数据传输
        let a2b = transmap.remove(&mut (node_a, node_b)).unwrap();
        let b2a = transmap.remove(&mut (node_b, node_a)).unwrap();
        let total_bandwith = self.node_ops().get_speed_btwn(node_a, node_b);
        let each_path_bandwith = total_bandwith / (a2b.path_cnt() as f32);

        let updata_trans = |env: &mut SimEnv, from: NodeId, to: NodeId, t: &TransPath| {
            let (all, recved) = env.nodes[node_b]
                .fn_containers
                .get_mut(&t.fn_id)
                .unwrap()
                .req_fn_state
                .get_mut(&t.req_id)
                .unwrap()
                .data_recv
                .get_mut(&node_a)
                .unwrap();
            if *all >= *recved {
                // 该数据已经传输完毕
            } else {
                *recved += each_path_bandwith;
            }
        };

        // a，b之间单个任务的传输速度，取决于a，b间的路径数
        for t in a2b.send_paths {
            // a2b
            updata_trans(self, node_a, node_b, &t);
        }

        for t in a2b.recv_paths {
            updata_trans(self, node_b, node_a, &t);
        }
    }

    fn sim_transfers(&mut self) {
        // 收集所有node向其他函数发送和接收的路径数，这样每个接收函数可以知道从远程node收到多少数据，
        // 模拟传输时，一个一个路径遍历过来，
        //   两边一定有一个网速更快，那么选择慢的；然后快的那边可以把带宽分给其他的传输路径
        //
        let mut node2node_trans: NodeTransMap = HashMap::new();
        for x in 0..self.nodes.len() {
            for y in 0..self.nodes.len() {
                if x != y {
                    node2node_trans.insert(
                        (x, y),
                        NodeTrans {
                            send_paths: vec![],
                            recv_paths: vec![],
                        },
                    );
                }
            }
        }

        // go through all the fn task scheduled on node, and collect the transfer paths
        for node in &mut self.nodes {
            for (fnid, fn_container) in &mut node.fn_containers {
                for (req_id, fnrun) in &mut fn_container.req_fn_state {
                    for (send_node, (all, recved)) in &mut fnrun.data_recv {
                        // 数据还没接受完才需要传输
                        if *recved < *all {
                            if *send_node == node.node_id {
                                // data transfer on same node can be done immediately
                                *recved = *all + 0.001;
                            } else {
                                let path = TransPath {
                                    req_id: *req_id,
                                    fn_id: *fnid,
                                };

                                let send_2_recv = node2node_trans
                                    .get_mut(&(*send_node, node.node_id))
                                    .unwrap();
                                send_2_recv.send_paths.push(path.clone());

                                let recv_2_send = node2node_trans
                                    .get_mut(&(node.node_id, *send_node))
                                    .unwrap();
                                recv_2_send.recv_paths.push(path.clone());
                            }
                        }
                    }
                }
            }
        }
        // go through all the transfer paths, and simulate the transfer
        for x in 0..self.nodes.len() {
            for y in 0..self.nodes.len() {
                if x > y {
                    // simu transfer between node x and y
                    self.sim_transfer_btwn_nodes(x, y, &mut node2node_trans);
                }
            }
        }
    }

    fn sim_computes(&mut self) {
        for n in &self.nodes {
            let mut fn_task_cnt = 0;

            for (_fid, fc) in &n.fn_containers {
                for (fnid, fn_running_state) in &fc.req_fn_state {
                    if fn_running_state.data_recv_done() && !fn_running_state.compute_done() {
                        fn_task_cnt += 1;
                    }
                }
            }

            if fn_task_cnt == 0 {
                continue;
            }

            // 计算任务数，每个任务平分计算量
            let each_fn_cpu = n.cpu / (fn_task_cnt as f32);

            for (fnid, fc) in &n.fn_containers {
                let mut done_reqs = vec![];
                for (reqid, fn_running_state) in &fc.req_fn_state {
                    if fn_running_state.data_recv_done() {
                        fn_running_state.left_calc -= each_fn_cpu;
                    }
                    if fn_running_state.compute_done() {
                        done_reqs.push(*reqid);
                    }
                }
                for reqid in done_reqs {
                    fc.req_fn_state.remove(&reqid).unwrap();
                    self.requests
                        .get_mut(&reqid)
                        .unwrap()
                        .fn_done(*fnid, self.current_frame);
                }
            }
        }
    }

    fn sim_run(&mut self) {
        self.sim_transfers();
        self.sim_computes();
    }
    pub fn schedule_fn(&mut self) {
        self.try_put_fn();
        self.sim_run();
    }
}
