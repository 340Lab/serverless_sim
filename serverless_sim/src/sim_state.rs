// 关系图
//  - reqs:{
//      - req_id:
//      - dag_id:
// .    - dag_fns:{fn_id:,node:}  dag_fn 与 node 的映射关系
//      - done_fns:
//    }
//  - dags:{
//      - dag_id：
//      - dag图
// .  }
//  - node2node_speed: {node_a,node_b,speed}
//  - nodes:{
//        cpu:
// 。     mem：
// .  }

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    fn_dag::{DagId, FnDagInner, FnId},
    request::ReqId,
    sim_env::SimEnv,
};

use daggy::{petgraph::visit::Topo, Walker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct VecGraph {
    fn_id_2_index: BTreeMap<FnId, usize>,
    vec: Vec<Vec<f32>>,
}

impl VecGraph {
    fn new(graph: &FnDagInner) -> Self {
        let topo = Topo::new(graph);
        let mut fn_id_2_index = BTreeMap::new();
        let mut gvec = vec![];
        for _ in 0..graph.node_count() {
            gvec.push(vec![0f32; graph.node_count()]);
        }
        let mut cur_fn_index = 0;
        while let Some(node) = topo.next(graph) {
            let fn_id = graph.node_weight(node).unwrap();
            fn_id_2_index.insert(fn_id, cur_fn_index);
            let parents = graph.parents(node);
            for (edge, parent) in parents.iter(graph) {
                let parent_fn_id = graph.node_weight(parent).unwrap();
                let parent_i = *fn_id_2_index.get(&parent_fn_id).unwrap();
                gvec[cur_fn_index][parent_i] = *graph.edge_weight(edge).unwrap();
            }

            cur_fn_index += 1;
        }
        Self {
            fn_id_2_index,
            vec: gvec,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SerialFunc {
    fn_id: FnId,
    cpu: f32,
    mem: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct SerialDag {
    dag_id: DagId,
    dag_graph: VecGraph,
}

#[derive(Serialize, Deserialize, Debug)]
struct SerialRequest {
    req_id: ReqId,
    dag_id: DagId,
    dag_fn__node: BTreeMap<FnId, usize>,
    done_fns: BTreeSet<FnId>,
}
struct State {
    node2node_graph: Vec<Vec<f32>>,
    fns: Vec<SerialFunc>,
    dags: Vec<SerialDag>,
    requests: Vec<SerialRequest>,
}

impl SimEnv {
    fn state_node2node_graph(&self) -> Vec<Vec<f32>> {
        self.node2node_graph.clone()
    }
    fn state_fns(&self) -> Vec<SerialFunc> {
        let mut fns = vec![];
        for f in &self.fns {
            fns.push(SerialFunc {
                fn_id: f.unique_i,
                cpu: f.cpu,
                mem: f.mem,
            })
        }
        fns
    }
    fn state_dags(&self) -> Vec<SerialDag> {
        let mut dags = vec![];
        for (i, d) in self.dags.iter().enumerate() {
            let dag_topo_walker = Topo::new(&d.dag);
            let mut fn_id__index = BTreeMap::new();
            while let Some(gnode) = dag_topo_walker.next(&d.dag) {
                let fn_id = *d.dag.node_weight(gnode).unwrap();
                fn_id__index.insert(fn_id, fn_id__index.len());
            }
            dags.push(SerialDag {
                dag_id: i,
                dag_graph: VecGraph::new(&d.dag),
            })
        }
        dags
    }
    fn state_requests(&self) -> Vec<SerialRequest> {
        let mut reqs = vec![];
        for (req_id, req) in self.requests.iter() {
            let mut dag_fn__node = req.fn_node.iter().map(|(fid, nid)| (*fid, *nid)).collect();
            reqs.push(SerialRequest {
                req_id: req.req_id,
                dag_id: req.dag_i,
                dag_fn__node,
                done_fns: req.done_fns.iter().map(|fnid| *fnid).collect(),
            })
        }
        reqs
    }
    pub fn state(&self) -> String {
        serde_json::to_string(&State {
            node2node_graph: self.state_node2node_graph(),
            fns: self.state_fns(),
            dags: self.state_dags(),
            requests: self.state_requests(),
        })
        .unwrap()
    }
}
