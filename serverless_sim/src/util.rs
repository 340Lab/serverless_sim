use std::collections::{HashMap, HashSet, VecDeque};

use priority_queue::PriorityQueue;
use rand::Rng;

use crate::sim_env::SimEnv;
// use rand::Rng;

// pub fn rand_f(begin: f32, end: f32) -> f32 {
//     let a = rand::thread_rng().gen_range(begin..end);
//     a
// }
// pub fn rand_i(begin: usize, end: usize) -> usize {
//     let a = rand::thread_rng().gen_range(begin..end);
//     a
// }

// 滑动窗口
pub struct Window {
    // 存储的浮点数
    queue: VecDeque<f32>,

    // 窗口容量
    cap: usize,
}

impl Window {
    pub fn new(cap: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            cap,
        }
    }
    pub fn push(&mut self, ele: f32) {
        self.queue.push_back(ele);
        if self.queue.len() > self.cap {
            self.queue.pop_front();
        }
    }
    pub fn avg(&self) -> f32 {
        if self.queue.is_empty() {
            return 0.0;
        }
        let sum: f32 = self.queue.iter().sum();
        sum / (self.queue.len() as f32)
    }
}

pub fn to_range(r: f32, begin: usize, end: usize) -> usize {
    let mut v: usize = unsafe { ((begin as f32) + ((end - begin) as f32) * r).to_int_unchecked() };
    if v < begin {
        v = begin;
    }
    if v > end {
        v = end;
    }
    v
}

pub fn in_range(n: usize, begin: usize, end: usize) -> usize {
    if n < begin {
        begin
    } else if n > end {
        end
    } else {
        n
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OrdF32(pub f32);
impl PartialEq for OrdF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

pub mod graph {
    use super::*;
    use daggy::{
        petgraph::visit::{Topo, Visitable},
        Dag, NodeIndex, Walker,
    };

    pub fn new_dag_walker<N, E>(dag: &Dag<N, E>) -> Topo<NodeIndex, <Dag<N, E> as Visitable>::Map> {
        Topo::new(dag)
    }

    // 逆拓扑
    fn new_inverse_dag<N: Clone, E: Clone>(dag: &Dag<N, E>) -> Dag<N, E> {
        let mut inverse_dag = Dag::new();
        let mut walker = new_dag_walker(dag);
        while let Some(node) = walker.next(dag) {
            inverse_dag.add_node(dag[node].clone());
            let mut parents = dag.parents(node);
            while let Some((e, p)) = parents.walk_next(dag) {
                // let p = nodes.entry(p).or_insert_with(|| inverse_dag.add_node(dag[p]));
                inverse_dag
                    .add_edge(node, p, dag.edge_weight(e).unwrap().clone())
                    .unwrap();
            }
        }
        inverse_dag
    }

    // pub fn dag_edges<N>(dag: &Dag<N>) -> HashMap<(NodeIndex, NodeIndex), EdgeIndex> {
    //     let mut edges = HashMap::new();
    //     let mut walker = new_dag_walker(dag);
    //     while let Some(node) = walker.next(dag) {
    //         let mut parents = dag.parents(node);
    //         while let Some((e, p)) = parents.walk_next(dag) {
    //             edges.insert(dag[p].clone(), (dag[node].clone(), e));
    //         }
    //     }
    //     edges
    // }

    /// Notive, for aoe graph, the critical path is the longest path
    pub fn critical_path<N>(dag: &Dag<N, f32>) -> Vec<NodeIndex> {
        // 求关键路径
        // 1. 求拓扑排序
        let mut walker = new_dag_walker(dag);
        // 2. 求最早开始时间
        let mut early_start_time: HashMap<NodeIndex, (f32, Option<NodeIndex>)> = HashMap::new();
        let mut last_node = None;
        while let Some(node) = walker.next(&dag) {
            let mut max_time: f32 = 0.0;
            let mut prev = None;
            let mut parents = dag.parents(node);
            while let Some((e, p)) = parents.walk_next(&dag) {
                let time = early_start_time.get(&p).unwrap().0 + dag.edge_weight(e).unwrap();
                if time > max_time {
                    max_time = time;
                    prev = Some(p);
                }
            }
            early_start_time.insert(node, (max_time, prev));
            last_node = Some(node);
        }
        let mut path = vec![last_node.unwrap()];
        while let Some(prev) = early_start_time.get(&last_node.unwrap()).unwrap().1 {
            path.push(prev);
            last_node = Some(prev);
        }
        path.reverse();
        path
    }
}

#[allow(dead_code)]
pub struct DirectedGraph {
    node2nodes: HashMap<usize, HashSet<usize>>,
}
#[allow(dead_code)]
impl DirectedGraph {
    pub fn new() -> Self {
        Self {
            node2nodes: HashMap::new(),
        }
    }
    pub fn add(&mut self, n: usize) {
        self.node2nodes.entry(n).or_insert(HashSet::new());
    }
    pub fn add_a_after_b(&mut self, a: usize, b: usize) {
        self.add(a);
        self.node2nodes.entry(b).and_modify(|set| {
            set.insert(a);
        });
    }

    // return path
    pub fn find_min<F: Fn(usize, usize) -> f32>(
        &self,
        a: usize,
        b: usize,
        a2bdist: F,
    ) -> Vec<usize> {
        let mut visited = HashSet::new();
        let mut dists = HashMap::new(); // tostart_dist, prev_node
        let mut priority_queue = PriorityQueue::new();
        for (&n, _ns) in &self.node2nodes {
            dists.insert(n, (f32::MAX, None));
        }
        dists.entry(a).and_modify(|v| {
            v.0 = 0.0;
        });
        priority_queue.push(a, OrdF32(0.0));
        while let Some((node, dist)) = priority_queue.pop() {
            let dist = dist.0;
            if visited.contains(&node) {
                continue;
            }
            let neighbors = self.node2nodes.get(&node).unwrap();
            for &neighbor in neighbors {
                let weight = a2bdist(node, neighbor);
                let distance_through_current = dist + weight;
                let mut dist_info = dists.get_mut(&neighbor).unwrap();
                if distance_through_current < dist_info.0 {
                    dist_info.0 = distance_through_current;
                    dist_info.1 = Some(node);
                    // println!("push neighbor{}", neighbor);
                    priority_queue.push(neighbor, OrdF32(distance_through_current));
                }
            }
            // if node == b {
            //     break;
            // }
            visited.insert(node);
        }
        let mut res = vec![b];
        let mut current = b;
        while let Some(prev) = dists.get(&current).unwrap().1.clone() {
            res.push(prev);
            current = prev;
        }
        res
    }
}

impl SimEnv {
    /// in range of [min, max)
    pub fn env_rand_i(&self, min: usize, max: usize) -> usize {
        let mut rng = self.rander.borrow_mut();
        rng.gen_range(min..max)
    }
    /// in range of [min, max)
    pub fn env_rand_f(&self, min: f32, max: f32) -> f32 {
        let mut rng = self.rander.borrow_mut();
        rng.gen_range(min..max)
    }
}
