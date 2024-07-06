use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    mem::{size_of, zeroed},
    ptr::{self, NonNull},
};

use crate::sim_env::SimEnv;
use priority_queue::PriorityQueue;
use rand::Rng;
// use windows::Win32::{
//     Foundation::FILETIME,
//     System::Threading::{GetCurrentThread, GetThreadTimes, INFINITE},
// };
// use rand::Rng;

// pub fn rand_f(begin: f32, end: f32) -> f32 {
//     let a = rand::thread_rng().gen_range(begin..end);
//     a
// }
// pub fn rand_i(begin: usize, end: usize) -> usize {
//     let a = rand::thread_rng().gen_range(begin..end);
//     a
// }

#[derive(Clone)]
// 滑动窗口
pub struct Window {
    // 存储的浮点数
    pub queue: VecDeque<f32>,

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

    // pub fn critical_path_common<N>(next_node: impl Fn() -> Option<N>) {
    //     while let Some(n) = next_node() {

    //     }
    // }
    // pub fn aoa_critical_path<N>(dag: &Dag<N, f32>) -> Vec<NodeIndex> {

    // }

    /// Notive, for aoe graph, the critical path is the longest path
    pub fn aoe_critical_path<N>(dag: &Dag<N, f32>) -> Vec<NodeIndex> {
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
                let dist_info = dists.get_mut(&neighbor).unwrap();
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

pub fn now_ms() -> u64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
    // unsafe {
    //     let h_thread = GetCurrentThread();
    //     let mut creation_time: FILETIME = zeroed();
    //     let mut exit_time: FILETIME = zeroed();
    //     let mut kernel_start: FILETIME = zeroed();
    //     let mut user_start: FILETIME = zeroed();
    //     // let mut kernel_end: FILETIME = zeroed();
    //     // let mut user_end: FILETIME = zeroed();
    //     // Get initial thread times
    //     GetThreadTimes(
    //         h_thread,
    //         &mut creation_time,
    //         &mut exit_time,
    //         &mut kernel_start,
    //         &mut user_start
    //     );

    //     user_start_u64.
    // }
}

// pub struct MeasureThreadTime {
//     kernel_start: FILETIME,
//     user_start: FILETIME,
// }

// fn filetime_to_u64(ft: FILETIME) -> u64 {
//     ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64)
// }

// impl MeasureThreadTime {
//     pub fn new() -> Self {
//         unsafe {
//             let h_thread = GetCurrentThread();
//             let mut creation_time: FILETIME = zeroed();
//             let mut exit_time: FILETIME = zeroed();
//             let mut kernel_start: FILETIME = zeroed();
//             let mut user_start: FILETIME = zeroed();
//             // let mut kernel_end: FILETIME = zeroed();
//             // let mut user_end: FILETIME = zeroed();
//             // Get initial thread times
//             GetThreadTimes(
//                 h_thread,
//                 &mut creation_time,
//                 &mut exit_time,
//                 &mut kernel_start,
//                 &mut user_start,
//             )
//             .unwrap();

//             log::info!(
//                 "thread_start: kernel_start={:?}, user_start={:?}",
//                 kernel_start,
//                 user_start
//             );

//             // user_start_u64.
//             Self {
//                 kernel_start,
//                 user_start,
//             }
//         }
//     }
//     pub fn passed_100ns(&self) -> (u64, u64) {
//         unsafe {
//             let h_thread = GetCurrentThread();
//             let mut creation_time: FILETIME = zeroed();
//             let mut exit_time: FILETIME = zeroed();
//             let mut kernel_end: FILETIME = zeroed();
//             let mut user_end: FILETIME = zeroed();
//             // let mut kernel_end: FILETIME = zeroed();
//             // let mut user_end: FILETIME = zeroed();
//             // Get initial thread times
//             GetThreadTimes(
//                 h_thread,
//                 &mut creation_time,
//                 &mut exit_time,
//                 &mut kernel_end,
//                 &mut user_end,
//             )
//             .unwrap();

//             log::info!(
//                 "thread_end: kernel_end={:?}, user_end={:?}",
//                 kernel_end,
//                 user_end
//             );

//             let kernel_start_u64 = filetime_to_u64(self.kernel_start);
//             let user_start_u64 = filetime_to_u64(self.user_start);
//             let kernel_end_u64 = filetime_to_u64(kernel_end);
//             let user_end_u64 = filetime_to_u64(user_end);

//             (
//                 kernel_end_u64 - kernel_start_u64,
//                 user_end_u64 - user_start_u64,
//             )
//         }
//     }
// }

// pub fn now_ns() -> u128 {
//     let duration_since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
//     let timestamp_nanos = duration_since_epoch.as_nanos(); // u128
//     timestamp_nanos
// }

pub struct SendNonNull<T>(pub NonNull<T>);

// impl<T> Deref for SendNonNull<T> {}

unsafe impl<T> Send for SendNonNull<T> {}

pub unsafe fn non_null<T>(v: &T) -> SendNonNull<T> {
    let ptr = v as *const T as *mut T;
    let non_null = NonNull::new_unchecked(ptr);
    SendNonNull(non_null)
}

// use windows::core::*;
// use windows::Win32::Foundation::*;
// use windows::Win32::System::Diagnostics::Etw::*;
// use std::ptr::null_mut;

// unsafe extern "system" fn event_record_callback(event_record: *mut EVENT_RECORD) {
//     if (*event_record).EventHeader.EventDescriptor.Opcode == 36 {
//         println!("Context Switch Event Captured");
//     }
// }

// const KERNEL_PROVIDER_GUID: GUID = GUID::from_u128(0x9e814aad_3204_11d2_9a82_006008a86939);

// pub fn stop_trace(session_name: PCWSTR) {
//     unsafe {
//         let mut properties: EVENT_TRACE_PROPERTIES = zeroed();
//         properties.Wnode.BufferSize = size_of::<EVENT_TRACE_PROPERTIES>() as u32;

//         let mut session_handle: CONTROLTRACE_HANDLE = CONTROLTRACE_HANDLE { Value: 0 };
//         // let session_name_wide: Vec<u16> = session_name.encode_utf16().collect();

//         let status = ControlTraceW(
//             session_handle,
//             session_name,
//             &mut properties,
//             EVENT_TRACE_CONTROL_STOP
//         );

//         // if status != ERROR_SUCCESS {
//         //     if status == ERROR_WMI_INSTANCE_NOT_FOUND {
//         //         // No existing trace to stop, this is not an error in this context.
//         //         Ok(())
//         //     } else {
//         //         println!("ControlTrace (stop) failed with error code: {}", status);
//         //         Err(windows::core::Error::from_win32())
//         //     }
//         // } else {
//         //     Ok(())
//         // }
//     }
// }

// pub fn entry_trace() -> Result<()> {
//     unsafe {
//         // let mut properties: EVENT_TRACE_PROPERTIES = unsafe { zeroed() };
//         // let mut session_handle: CONTROLTRACE_HANDLE = CONTROLTRACE_HANDLE {
//         //     Value: 0,
//         // };
//         // let mut buffer: [u16; 1024] = [0; 1024];
//         // let session_name = KERNEL_LOGGER_NAMEW;

//         // stop_trace(session_name);

//         // properties.Wnode.BufferSize =
//         //     (size_of::<EVENT_TRACE_PROPERTIES>() as u32) +
//         //     ((buffer.len() * size_of::<u16>()) as u32);
//         // properties.Wnode.Flags = WNODE_FLAG_TRACED_GUID;
//         // properties.Wnode.ClientContext = 1;
//         // properties.Wnode.Guid = SystemTraceControlGuid;
//         // properties.LogFileMode = 0x00000100; // EVENT_TRACE_REAL_TIME_MODE
//         // properties.LoggerNameOffset = size_of::<EVENT_TRACE_PROPERTIES>() as u32;
//         // properties.EnableFlags = EVENT_TRACE_FLAG_CSWITCH;

//         unsafe {
//             // let status = StartTraceW(&mut session_handle, session_name, &mut properties);
//             // if let Err(e) = status {
//             //     println!("StartTrace failed with {}", e);
//             //     return Err(windows::core::Error::from_win32());
//             // }

//             // let status = EnableTraceEx2(
//             //     session_handle,
//             //     &KERNEL_PROVIDER_GUID,
//             //     EVENT_CONTROL_CODE_ENABLE_PROVIDER.0,
//             //     TRACE_LEVEL_INFORMATION as u8,
//             //     0,
//             //     0,
//             //     0,
//             //     None
//             // );
//             // if let Err(e) = status {
//             //     println!("EnableTraceEx2 failed with {:?}", e);
//             //     return Err(windows::core::Error::from_win32());
//             // }

//             let mut logfile: EVENT_TRACE_LOGFILEW = zeroed();
//             logfile.LogFileName = PWSTR::from_raw(&"spotless-tracing.etl" as *const _ as *mut _);
//             // logfile.LoggerName = PWSTR::from_raw(session_name.as_ptr() as *mut _);
//             logfile.Anonymous1.ProcessTraceMode = 0x00000100 | 0x00000010; // PROCESS_TRACE_MODE_REAL_TIME | PROCESS_TRACE_MODE_EVENT_RECORD
//             logfile.Anonymous2.EventRecordCallback = Some(event_record_callback);
//             logfile.IsKernelTrace = 1;

//             let trace_handle = OpenTraceW(&mut logfile);
//             if trace_handle.Value == 0xffffffffffffffff {
//                 println!("OpenTrace failed with {}", windows::core::Error::from_win32());
//                 return Err(windows::core::Error::from_win32());
//             }

//             let status = ProcessTrace(&[trace_handle], None, None);
//             if let Err(e) = status {
//                 println!("ProcessTrace failed with {:?}", e);
//                 return Err(windows::core::Error::from_win32());
//             }
//         }
//     }
//     Ok(())
// }
