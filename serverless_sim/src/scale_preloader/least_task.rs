use super::ScalePreLoader;
use crate::{fn_dag::FnId, sim_env::SimEnv};

pub struct LeastTaskPreLoader;

impl LeastTaskPreLoader {
    pub fn new() -> Self {
        LeastTaskPreLoader {}
    }
}

impl ScalePreLoader for LeastTaskPreLoader {
    fn pre_load(&self, target_cnt: usize, fnid: FnId, env: &SimEnv) {
        let mut nodes_no_container = env
            .nodes()
            .iter()
            .filter(|n| n.container(fnid).is_none())
            .map(|n| n.node_id())
            .collect::<Vec<_>>();

        let nodes_with_container_cnt = env.nodes().len() - nodes_no_container.len();
        if nodes_with_container_cnt < target_cnt {
            let to_scale_up_cnt = target_cnt - nodes_with_container_cnt;
            nodes_no_container.sort_by(|&a, &b| {
                env.node(a)
                    .all_task_cnt()
                    .partial_cmp(&env.node(b).all_task_cnt())
                    .unwrap()
            });
            nodes_no_container.reverse();
            for _ in 0..to_scale_up_cnt {
                let node_2_load_contaienr = nodes_no_container.pop().unwrap();
                env.node(node_2_load_contaienr)
                    .try_load_spec_container(fnid, env);
            }
        }
        //         let parent_fns = env.func(fnid).parent_fns(env);

        //         if parent_fns.len() > 0 {
        //             nodes.sort_by(|&a, &b| {
        //                 node_score(&parent_fns, a)
        //                     .partial_cmp(&node_score(&parent_fns, b))
        //                     .unwrap()
        //             });
        //             // score 大的先被pop
        //         } else {
        //             // 按任务数排序，从小
        //             nodes.sort_by(|&a, &b| {
        //                 env.node(a)
        //                     .all_task_cnt()
        //                     .partial_cmp(&env.node(b).all_task_cnt())
        //                     .unwrap()
        //             });

        //             // 从大到小
        //             nodes.reverse();
        //         }

        // while nodes_with_container.len() < scheable_node_count {
        //     let node_2_load_contaienr = nodes.pop().unwrap();
        //     env.node(node_2_load_contaienr)
        //         .try_load_spec_container(fnid, env);
        //     nodes_with_container.push(node_2_load_contaienr);
        // }
    }
}
