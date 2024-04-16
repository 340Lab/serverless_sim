use super::ScaleUpExec;
use crate::{fn_dag::FnId, mechanism::UpCmd, sim_env::SimEnv};

pub struct LeastTaskScaleUpExec;

impl LeastTaskScaleUpExec {
    pub fn new() -> Self {
        LeastTaskScaleUpExec {}
    }
}

impl ScaleUpExec for LeastTaskScaleUpExec {
    fn exec_scale_up(&self, target_cnt: usize, fnid: FnId, env: &SimEnv) -> Vec<UpCmd> {
        let mech_metric = || env.help.mech_metric_mut();
        let mut up_cmds = vec![];

        let mut nodes_no_container = env
            .nodes()
            .iter()
            .filter(|n| n.container(fnid).is_none())
            .map(|n| n.node_id())
            .collect::<Vec<_>>();

        let nodes_with_container_cnt = env.nodes().len() - nodes_no_container.len();
        if nodes_with_container_cnt < target_cnt {
            let to_scale_up_cnt = target_cnt - nodes_with_container_cnt;
            // 对不含容器的节点按照其所有任务数量进行降序排序
            nodes_no_container.sort_by(|&a, &b| {
                let acnt = mech_metric().node_task_new_cnt(a);
                let bcnt = mech_metric().node_task_new_cnt(b);
                acnt.partial_cmp(&bcnt).unwrap()
            });
            // 反转，即优先选择任务数量最少的节点进行预加载
            nodes_no_container.reverse();
            for _ in 0..to_scale_up_cnt {
                let node_2_load_contaienr = nodes_no_container.pop().unwrap();
                up_cmds.push(UpCmd {
                    nid: node_2_load_contaienr,
                    fnid,
                })
            }
        }

        up_cmds
    }
}
