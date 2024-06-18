use crate::{
    actions::{ESActionWrapper, RawAction},
    algos::ContainerMetric,
    config::Config,
    fn_dag::FnId,
    mechanism::Mechanism,
    node::NodeId,
    request::ReqId,
    scale::num::{hpa::HpaScaleNum, lass::LassScaleNum, no::NoScaleNum, ScaleNum},
    sim_env::SimEnv,
    sim_run::Scheduler,
};
use enum_as_inner::EnumAsInner;
use moka::sync::Cache;
use std::{
    cell::RefMut,
    collections::{BTreeMap, VecDeque},
};

impl SimEnv {
    /// raw_action[0] container count
    pub fn step_es(&mut self, raw_action: ESActionWrapper) -> (f32, String) {
        self.avoid_gc();

        // 只有确定了下一个action，才会有可以返回的state

        loop {
            // 进行帧开始时处理
            self.on_frame_begin();

            // 生成新的请求，并添加到环境对象的请求映射中
            self.req_sim_gen_requests();

            // 新请求生成之后将系统中请求和节点更新到最新状态
            self.help.mech_metric_mut().on_new_req_generated(self);

            // 获得 扩容、缩容、调度 的指令
            let (ups, downs, sches) = self.new_mech.step(self, raw_action.clone());

            // FIXME: Should transfer the cmds for a while.
            // FIXME: should remove conflict cmds
            // TODO: ScheCmd has memlimit
            for sche in sches.iter() {
                self.schedule_reqfn_on_node(&mut self.request_mut(sche.reqid), sche.fnid, sche.nid);
            }
            for down in downs.iter() {
                //更新cache
                self.node_mut(down.nid)
                    .try_unload_container(down.fnid, self);
            }
            for up in ups.iter() {
                self.node_mut(up.nid).try_load_container(up.fnid, self);
            }

            self.sim_run();

            self.on_frame_end();

            if self.current_frame() > 1000 {
                self.help.metric_record_mut().flush(self);
                self.reset();
                break;
            }
        }

        // state should has prompt info for next action
        (0.0, "no action".to_string())
    }
}
