use crate::{
    // parse_arg,
    sim_env::SimEnv,
    sim_scale_from_zero::ScaleFromZeroType,
    sim_scaler::ScalerType,
};
use chrono;
use serde::{ Deserialize, Serialize };
use std::{ fs::{ self, File }, io::Write };

// #[derive(Serialize, Deserialize)]
// pub struct ReqFrame {
//     // reqid
//     r: ReqId,
//     // dagid
//     d: DagId,
//     // new in
//     n: bool,
// }

// #[derive(Serialize, Deserialize)]
// pub struct NodeFrame {
//     // node id
//     n: NodeId,
//     // cpu
//     c: f32,
//     // mem
//     m: f32,
// }

// #[derive(Serialize, Deserialize)]
// pub struct RecordOneFrame {
//     frame: usize,
//     running_reqs: Vec<Value>,
//     nodes: Vec<Value>,
//     req_done_time_avg: f32,
//     req_done_time_std: f32,
//     req_done_time_avg_90p: f32,
//     cost: f32,
//     score:f32,
// }

pub struct OneFrameRecord {
    pub frame: usize,
}

#[derive(Serialize, Deserialize)]
pub struct Records {
    record_name: String,
    // 0 frame,
    // 1 running_reqs,
    // 2 nodes,
    // 3 req_done_time_avg,
    // 4 req_done_time_std,
    // 5 req_done_time_avg_90p,
    // 6 cost
    pub frames: Vec<Vec<serde_json::Value>>,
}
impl Records {
    pub fn new(mut key: String) -> Self {
        // let args = parse_arg::get_arg();
        key = key.replace(":", "_");
        key = key.replace(",", ".");
        key = key.replace("\"", "");
        let record_name = format!(
            "{}.{}",
            key,
            // match args.scale_from_zero {
            //     ScaleFromZeroType::LazyScaleFromZero => "lazy_scale_from_zero",
            //     ScaleFromZeroType::DirectlyScaleFromZero => "directly_scale_from_zero",
            // },
            chrono::offset::Utc::now().format("UTC_%Y_%m_%d_%H_%M_%S")
        );
        Self {
            record_name,
            frames: Vec::new(),
        }
    }
    pub fn add_frame(&mut self, sim_env: &SimEnv) {
        let frame = vec![
            (*sim_env.current_frame.borrow()).into(),
            sim_env.requests
                .borrow()
                .iter()
                .map(|(reqid, req)| {
                    serde_json::json!({
                        "r": *reqid,
                        "d": req.dag_i,
                        "n": (req.begin_frame == sim_env.current_frame()),
                    })
                })
                .collect::<Vec<_>>()
                .into(),
            sim_env.nodes
                .borrow()
                .iter()
                .map(|node| {
                    serde_json::json!( {
                        "n": node.node_id(),
                        "c": node.cpu,
                        "m": node.mem,
                    })
                })
                .collect::<Vec<_>>()
                .into(),
            sim_env.req_done_time_avg().into(),
            sim_env.req_done_time_std().into(),
            sim_env.req_done_time_avg_90p().into(),
            sim_env.cost_each_req().into(),
            sim_env.score().into()
        ];
        self.frames.push(frame);
    }
    pub fn flush(&self) {
        if self.frames.len() > 9 {
            fs::create_dir_all("records").unwrap();

            log::info!("flush to target key: {}", self.record_name);
            let mut file = File::create(format!("records/{}.json", self.record_name)).unwrap();
            file.write_all(serde_json::to_string(self).unwrap().as_bytes()).unwrap();
        }
    }
}
