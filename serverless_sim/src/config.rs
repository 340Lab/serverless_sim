use core::panic;
use std::{
    collections::{hash_map, HashMap},
    fs::File,
};

use clap::builder::Str;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    scale::{down_exec::SCALE_DOWN_EXEC_NAMES, num::SCALE_NUM_NAMES, up_exec::SCALE_UP_EXEC_NAMES},
    sche::SCHE_NAMES,
};

pub struct ModuleESConf(pub ESConfig);

impl ModuleESConf {
    pub fn new() -> Self {
        ModuleESConf(ESConfig {
            scale_num: {
                SCALE_NUM_NAMES
                    .iter()
                    .map(|v| (v.to_string(), None))
                    .collect()
            },
            scale_down_exec: SCALE_DOWN_EXEC_NAMES
                .iter()
                .map(|v| (v.to_string(), None))
                .collect(),
            scale_up_exec: SCALE_UP_EXEC_NAMES
                .iter()
                .map(|v| (v.to_string(), None))
                .collect(),
            sche: SCHE_NAMES.iter().map(|v| (v.to_string(), None)).collect(),
        })
    }
    pub fn export_module_file(&self) {
        let file = File::create("module_conf_es.json").unwrap();
        serde_json::to_writer_pretty(file, &self.0).unwrap();
    }
    pub fn check_conf_by_module(&self, conf: &ESConfig) -> bool {
        fn compare_sub_hashmap(
            module: &HashMap<String, Option<String>>,
            conf: &HashMap<String, Option<String>>,
        ) -> bool {
            // len must be same
            if module.len() != conf.len() {
                log::warn!(
                    "Sub conf len is not match module:{} conf:{}",
                    module.len(),
                    conf.len()
                );
                return false;
            }
            // only one can be some
            let somecnt = conf.iter().filter(|(k, v)| v.is_some()).count();
            if somecnt != 1 {
                log::warn!("Sub conf with multi some, cnt:{}", somecnt);
            }
            true
        }
        if !compare_sub_hashmap(&self.0.scale_down_exec, &conf.scale_down_exec) {
            log::warn!("scale_down_exec is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.scale_num, &conf.scale_num) {
            log::warn!("scale_num is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.scale_up_exec, &conf.scale_up_exec) {
            log::warn!("scale_up_exec is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.sche, &conf.sche) {
            log::warn!("sche is not match");
            return false;
        }
        true
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ESConfig {
    pub scale_num: HashMap<String, Option<String>>,
    pub scale_down_exec: HashMap<String, Option<String>>,
    pub scale_up_exec: HashMap<String, Option<String>>,
    pub sche: HashMap<String, Option<String>>,
}

impl ESConfig {
    // return (name,attr)
    pub fn scale_num_conf(&self) -> (String, String) {
        self.scale_num
            .iter()
            .filter(|(_k, v)| v.is_some())
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.clone()
                        .unwrap_or_else(|| panic!("scale_num_conf {:?}", self.scale_num)),
                )
            })
            .next()
            .unwrap()
    }
    pub fn scale_down_exec_conf(&self) -> (String, String) {
        self.scale_down_exec
            .iter()
            .filter(|(_k, v)| v.is_some())
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.clone().unwrap_or_else(|| {
                        panic!("scale_down_exec_conf {:?}", self.scale_down_exec)
                    }),
                )
            })
            .next()
            .unwrap()
    }
    pub fn scale_up_exec_conf(&self) -> (String, String) {
        self.scale_up_exec
            .iter()
            .filter(|(_k, v)| v.is_some())
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.clone()
                        .unwrap_or_else(|| panic!("scale_up_exec_conf {:?}", self.scale_up_exec)),
                )
            })
            .next()
            .unwrap()
    }
    pub fn sche_conf(&self) -> (String, String) {
        self.sche
            .iter()
            .filter(|(_k, v)| v.is_some())
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.clone()
                        .unwrap_or_else(|| panic!("sche_conf {:?}", self.sche)),
                )
            })
            .next()
            .unwrap()
    }
    // pub fn sche_ai(&self) -> bool {
    //     if &*self.sche == "ai" {
    //         return true;
    //     }
    //     false
    // }
    // pub fn sche_rule(&self) -> bool {
    //     if &*self.sche == "rule" {
    //         return true;
    //     }
    //     false
    // }
    // pub fn sche_faas_flow(&self) -> bool {
    //     if &*self.sche == "faasflow" {
    //         return true;
    //     }
    //     false
    // }
    // pub fn sche_fnsche(&self) -> bool {
    //     if &*self.sche == "fnsche" {
    //         return true;
    //     }
    //     false
    // }
    // pub fn sche_time(&self) -> bool {
    //     &*self.sche == "time"
    // }
    // pub fn sche_rule_prewarm_succ(&self) -> bool {
    //     if &*self.sche == "rule_prewarm_succ" {
    //         return true;
    //     }
    //     false
    // }

    // pub fn sche_round_robin(&self) -> bool {
    //     if &*self.sche == "round_robin" {
    //         return true;
    //     }
    //     false
    // }

    // pub fn sche_random(&self) -> bool {
    //     if &*self.sche == "random" {
    //         return true;
    //     }
    //     false
    // }

    // pub fn sche_load_least(&self) -> bool {
    //     if &*self.sche == "load_least" {
    //         return true;
    //     }
    //     false
    // }

    // pub fn sche_gofs(&self) -> bool {
    //     if &*self.sche == "gofs" {
    //         return true;
    //     }
    //     false
    // }

    // pub fn sche_pass(&self) -> bool {
    //     &*self.sche == "pass"
    // }

    // pub fn scale_up_no(&self) -> bool {
    //     &*self.up == "no"
    // }

    // pub fn scale_lass(&self) -> bool {
    //     if &*self.up == "lass" && &*self.down == "lass" {
    //         return true;
    //     }
    //     false
    // }
    // pub fn scale_ai(&self) -> bool {
    //     if &*self.up == "ai" && &*self.down == "ai" {
    //         return true;
    //     }
    //     false
    // }
    // pub fn scale_hpa(&self) -> bool {
    //     if &*self.up == "hpa" && &*self.down == "hpa" {
    //         return true;
    //     }
    //     false
    // }
}

// 存储应用配置信息
#[derive(Serialize, Deserialize, Clone)]
pub struct APPConfig {
    // 应用的数量
    pub app_cnt: usize,
    // 表示请求频率
    pub request_freq: String,
    /// dag type: single, chain, dag
    pub dag_type: String,
    /// cold start: high, low, mix
    /// 冷启动情况
    pub cold_start: String,
    /// cpu, memory,datasize
    // pub fn_cpu: String,
    // pub fn_mem: String,
    // pub fn_data: String,
    // 函数的CPU、内存和数据大小需求
    pub fn_cpu: f32,
    pub fn_mem: f32,
    pub fn_data: f32,
    /// is time sensitive app=1
    pub app_is_sens: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    /// for the different algos, should use the same seed
    pub rand_seed: String,
    /// low middle high
    pub request_freq: String,
    /// dag type: single, chain, dag, mix
    pub dag_type: String,
    /// cold start: high, low, mix
    pub cold_start: String,
    /// cpu, data, mix
    pub fn_type: String,
    /// each stage control algorithm settings
    // pub app_types: Vec<APPConfig>,
    pub es: ESConfig,
    /// whether to log the resultz
    pub no_log: bool,
}

impl Config {
    pub fn request_freq_low(&self) -> bool {
        if &*self.request_freq == "low" {
            return true;
        }
        false
    }
    pub fn request_freq_middle(&self) -> bool {
        if &*self.request_freq == "middle" {
            return true;
        }
        false
    }
    pub fn request_freq_high(&self) -> bool {
        if &*self.request_freq == "high" {
            return true;
        }
        false
    }

    pub fn dag_type_single(&self) -> bool {
        if &*self.dag_type == "single" {
            return true;
        }
        false
    }

    pub fn dag_type_dag(&self) -> bool {
        if &*self.dag_type == "dag" {
            return true;
        }
        false
    }

    pub fn dag_type_mix(&self) -> bool {
        if &*self.dag_type == "mix" {
            return true;
        }
        false
    }

    pub fn fntype_cpu(&self) -> bool {
        if &*self.fn_type == "cpu" {
            return true;
        }
        false
    }

    pub fn fntype_data(&self) -> bool {
        if &*self.fn_type == "data" {
            return true;
        }
        false
    }

    // pub fn check_valid(&self) {
    //     match &*self.request_freq {
    //         "low" | "middle" | "high" => {}
    //         _ => panic!("request_freq should be low, middle or high"),
    //     }
    //     match &*self.dag_type {
    //         "single" | "chain" | "dag" | "mix" => {}
    //         _ => panic!("dag_type should be single, chain, dag or mix"),
    //     }
    //     match &*self.cold_start {
    //         "high" | "low" | "mix" => {}
    //         _ => panic!("cold_start should be high, low or mix"),
    //     }
    //     match &*self.fn_type {
    //         "cpu" | "data" | "mix" => {}
    //         _ => panic!("fn_type should be cpu, data or mix"),
    //     }
    //     match &*self.es.up {
    //         // "ai","lass","fnsche","hpa","faasflow"
    //         "lass" | "ai" | "fnsche" | "hpa" | "faasflow" => {}
    //         _ => panic!("ef.up should be lass, ai, fnsche, hpa or faasflow"),
    //     }
    //     match &*self.es.down {
    //         // "ai","lass","fnsche","hpa","faasflow"
    //         "lass" | "ai" | "fnsche" | "hpa" | "faasflow" => {}
    //         _ => panic!("ef.down should be lass, ai, fnsche, hpa or faasflow"),
    //     }
    //     match &*self.es.sche {
    //         "rule" | "ai" | "faasflow" | "fnsche" | "rule_prewarm_succ" | "random"
    //         | "round_robin" | "load_least" | "gofs" | "pass" => {}
    //         _ => panic!("ef.sche should be rule, ai, faasflow or fnsche"),
    //     }
    //     match &*self.es.down_smooth {
    //         "direct" | "smooth_30" | "smooth_100" => {}
    //         _ => panic!("ef.down_smooth should be direct, smooth_30 or smooth_100"),
    //     }
    //     if self.es.sche_ai() {
    //         match &**self.es.ai_type.as_ref().unwrap() {
    //             "sac" | "ppo" | "mat" => {}
    //             _ => panic!("ef.ai_type should be sac, ppo or mat"),
    //         }
    //     }
    // }
    pub fn str(&self) -> String {
        let scnum = self.es.scale_num_conf();
        let scdown = self.es.scale_down_exec_conf();
        let scup = self.es.scale_up_exec_conf();
        let sche = self.es.sche_conf();
        format!(
            "sd{}.rf{}.dt{}.cs{}.ft{}.scl({},{})({},{})({},{}).scd({},{})",
            self.rand_seed,
            self.request_freq,
            self.dag_type,
            self.cold_start,
            self.fn_type,
            scnum.0,
            scnum.1,
            scdown.0,
            scdown.1,
            scup.0,
            scup.1,
            sche.0,
            sche.1
        )
    }
}
