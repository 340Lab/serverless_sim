use core::panic;
use std::{
    collections::{hash_map, HashMap},
    fs::File,
};

use clap::builder::Str;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mechanism_conf::MechConfig;

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
    pub mech: MechConfig,
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
        let scnum = self.mech.scale_num_conf();
        let scdown = self.mech.scale_down_exec_conf();
        let scup = self.mech.scale_up_exec_conf();
        let sche = self.mech.sche_conf();
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
