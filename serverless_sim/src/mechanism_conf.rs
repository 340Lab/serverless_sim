use serde::{Deserialize, Serialize};

use crate::mechanism::{
    FILTER_NAMES, MECH_NAMES, SCALE_DOWN_EXEC_NAMES, SCALE_NUM_NAMES, SCALE_UP_EXEC_NAMES,
    SCHE_NAMES,
};
use std::{collections::HashMap, fs::File};

pub struct ModuleMechConf(pub MechConfig);

impl ModuleMechConf {
    pub fn new() -> Self {
        ModuleMechConf(MechConfig {
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
            mech_type: MECH_NAMES.iter().map(|v| (v.to_string(), None)).collect(),
            filter: FILTER_NAMES.iter().map(|v| (v.to_string(), None)).collect(),
        })
    }
    ///将结构体中的配置数据导出为一个JSON文件
    pub fn export_module_file(&self) {
        let file = File::create("module_conf_es.json").unwrap();
        serde_json::to_writer_pretty(file, &self.0).unwrap();
    }
    ///检查提供的MechConfig配置是否与模块的预期配置匹配
    pub fn check_conf_by_module(&self, conf: &MechConfig) -> bool {
        fn compare_sub_hashmap(
            module: &HashMap<String, Option<String>>,
            conf: &HashMap<String, Option<String>>,
            must_one_some: bool,
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
            if must_one_some && somecnt != 1 {
                log::warn!("Sub conf with multi some, cnt:{}", somecnt);
                return false;
            }
            true
        }
        if !compare_sub_hashmap(&self.0.scale_down_exec, &conf.scale_down_exec, true) {
            log::warn!("scale_down_exec is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.scale_num, &conf.scale_num, true) {
            log::warn!("scale_num is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.scale_up_exec, &conf.scale_up_exec, true) {
            log::warn!("scale_up_exec is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.sche, &conf.sche, true) {
            log::warn!("sche is not match");
            return false;
        }
        if !compare_sub_hashmap(&self.0.mech_type, &conf.mech_type, false) {
            log::warn!("mech_type is not match");
            return false;
        }
        true
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MechConfig {
    pub mech_type: HashMap<String, Option<String>>,
    pub scale_num: HashMap<String, Option<String>>,
    pub scale_down_exec: HashMap<String, Option<String>>,
    pub scale_up_exec: HashMap<String, Option<String>>,
    pub sche: HashMap<String, Option<String>>,
    pub filter: HashMap<String, Option<String>>,
}

impl MechConfig {
    pub fn mech_type(&self) -> (String, String) {
        self.mech_type
            .iter()
            .filter(|(_k, v)| v.is_some())
            .map(|(k, v)| (k.clone(), v.clone().unwrap()))
            .next()
            .unwrap()
    }
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
