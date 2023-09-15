use serde::{ Deserialize, Serialize };

#[derive(Serialize, Deserialize, Clone)]
pub struct ESConfig {
    /// ai, lass, hpa
    pub up: String,
    /// no, ai, rule
    pub down: String,
    /// rule,ai,faasflow
    pub sche: String,
}

impl ESConfig {
    pub fn sche_ai(&self) -> bool {
        if &*self.sche == "ai" {
            return true;
        }
        false
    }
    pub fn sche_rule(&self) -> bool {
        if &*self.sche == "rule" {
            return true;
        }
        false
    }
    pub fn sche_faas_flow(&self) -> bool {
        if &*self.sche == "faasflow" {
            return true;
        }
        false
    }
    pub fn sche_fnsche(&self) -> bool {
        if &*self.sche == "fnsche" {
            return true;
        }
        false
    }
    pub fn scale_lass(&self) -> bool {
        if &*self.up == "lass" && &*self.down == "lass" {
            return true;
        }
        false
    }
    pub fn scale_ai(&self) -> bool {
        if &*self.up == "ai" && &*self.down == "ai" {
            return true;
        }
        false
    }
    pub fn scale_hpa(&self) -> bool {
        if &*self.up == "hpa" && &*self.down == "hpa" {
            return true;
        }
        false
    }
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
    pub es: ESConfig,
}

impl Config {
    pub fn check_valid(&self) {
        match &*self.request_freq {
            "low" | "middle" | "high" => {}
            _ => panic!("request_freq should be low, middle or high"),
        }
        match &*self.dag_type {
            "single" | "chain" | "dag" | "mix" => {}
            _ => panic!("dag_type should be single, chain, dag or mix"),
        }
        match &*self.cold_start {
            "high" | "low" | "mix" => {}
            _ => panic!("cold_start should be high, low or mix"),
        }
        match &*self.fn_type {
            "cpu" | "data" | "mix" => {}
            _ => panic!("fn_type should be cpu, data or mix"),
        }
        match &*self.es.up {
            "ai" | "lass" | "hpa" => {}
            _ => panic!("ef.up should be ai, lass or hpa"),
        }
        match &*self.es.down {
            "ai" | "rule" | "lass" => {}
            _ => panic!("ef.down should be ai, rule or lass"),
        }
        match &*self.es.sche {
            "rule" | "ai" | "faasflow" | "fnsche" => {}
            _ => panic!("ef.sche should be rule, ai, faasflow or fnsche"),
        }
    }
    pub fn str(&self) -> String {
        format!(
            "sd{}.rf{}.dt{}.cs{}.ft{}.up{}.dn{}.sc{}",
            self.rand_seed,
            self.request_freq,
            self.dag_type,
            self.cold_start,
            self.fn_type,
            self.es.up,
            self.es.down,
            self.es.sche
        )
    }
}
