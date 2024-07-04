use crate::mechanism::SimEnvObserve;

use crate::with_env_sub::{WithEnvCore};
use crate::{actions::ESActionWrapper, fn_dag::FnId};

use super::{
    ScaleNum,
};

pub struct FpScaleNum {}

impl FpScaleNum {
    pub fn new() -> Self {
        Self {
            
        }
    }
}

impl ScaleNum for FpScaleNum {
    fn scale_for_fn(&mut self, env: &SimEnvObserve, _fnid: FnId, _action: &ESActionWrapper) -> usize {
        env.core().nodes().len()
    }
}
