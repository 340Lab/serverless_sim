use crate::{
    sim_env::{SimEnv, SimEnvCoreState, SimEnvHelperState},
};

pub trait WithEnvCore {
    fn core(&self) -> &SimEnvCoreState;
}
impl WithEnvCore for SimEnv {
    fn core(&self) -> &SimEnvCoreState {
        &self.core
    }
}
pub trait WithEnvHelp {
    fn help(&self) -> &SimEnvHelperState;
}
