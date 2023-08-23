use std::sync::Mutex;

use crate::{sim_scale_from_zero::ScaleFromZeroType, sim_scaler::ScalerType};

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref PARSED_ARGS: Mutex<Args> = Mutex::new(Args{
        scaler: ScalerType::HpaScaler,
        scale_from_zero: ScaleFromZeroType::LazyScaleFromZero,
    });
}

use clap::{arg, command, Parser};

#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(value_enum)]
    pub scaler: ScalerType,

    #[arg(value_enum)]
    pub scale_from_zero: ScaleFromZeroType,
}

pub fn parse_arg() {
    *PARSED_ARGS.lock().unwrap() = Args::parse();
}

pub fn get_arg() -> Args {
    PARSED_ARGS.lock().unwrap().clone()
}
