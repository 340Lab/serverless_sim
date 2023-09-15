# Environment
## Pylibs
pip install -r requirements.txt
## Rust
rustup default 1.67
pip install maturin
python -m venv modelenv
source .env/bin/activate
maturin develop

## CUDA
https://developer.nvidia.com/cuda-downloads?target_os=Windows&target_arch=x86_64&target_version=11&target_type=exe_local

## Some Abbreviations

ES: each stage, the main stages are frame_begin, scale, schedule, sim_compute, analyze, frame_end.

# Start sim server
cd serverless_sim
```
cargo run ai-scaler lazy-scale-from-zero 2>&1 | tee log
cargo run hpa-scaler lazy-scale-from-zero 2>&1 | tee log
```

# Run HPA simulation
python3 -m run_hpa

# Run RL simulation
python3 -m run_ddqn