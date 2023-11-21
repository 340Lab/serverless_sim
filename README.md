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

## Project sctructure
``` c
// Simulate main program
serverless_sim

// UI to analyze test result
serverless_sim_ui

// The proxy of sim env
proxy_env.py 

// call the backend to analyze the latest frame record into serverless_sim/records/seed_xxx.json
collect_seed_metrics.py 
```
## Start sim server
cd serverless_sim
```
cargo run
```

## Tests

1. Clone a test script from scripts_examples to root dir and run it.

2. Run collect_seed_metrics.py, check result in `serverless_sim/records/seed_xxx.json`.

3. Start and analyze on serverless_sim_ui.