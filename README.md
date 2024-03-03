# Lark doc (Project Design and RoadMap)
[https://fvd360f8oos.feishu.cn/docx/Q3c6dJG5Go3ov6xXofZcGp43nfb](https://fvd360f8oos.feishu.cn/docx/Q3c6dJG5Go3ov6xXofZcGp43nfb)

# Environment

## Rust
```
// Switch to tested default version

rustup default 1.67
```

## Pylibs (Just for RL Scaler)
pip install -r requirements.txt

## CUDA (Just for RL Scaler)
https://developer.nvidia.com/cuda-downloads?target_os=Windows&target_arch=x86_64&target_version=11&target_type=exe_local


# Start sim server
cd serverless_sim
```
cargo run
```

# Tests

1. Copy a test script from scripts_examples to root dir and run it.

2. Run collect_seed_metrics.py, check result in `serverless_sim/records/seed_xxx.json`.

3. Start and analyze on serverless_sim_ui.