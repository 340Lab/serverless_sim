import json
import os
import subprocess
import time
import matplotlib.pyplot as plt
import numpy as np

RUN_TIMES = 10
script_path = "./run_different_req_freq.py"

def run_script():
    for i in range(RUN_TIMES):
        # 使用subprocess.run来运行脚本，并等待其完成
        result = subprocess.run(['python3', script_path], check=True)
        # 检查运行结果，如果失败则抛出异常
        if result.returncode != 0:
            raise Exception(f"脚本运行失败，返回码: {result.returncode}")
        # 可以在这里添加等待时间，如果需要的话
        print(f"第{i+1}次运行完成")
        time.sleep(5)

if __name__ == '__main__':
    run_script()