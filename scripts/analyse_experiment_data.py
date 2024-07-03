# 自动运行同路径下的run_different_req_freq.py
import json
import os
import subprocess
import time
import matplotlib.pyplot as plt
import numpy as np

# json文件中各个字段的索引
FRAME_IDX_FRAME = 0;                           # 帧数
FRAME_IDX_RUNNING_REQS = 1;                    # 请求数量
FRAME_IDX_NODES = 2;                           # 节点的状态：cpu、mem
FRAME_IDX_REQ_DONE_TIME_AVG = 3;               # 请求的平均完成时间
FRAME_IDX_REQ_DONE_TIME_STD = 4;               # 请求的完成时间的标准差
FRAME_IDX_REQ_DONE_TIME_AVG_90P = 5;           # 请求的90%完成时间
FRAME_IDX_COST = 6;                            # 成本
FRAME_IDX_SCORE = 7;                           # 得分（强化学习用）
FRAME_IDX_DONE_REQ_COUNT = 8;                  # 已完成请求数量
FRAME_IDX_REQ_WAIT_SCHE_TIME = 9;              # 等待调度的时间
FRAME_IDX_REQ_WAIT_COLDSTART_TIME = 10;        # 冷启动的时间
FRAME_IDX_REQ_DATA_RECV_TIME = 11;             # 数据接收时间
FRAME_IDX_REQ_EXE_TIME = 12;                   # 请求的执行时间
FRAME_IDX_ALGO_EXE_TIME = 13;                  # 算法执行时间
FRAME_IDX_FNCONTAINER_COUNT = 14;              # 总的容器数量

""" 
目前比较的指标有：
FRAME_IDX_REQ_DONE_TIME_AVG = 3;               # 请求的平均完成时间
FRAME_IDX_COST = 6;                            # 成本
性价比: 1 / (FRAME_IDX_REQ_DONE_TIME_AVG * FRAME_IDX_COST)
FRAME_IDX_REQ_WAIT_COLDSTART_TIME = 10;        # 冷启动的时间
"""

# 记录文件的路径，相对路径报错
records_path = "D:\\Desktop\\Program\\serverless_sim\\serverless_sim\\records"
script_path = ".\\run_different_req_freq.py"       # py脚本的路径
output_path = "D:\\Desktop\\实验结果\\算法延迟\\实验结果-10帧生成"
RUN_TIMES = 10                                  # 运行次数
# 算法组合，key为算法名，value为数组，数组的元素为字典，key为参数名，value为参数值
algos_metrics = {}  # HashMap<algo, List<HashMap<param, value>>>

# 多次运行脚本以分析实验数据
def run_script():
    for _ in range(RUN_TIMES):
        # 使用subprocess.run来运行脚本，并等待其完成
        result = subprocess.run(['python', script_path], check=True)
        # 检查运行结果，如果失败则抛出异常
        if result.returncode != 0:
            raise Exception(f"脚本运行失败，返回码: {result.returncode}")
        # 可以在这里添加等待时间，如果需要的话
        time.sleep(1)


# 根据执行后的json文件分析运行了哪些算法组合
def  analyze_which_algo():
    json_files = [f for f in os.listdir(records_path) if f.endswith('.json')]
    algos = []
    for file in json_files:
        # 用 . 分割文件名，取出算法名
        compete_name = file.split('.')
        a = 1
        algo_name = compete_name[5][4:] + "." + compete_name[9][4:] + "." + compete_name[11][3:]
        if algo_name not in algos:
            algos.append(algo_name)
    for algo in algos:
        algos_metrics[algo] = []


# 分析同一个算法运行 RUN_TIMES 次的实验结果折线图
def analyze_same_algo_metrics_bytimes():
    json_files = [f for f in os.listdir(records_path) if f.endswith('.json')]
    for file in json_files:
        # 取出算法名
        compete_name = file.split('.')
        algo_name = compete_name[5][4:] + "." + compete_name[9][4:] + "." + compete_name[11][3:]

        with open(os.path.join(records_path, file), 'r') as f:
            # 读取json数据
            record = json.load(f)
            frames = record['frames']
            done_time = frames[len(frames) - 1][3]
            cost = frames[len(frames) - 1][6]
            efficency = 1 / (frames[len(frames) - 1][3] * frames[len(frames) - 1][6])
            cold_start_time = frames[len(frames) - 1][10]
            algos_metrics[algo_name].append({'req_done_time_avg': done_time, 'cost': cost, 'efficency': efficency, 'cold_start_time': cold_start_time})
                

# 分析不同算法运行 RUN_TIMES 次的平均指标柱状图
def analyze_diff_algo_avg_metrics():
    metrics = ['req_done_time_avg', 'cost', 'efficency', 'cold_start_time']
    colors = ['b', 'g', 'r', 'c', 'm', 'y']

    # 生成折线图
    for algo, data in algos_metrics.items():
        fig, axs = plt.subplots(2, 2, figsize=(15, 10))
        for idx, metric in enumerate(metrics):
            ax = axs[idx // 2, idx % 2]
            values = [d[metric] for d in data]
            for i, entry in enumerate(data):
                ax.plot(range(len(data)), values, color=colors[i % len(colors)])
            std_value = np.std(values)
            ax.text(0.95, 0.95, f'STD: {std_value:.5f}', transform=ax.transAxes, fontsize=12, verticalalignment='top', horizontalalignment='right')
            ax.set_title(f'{metric}')
            ax.set_xlabel('TIMES')
            ax.set_ylabel(metric)
            ax.legend()
        fig.suptitle(f'{algo} - Metrics')
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plt.savefig(os.path.join(output_path, f"{algo}.png"))

    # 生成直方图
    avg_metrics = {algo: {metric: np.mean([entry[metric] for entry in data]) for metric in metrics} for algo, data in algos_metrics.items()}

    fig, axs = plt.subplots(2, 2, figsize=(15, 10))
    for idx, metric in enumerate(metrics):
        ax = axs[idx // 2, idx % 2]
        algo_names = list(avg_metrics.keys())
        values = [avg_metrics[algo][metric] for algo in algo_names]
        bars = ax.bar(algo_names, values, color=colors[:len(algo_names)])
        ax.set_title(f'Average {metric} Comparison')
        ax.set_xlabel('Algorithm')
        ax.set_ylabel(metric)
        ax.set_xticklabels(algo_names, rotation=45, ha='right')
        # 在每个直方上方显示具体数值
        for bar, value in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{value:.2f}', ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, "avg_comparison.png"))


if __name__ == "__main__":
    run_script()

    analyze_which_algo()


    analyze_same_algo_metrics_bytimes()


    analyze_diff_algo_avg_metrics()
