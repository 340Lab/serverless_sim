import subprocess 
import threading
import time
from proxy_env2 import ProxyEnv2
# 执行命令并返回输出
def run_command(command):
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return output.decode("utf-8")
    except subprocess.CalledProcessError as e:
        return e.output.decode("utf-8")
 
# 创建子进程并执行命令
def create_subprocess(command):
    try:
        process = subprocess.Popen(command, 
            shell=True,
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            cwd="./serverless_sim",
            )
        output, error = process.communicate()
        output = output.decode("utf-8")
        error = error.decode("utf-8")
        return output, error
    except subprocess.CalledProcessError as e:
        return "", e.output.decode("utf-8")
        



# # 示例1：执行命令并返回输出
# command = "ls"
# output = run_command(command)
# print(output)
 
 
# # 示例2：创建子进程并执行命令
# command = "ls"
# output, error = create_subprocess(command)
# print(output)
# print(error)
# def start_hpa_simu():
#     create_subprocess("cargo run hpa-scaler lazy-scale-from-zero 2>&1 | tee log")

# thread1 = threading.Thread(target=start_hpa_simu)
# thread1.start()

# time.sleep(5)


env=ProxyEnv2(False,{
    "rand_seed":"hello",
    "request_freq":"middle",
    "dag_type":"single",
    "cold_start":"high",
    "fn_type":"cpu",
    "es": {
        "up":"faasflow",
        "down":"faasflow",
        "sche":"faasflow",
        "down_smooth":"direct",
    },    
})

env.reset()

for i in range(1):
    state,score,stop,info=env.step(1)
    print(state,score,stop,info)

#save record
env.reset()

