import subprocess 
import threading
import time
from proxy_env import ProxyEnv
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

env=ProxyEnv({
    
})

for j in range(3):

    for i in range(2000):
        # if i%100==0:
        #     time.sleep(0.1)
        state,score,stop,info=env.step(1)
        print(state,score,stop,info)

    env.reset()
