import os

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)

# chdir to the directory of this script
os.chdir(CUR_FDIR)

import requests
import json
from pprint import pprint
# import serverless_sim
import random

SIM_URL = "http://127.0.0.1:3000/"



class ProxyEnv3:

    url = SIM_URL

    # 传入sim系统的config配置
    config = {
        # /// "ai", "lass", "hpa", "es"
        "rand_seed": "",
        "request_freq": "low",
        "dag_type": "single",
        "cold_start": "high",
        "fn_type": "cpu",
        "no_log": False,
        "total_frame":1000,
        # // optional
        "mech": {}
    }

    env_id=""

    # 将加载的JSON数据赋给self.config["mech"]
    def __init__(self):
        # read ./serverless_sim/module_conf_es.json and set to config["es"]
        with open("../serverless_sim/module_conf_es.json", "r") as f:
            self.config["mech"] = json.load(f)
        print(f"Config Templete {self.config}")
        print("\n\n")

    # 向指定的API发出POST请求，并返回响应结果
    def __request(self, api, data=None):
        # print("request: ",self.url+api,", data: ",data)
        print("\n")
        print(f"[{api}] req: {data}")

        if data is None:
            res= requests.post(self.url+api)
        else:
            res= requests.post(self.url+api, json=data)

        print(f"[{api}] res: {res.status_code} {res.reason} {res.text}")
        # print(f"[{api}] res json: {res.json()}")
        print("\n")

        return res

    # 使用配置信息向API发送重置环境的请求，并返回从API接收到的内核信息
    def reset(self):
        res=self.__request("reset", {
            "config":self.config
        })
        print(f"reset success: {res.json()}")
        self.env_id = res.json()['kernel']["env_id"]
        return res.json()['kernel']

    # 用于向模拟环境的API发送执行步骤的请求，并返回API响应中的kernel部分
    def step(self, action):
        if self.env_id=="":
            print("env_id is empty, please reset the environment first.")
            print("\n\n")
            return
        
        # 向模拟环境的API发送一个step请求，其中包含action和env_id信息
        res = self.__request("step", {"action": action, "env_id": self.env_id})
        return res.json()['kernel']
