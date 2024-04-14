import os

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)

# chdir to the directory of this script
os.chdir(CUR_FDIR)

import requests
import json
from pprint import pprint
import serverless_sim
import random

SIM_URL = "http://127.0.0.1:3000/"



class ProxyEnv3:

    url = SIM_URL

    config = {
        # /// "ai", "lass", "hpa", "es"
        "rand_seed": "",
        "request_freq": "low",
        "dag_type": "single",
        "cold_start": "high",
        "fn_type": "cpu",
        "no_log": False,
        # // optional
        "es": {}
    }

    env_id=""

    def __init__(self):
        # read ./serverless_sim/module_conf_es.json and set to config["es"]
        with open("./serverless_sim/module_conf_es.json", "r") as f:
            self.config["es"] = json.load(f)
        print(f"Config Templete {self.config}")
        print("\n\n")

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

    def reset(self):
        res=self.__request("reset", {
            "config":self.config
        })
        print(f"reset success: {res.json()}")
        self.env_id = res.json()['kernel']["env_id"]
        return res.json()['kernel']

    def step(self, action):
        if self.env_id=="":
            print("env_id is empty, please reset the environment first.")
            print("\n\n")
            return
        res = self.__request("step", {"action": action, "env_id": self.env_id})
        return res.json()['kernel']
