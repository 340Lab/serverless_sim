import os

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)

# chdir to the directory of this script
os.chdir(CUR_FDIR)

import requests
import numpy as np
import json
from pprint import pprint
from gym.spaces import Box
from numpy import uint8
import serverless_sim
import random

OBSERVATION_N=80

SIM_URL="http://127.0.0.1:3000/"

ACTION_SPACE_LOW=0
ACTION_SPACE_HIGH=1

class Client:

    url=SIM_URL

    def request(self,api,data=None):
        # print("request: ",self.url+api,", data: ",data)
        if data is None:
            return requests.post(self.url+api)
        else:
            return requests.post(self.url+api,json=data)
    
    
Client().request("collect_seed_metrics")



# class EnvForAI:
#     env=ProxyEnv2({
#         "rand_seed":"hello",
#         "request_freq":"middle",
#         "dag_type":"single",
#         "cold_start":"high",
#         "fn_type":"cpu",
#         "es": {
#             "up":"fnsche",
#             "down":"fnsche",
#             "sche":"fnsche",
#         },    
#     })
#     def __init__(self):
#         self.observation_space=self.env.observation_space
#         self.action_space=self.env.action_space
#         self.spec=self.env.spec
        
#     def step(self,action):
#         return self.env.step(action)
#     def reset(self):
#         self.env.config["rand_seed"]=generate_random_str()
#         return self.env.reset()