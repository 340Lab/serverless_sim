import requests
import numpy as np
import json
from pprint import pprint
from gym.spaces import Box,Discrete
from numpy import uint8
import serverless_sim


OBSERVATION_N=80
ACTION_N=12

SIM_URL="http://127.0.0.1:3000/"


class ProxyEnv2:

    url=SIM_URL

    action_space=Discrete(ACTION_N)

    spec=type('',(object,),{"id":"proxy_env"})()
    
    observation_space=Box(-1, np.Inf, (OBSERVATION_N,), np.float32)

    obs=np.zeros((OBSERVATION_N,))

    step_cnt=0
    
    # according to network config
    config={
        # /// "ai", "lass", "hpa", "es"
        "plan": "",
        # // optional
        "es": {
            # // ai, lass, hpa
            "up": "ai",
            # // no, ai, rule
            "down": "ai",
            # // rule,ai,faasflow
            "sche": "ai",
        },
    }
    def typekey(self):
        if "es" in self.config:
            return self.config["plan"]+"_"+self.config["es"]["up"]+"_"+self.config["es"]["down"]+"_"+self.config["es"]["sche"]
        return self.config["plan"]

    def __init__(self,config):
        allowed_plans=["hpa","es"]
        allowed_up=["ai","lass","fnsche","hpa","faasflow"]
        allowed_down=["ai","lass","fnsche","hpa","faasflow"]
        allowed_sche=["rule","fnsche","faasflow"]
        up_down_must_same=["ai","lass","hpa","faasflow","fnsche"]
        scale_sche_must_same=["fnsche","faasflow"]

        assert config["plan"] in allowed_plans
        if "es" in config:
            assert config["es"]["up"] in allowed_up
            assert config["es"]["down"] in allowed_down
            assert config["es"]["sche"] in allowed_sche
            if config["es"]["up"] in up_down_must_same:
                assert config["es"]["up"]==config["es"]["down"]
            if config["es"]["sche"] in scale_sche_must_same:
                assert config["es"]["sche"]==config["es"]["up"]
        self.config=config

    def __request(self,api,data=None):
        # print("request: ",self.url+api,", data: ",data)
        if data is None:
            return requests.post(self.url+api)
        else:
            return requests.post(self.url+api,json=data)
    
    def reset(self):
        self.step_cnt=0
        self.__request("reset",self.config)
        return self.obs
        # serverless_sim.fn_reset(json.dumps(self.config))

    def step(self,action:int):
        
        # res=serverless_sim.fn_step(json.dumps({"action":action,"config":self.config}))
        res=self.__request("step",{"action":action,"config":self.config})
        print("res",res)
        res=res.json()

        # res=json.loads(res)

        # print("res: ",res)
        # print("res: ",res.status_code,res.text)
        # res=res.json()
        # return res["observation"],res["reward"],res["done"],res["info"]
        state_arr=json.loads(res["state"])
        print("state arr len",len(state_arr),state_arr,"current step",self.step_cnt)
        # state_arr
        # for c in state_str:
        #     state_arr.append(ord(c))
        if len(state_arr) < OBSERVATION_N:
            for i in range(OBSERVATION_N-len(state_arr)):
                state_arr.append(0)
        # elif len(state_arr) > OBSERVATION_N*OBSERVATION_N:
        #     print("Warning: state length is greater than OBSERVATION_N, truncating, info may be lost",len(state_arr))
        #     state_arr=state_arr[:OBSERVATION_N*OBSERVATION_N]
        state_arr=np.reshape(state_arr,self.obs.shape)
        self.step_cnt+=1
        # if self.step_cnt==10000:
        #     res["stop"]=True
        return state_arr,res["score"],res["stop"],res["info"]

