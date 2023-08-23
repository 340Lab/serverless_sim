import requests
import numpy as np
from pprint import pprint

OBSERVATION_N=400000

SIM_URL="http://127.0.0.1:3000/"

ACTION_SPACE=5

class ProxyEnv:

    url=SIM_URL

    action_space=type('',(object,),{"n":ACTION_SPACE})()
    
    observation_space=np.arange(OBSERVATION_N)
    
    def __request(self,api,data=None):
        # print("request: ",self.url+api,", data: ",data)
        if data is None:
            return requests.post(self.url+api)
        else:
            return requests.post(self.url+api,json=data)
    
    def reset(self):
        self.__request("reset")

    def step(self,action:int):
        res=self.__request("step",{"action":action})
        res=res.json()
        # print("res: ",res)
        # print("res: ",res.status_code,res.text)
        # res=res.json()
        # return res["observation"],res["reward"],res["done"],res["info"]
        state_str=res["state"]
        state_arr=[]
        for c in state_str:
            state_arr.append(ord(c))
        if len(state_arr) < OBSERVATION_N:
            state_arr.extend([0]*(OBSERVATION_N-len(state_arr)))
        elif len(state_arr) > OBSERVATION_N:
            print("Warning: state length is greater than OBSERVATION_N, truncating, info may be lost",len(state_arr))
            state_arr=state_arr[:OBSERVATION_N]
        state_mat=np.mat(state_arr)

        return state_mat,res["score"],res["stop"],res["info"]

