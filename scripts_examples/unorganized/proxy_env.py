import requests
import numpy as np
from pprint import pprint
from gym.spaces import Box
from numpy import uint8

OBSERVATION_N=400

SIM_URL="http://127.0.0.1:3000/"

ACTION_SPACE=12

class ProxyEnv:

    url=SIM_URL

    action_space=type('',(object,),{"n":ACTION_SPACE})()

    spec=type('',(object,),{"id":"proxy_env"})()
    
    observation_space=Box(0, 255, (1,OBSERVATION_N, OBSERVATION_N), uint8)

    obs=np.zeros((1,OBSERVATION_N,OBSERVATION_N))

    step_cnt=0
    
    def __request(self,api,data=None):
        # print("request: ",self.url+api,", data: ",data)
        if data is None:
            return requests.post(self.url+api)
        else:
            return requests.post(self.url+api,json=data)
    
    def reset(self):
        self.step_cnt=0
        self.__request("reset")

    def step(self,action:int):
        res=self.__request("step",{"action":action})
        res=res.json()
        # print("res: ",res)
        # print("res: ",res.status_code,res.text)
        # res=res.json()
        # return res["observation"],res["reward"],res["done"],res["info"]
        state_arr=json.loads(res["state"])
        print("state arr len",len(state_arr))
        # state_arr
        # for c in state_str:
        #     state_arr.append(ord(c))
        # if len(state_arr) < OBSERVATION_N*OBSERVATION_N:
        #     for i in range(OBSERVATION_N*OBSERVATION_N-len(state_arr)):
        #         state_arr.append(0)
        # elif len(state_arr) > OBSERVATION_N*OBSERVATION_N:
        #     print("Warning: state length is greater than OBSERVATION_N, truncating, info may be lost",len(state_arr))
        #     state_arr=state_arr[:OBSERVATION_N*OBSERVATION_N]
        state_mat=np.reshape(state_arr,self.obs.shape)
        self.step_cnt+=1
        if self.step_cnt==10000:
            res["stop"]=True
        return state_mat,res["score"],res["stop"],res["info"]

