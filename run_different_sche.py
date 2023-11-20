dag_type_request_freq_s=[#["cpu","low"],["data","low"],
["cpu","dag"]]#,["data","dag"],["cpu","single"]]

import threading
from proxy_env2 import ProxyEnv2

class Task: 
    def algo(self,up:str,down:str,sche:str):
        self.env=ProxyEnv2(False,{
            "rand_seed":"hello",
            "request_freq":"middle",
            "dag_type":"dag",
            "cold_start":"high",
            "fn_type":"cpu",
            "no_log":False,
            "es": {
                "up":up,
                "down":down,
                "sche":sche,
                "down_smooth":"direct",
            },    
        })
        return self
        
    def config(self,config_cb):
        config_cb(self.env.config)
        return self
        
    def run(self):
        self.env.reset()
        
        state,score,stop,info=self.env.step(1)
        print(state,score,stop,info)
        self.env.reset()
        return self

algos=[
    ["hpa","hpa","rule"],
    # ["hpa","hpa","rule_prewarm_succ"],
    # ["hpa","hpa","round_robin"],
    # ["hpa","hpa","random"],
    # ["hpa","hpa","load_least"],
    # ["hpa","hpa","gofs"],
    
    # ["fnsche","fnsche","fnsche"],
    # ["faasflow","faasflow","faasflow"],
]

ts=[]

for dag_type_request_freq in dag_type_request_freq_s:    
    for algo in algos:
        def cb(config):
            config["fn_type"]=dag_type_request_freq[0]
            config["dag_type"]=dag_type_request_freq[1]
        def task():
            Task() \
                .algo(algo[0],algo[1],algo[2]) \
                .config(cb) \
                .run()
        t = threading.Thread(target=task, args=())
        t.start()
        ts.append(t)

for t in ts:
    t.join()

    