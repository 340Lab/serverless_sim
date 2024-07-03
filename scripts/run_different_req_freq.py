import os

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)

# chdir to the directory of this script
os.chdir(CUR_FDIR)

req_freqs=["low"]
# req_freqs=["middle"]
# req_freqs=["high"]

import threading
from proxy_env3 import ProxyEnv3


class Task: 

    # 根据给定的算法配置来配置代理环境的参数
    def algo(self,algo_conf):
        self.env=ProxyEnv3()
        confs=[
            'mech_type',
            'scale_num',
            'scale_down_exec',
            'scale_up_exec',
            'sche',
            'instance_cache_policy'
        ]
        for i,conf in enumerate(confs[:5]):
            print("configuring ",conf," with ",algo_conf[i][0],"=",algo_conf[i][1])
            self.env.config["mech"][conf][algo_conf[i][0]]=algo_conf[i][1]
        for f in algo_conf[5]:
            print("configuring filter with ",list(f.keys())[0],"=",list(f.values())[0])
            filter_name=list(f.keys())[0]
            attr=f[filter_name]
            self.env.config["mech"]['filter'][filter_name]=attr
        for i,conf in enumerate(confs[5:]):
            print("configuring ",conf," with ",algo_conf[i+6][0],"=",algo_conf[i+6][1])
            self.env.config["mech"][conf][algo_conf[i+6][0]]=algo_conf[i+6][1]

        print("\n\n-------- testing: ",self.env.config["mech"])
        # self.env.config["mech"]['mech_type']
        # self.env.config["mech"]['scale_num'][algo_conf[0][0]]=algo_conf[0][1]
        # self.env.config["mech"]['scale_down_exec'][algo_conf[1][0]]=algo_conf[1][1]
        # self.env.config["mech"]['scale_up_exec'][algo_conf[2][0]]=algo_conf[2][1]
        # self.env.config["mech"]['sche'][algo_conf[3][0]]=algo_conf[3][1]
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
    # mechtype, scale_num, scale_down_exec, scale_up_exec, sche
    # [['scale_sche_separated',''],["hpa",""],["default",""],["least_task",""],["greedy",""],[{'careful_down':''}],["no_evict",""]],
    # [['scale_sche_separated',''],["hpa",""],["default",""],["least_task",""],["greedy",""],[{'careful_down':''}],["lru","10"]],
    
    # [['scale_sche_separated',''],["hpa",""],["default",""],["least_task",""],["random",""],[{'careful_down':''}],["no_evict",""]],
    # [['scale_sche_separated',''],["hpa",""],["default",""],["least_task",""],["random",""],[{'careful_down':''}],["lru","10"]],

    # [['scale_sche_separated',''],["full_placement",""],["default",""],["least_task",""],["random",""],[{'careful_down':''}],["no_evict","10"]],
    # [['scale_sche_separated',''],["full_placement",""],["default",""],["least_task",""],["random",""],[{'careful_down':''}],["lru","10"]],

    # [['scale_sche_joint',''],["temp_scaler",""],["default",""],["least_task",""],["pos",""],[{'careful_down':''}],["lru","10"]],
    [['scale_sche_joint',''],["temp_scaler",""],["default",""],["least_task",""],["pos",""],[{'careful_down':''}],["no_evict","10"]],
    # [['scale_sche_joint',''],["temp_scaler",""],["default",""],["least_task",""],["bp_balance",""],[{'careful_down':''}],["lru","10"]],
    # [['scale_sche_joint',''],["temp_scaler",""],["default",""],["least_task",""],["bp_balance",""],[{'careful_down':''}],["lru","10"]],

    # [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["greedy",""],[{'careful_down':''}],["lru","10"]],
    [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["greedy",""],[{'careful_down':''}],["no_evict","10"]],
    # [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["rotate",""],[{'careful_down':''}],["lru","10"]],
    [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["rotate",""],[{'careful_down':''}],["no_evict","10"]],
    # [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["hash",""],[{'careful_down':''}],["lru","10"]],
    [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["hash",""],[{'careful_down':''}],["no_evict","10"]],
    # [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["bp_balance",""],[{'careful_down':''}],["lru","10"]],
    # [['scale_sche_joint',''],["hpa",""],["default",""],["least_task",""],["bp_balance",""],[{'careful_down':''}],["lru","10"]],

    

    # [['scale_sche_joint',''],["full_placement",""],["default",""],["least_task",""],["pos",""],[{'careful_down':''}],["no_evict","10"]],
    # [['scale_sche_joint',''],["full_placement",""],["default",""],["least_task",""],["pos",""],[{'careful_down':''}],["lru","10"]],    
    
    # [['no_scale',''],['no',''],["default",""],['no',''],['faasflow','']],
    # [['no_scale',''],["no",""],["default",""],["no",""],["consistenthash",""]],
    # ["lass","lass","rule"],
    # ["fnsche","fnsche","fnsche"],
    # ["faasflow","faasflow","faasflow"],
]

# ts=[]

for req_freq in req_freqs:    
    for algo in algos:
        def cb(config):
            config["request_freq"]=req_freq
        # def task():
        Task() \
            .algo(algo) \
            .config(cb) \
            .run()
        # t = threading.Thread(target=task, args=())
        # t.start()
        # ts.append(t)

# for t in ts:
#     t.join()

    