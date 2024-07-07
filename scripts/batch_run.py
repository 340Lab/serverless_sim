import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

import yaml,time
with open("batch_run.yml", 'r') as stream:
    batchconf = yaml.safe_load(stream)


run_time=batchconf['run_time']
# request_freq=batchconf['params']['request_freq']
# dag_type=batchconf['params']['dag_type']
mech_scale_sche=batchconf['mech_scale_sche']
mech_other=batchconf['mech_other']


import records_read
from proxy_env3 import ProxyEnv3
env=ProxyEnv3()

def apply_mech(env,mechkey,mechconf):
    # if mechconf is arr
    if isinstance(mechconf, list):
        for mechconf_one in mechconf:
            apply_mech(env,mechkey,mechconf_one)
        return
    
    mechconfkey=mechconf.keys().__iter__().__next__()
    mechconfval=mechconf[mechconfkey]
    if mechconfval is None:
        mechconfval=''
    mechconfval=str(mechconfval)
    # print("apply_mech",mechconfkey,mechconfval)

    # assert(mechconfval!=None)
    env.config['mech'][mechkey][mechconfkey]=mechconfval
    
def unapply_mech(env,mechkey):
    # del env.config['mech'][mechkey]
    for mechconfkey in env.config['mech'][mechkey]:
        env.config['mech'][mechkey][mechconfkey]=None

compose_cnt=0

params=['request_freq','dag_type']
def dfs_params(paramsconf,i,cb):
    if i==len(params):
        cb()
        return
    for paramsconf_one in paramsconf[params[i]]:
        env.config[params[i]]=paramsconf_one.keys().__iter__().__next__()
        dfs_params(paramsconf,i+1,cb)

mech_other_params=['instance_cache_policy']
def dfs_mech_other(mech_otherconf,i,cb):
    if i==len(mech_other):
        cb()
        return
    for mech_otherconf_one in mech_otherconf[mech_other_params[i]]:
        apply_mech(env,mech_other_params[i],mech_otherconf_one)
        dfs_mech_other(mech_otherconf,i+1,cb)
        unapply_mech(env,mech_other_params[i])

def params_compistion():
    for mech_scale_sche_one in mech_scale_sche:
        print('mech type',mech_scale_sche_one)
        mech_scale_sche_conf=mech_scale_sche[mech_scale_sche_one]

        unapply_mech(env,'mech_type')
        apply_mech(env,'mech_type',{mech_scale_sche_one:''})
        
        mech_scale_sche_args=['scale_num','scale_down_exec','scale_up_exec','sche','filter']
        def dfs(mech_scale_sche_conf,i,cb):
            if i==len(mech_scale_sche_args):
                cb()
                return
            for mech_scale_sche_conf_one in mech_scale_sche_conf[mech_scale_sche_args[i]]:
                apply_mech(env,mech_scale_sche_args[i],mech_scale_sche_conf_one)
                # print("mech conf",env.config['mech'][mech_scale_sche_args[i]])
                dfs(mech_scale_sche_conf,i+1,cb)
                unapply_mech(env,mech_scale_sche_args[i])
       
        

        def compose_mech_other():
            def one_composition():
                global compose_cnt
                compose_cnt+=1
                print("composition_cnt:",compose_cnt)
                print(records_read.conf_str(env.config))
                cnt=records_read.spec_conf_cnt(env.config)
                needrun=0
                if run_time>cnt:
                    needrun=run_time-cnt
                print(f"need to run {needrun}")
                print("")
                print("-"*40)

                for i in range(needrun):
                    env.reset()
                    env.step(1)
                
                # mkdir ../serverless_sim/records
#                 os.system(f"mkdir -p ../serverless_sim/records")
#                 for i in range(needrun):
#                     with open(f"../serverless_sim/records/{records_read.conf_str(env.config)}.UTC_{time.time()}",'w') as f:
#                         f.write("""[1000,[{"d":26,"n":false,"r":24742},{"d":47,"n":false,"r":24751},{"d":88,"n":false,"r":24761},{"d":88,"n":false,"r":24787},{"d":88,"n":false,"r":24788},{"d":88,"n":false,"r":24846},{"d":88,"n":false,"r":24847},{"d":88,"n":false,"r":24868},{"d":88,"n":false,"r":24869},{"d":26,"n":false,"r":24871},{"d":26,"n":false,"r":24872},{"d":26,"n":false,"r":24873},{"d":36,"n":false,"r":24874},{"d":36,"n":false,"r":24875},{"d":36,"n":false,"r":24876},{"d":36,"n":false,"r":24877},{"d":36,"n":false,"r":24878},{"d":36,"n":false,"r":24879},{"d":36,"n":false,"r":24880},{"d":36,"n":false,"r":24881},{"d":36,"n":false,"r":24882},{"d":77,"n":false,"r":24883},{"d":78,"n":false,"r":24884},{"d":78,"n":false,"r":24885},{"d":78,"n":false,"r":24886},{"d":78,"n":false,"r":24887},{"d":79,"n":false,"r":24888},{"d":88,"n":false,"r":24889},{"d":88,"n":false,"r":24890},{"d":88,"n":false,"r":24891},{"d":88,"n":false,"r":24892},{"d":88,"n":false,"r":24893},{"d":88,"n":false,"r":24894},{"d":88,"n":false,"r":24895},{"d":26,"n":false,"r":24896},{"d":26,"n":false,"r":24897},{"d":26,"n":false,"r":24898},{"d":36,"n":false,"r":24899},{"d":36,"n":false,"r":24900},{"d":47,"n":false,"r":24901},{"d":47,"n":false,"r":24902},{"d":47,"n":false,"r":24903},{"d":47,"n":false,"r":24904},{"d":47,"n":false,"r":24905},{"d":47,"n":false,"r":24906},{"d":47,"n":false,"r":24907},{"d":58,"n":false,"r":24908},{"d":78,"n":false,"r":24909},{"d":88,"n":false,"r":24910},{"d":91,"n":false,"r":24911},{"d":26,"n":true,"r":24912},{"d":26,"n":true,"r":24913},{"d":36,"n":true,"r":24914},{"d":36,"n":true,"r":24915},{"d":36,"n":true,"r":24916},{"d":47,"n":true,"r":24917},{"d":47,"n":true,"r":24918},{"d":47,"n":true,"r":24919},{"d":58,"n":true,"r":24920},{"d":65,"n":true,"r":24921},{"d":77,"n":true,"r":24922},{"d":78,"n":true,"r":24923},{"d":78,"n":true,"r":24924},{"d":79,"n":true,"r":24925},{"d":88,"n":true,"r":24926},{"d":88,"n":true,"r":24927},{"d":88,"n":true,"r":24928},{"d":88,"n":true,"r":24929},{"d":88,"n":true,"r":24930},{"d":91,"n":true,"r":24931},{"d":91,"n":true,"r":24932}],[{"c":61.39801025390625,"m":2756.555419921875,"n":0},{"c":104.56680297851562,"m":3329.638671875,"n":1},{"c":128.83419799804688,"m":1493.141845703125,"n":2},{"c":140.46856689453125,"m":1350.91259765625,"n":3},{"c":180.70849609375,"m":4410.79833984375,"n":4},{"c":180.70849609375,"m":4211.79833984375,"n":5},{"c":154.18869018554688,"m":4231.798828125,"n":6},{"c":78.42510223388672,"m":3442.47900390625,"n":7},{"c":196.14169311523438,"m":2353.732177734375,"n":8},{"c":124.12464904785156,"m":3355.92578125,"n":9},{"c":90.13284301757812,"m":2944.52197265625,"n":10},{"c":196.14169311523438,"m":1765.66064453125,"n":11},{"c":115.55776977539062,"m":3366.330810546875,"n":12},{"c":52.28340148925781,"m":2958.3193359375,"n":13},{"c":127.15361022949219,"m":3305.81591796875,"n":14},{"c":78.42510223388672,"m":3044.47900390625,"n":15},{"c":180.70849609375,"m":4211.79833984375,"n":16},{"c":115.8602066040039,"m":3914.727294921875,"n":17},{"c":104.56680297851562,"m":3727.638671875,"n":18},{"c":180.70849609375,"m":3813.79833984375,"n":19},{"c":127.66886138916016,"m":4450.79931640625,"n":20},{"c":52.28340148925781,"m":2560.3193359375,"n":21},{"c":150.10836791992188,"m":3555.28955078125,"n":22},{"c":127.15361022949219,"m":3703.81591796875,"n":23},{"c":180.70849609375,"m":3813.79833984375,"n":24},{"c":81.59451293945312,"m":3420.75390625,"n":25},{"c":143.72848510742188,"m":2431.35693359375,"n":26},{"c":142.07553100585938,"m":2764.562744140625,"n":27},{"c":70.10836791992188,"m":3366.81201171875,"n":28},{"c":124.17063903808594,"m":2604.06982421875,"n":29}],15.422371864318848,23.56015396118164,0.0,0.027521366253495216,-15.422371864318848,112,5.782318592071533,6.258909225463867,0.0,3.381143808364868,5.276595744680851,145]

# ]}""")

                print("")
                print("-"*40)
                print("")
                time.sleep(1)
                # for t in range(run_time):
                    # print("run")
                    # env.run()
            dfs_mech_other(mech_other,0,one_composition)

        dfs(mech_scale_sche_conf,0,compose_mech_other)

dfs_params(batchconf['params'],0,params_compistion)