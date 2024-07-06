import os
import re
import json
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

def conf_str(conf):
    rand_seed = conf['rand_seed']
    request_freq = conf['request_freq']
    dag_type = conf['dag_type']
    cold_start = conf['cold_start']
    fn_type= conf['fn_type']

    def mech_part_conf(mech):
        objmap=conf['mech'][mech]
        # print(mech,objmap)
        # filiter_one=[v for v in objmap.items() if type(v) is str]
        filter_one_kv=[(k,v) for k,v in objmap.items() if v is not None]
        # print(filter_one_kv)
        return filter_one_kv[0]
    def mech_part_conf_multi(mech):
        objmap=conf['mech'][mech]
        # filiter_one=[v for v in objmap.items() if type(v) is str]
        filter_one_kv=[(k,v) for k,v in objmap.items() if v is not None]
        return filter_one_kv

    mech_type=mech_part_conf('mech_type')
    scale_num=mech_part_conf('scale_num')
    scale_down_exec=mech_part_conf('scale_down_exec')
    scale_up_exec=mech_part_conf('scale_up_exec')
    sche=mech_part_conf('sche')

    # some_filter="!unready!"
    some_filter=mech_part_conf_multi('filter')
    some_filter.sort(key=lambda x: x[0])
    some_filter = "".join([f"({k}.{v})" for k, v in some_filter])

    instance_cache_policy=mech_part_conf('instance_cache_policy')

    return "sd{}.rf{}.dt{}.cs{}.ft{}.mt{}.scl({}.{})({}.{})({}.{})[{}].scd({}.{}).ic({}.{})".\
        format(rand_seed,request_freq,dag_type,cold_start,fn_type,mech_type[0],\
               scale_num[0],scale_num[1],\
               scale_down_exec[0],scale_down_exec[1],\
               scale_up_exec[0],scale_up_exec[1],\
               some_filter,
               sche[0],sche[1],\
               instance_cache_policy[0],instance_cache_policy[1])


def spec_conf_cnt(conf):
    # read dir ../serverless_sim/records
    confstr=conf_str(conf)
    files_begin_with_str=[]
    if not os.path.exists("../serverless_sim/records"):
        return 0
    for ith_file in os.listdir("../serverless_sim/records"):
        if ith_file.startswith(confstr):
            files_begin_with_str.append(ith_file)
    return len(files_begin_with_str)
# from proxy_env3 import ProxyEnv3

def group_by_conf_files():
    # read dir ../serverless_sim/records
    collect_by_config_str={}
    if not os.path.exists("../serverless_sim/records"):
        return {}
    for rec in os.listdir("../serverless_sim/records"):
        if rec.find(".UTC_")==-1:
            continue
        prefix=rec.split(".UTC_")[0]
        if prefix not in collect_by_config_str:
            collect_by_config_str[prefix]=[]
        collect_by_config_str[prefix].append(rec)

    return collect_by_config_str

# print(conf_str(ProxyEnv3().config))

def config_str_of_file(filename):
    return filename.split(".UTC_")[0]

class FlattenConfig:
    config_str=""

    rand_seed=""
    request_freq=""
    dag_type=""
    cold_start=""
    scale_num=""
    scale_down_exec=""
    scale_up_exec=""
    fn_type=""
    instance_cache_policy=""
    filter=""

    def __init__(self, configstr):
        self.configstr = configstr
        

        # compute sub values by config str
        self.parse_configstr()

    def parse_configstr(self):
        config_patterns = [
            (r'sd(\w+)\.rf', 'rand_seed'),
            (r'\.rf(\w+)\.', 'request_freq'),
            (r'\.dt(\w+)\.', 'dag_type'),
            (r'\.cs(\w+)\.', 'cold_start'),
            (r'\.ft(\w+)\.', 'fn_type'),
            (r'\.scl\(([^)]+)\)\(([^)]+)\)\(([^)]+)\)\[(.*?)\].', 'scale_num', 'scale_down_exec', 'scale_up_exec','filter'),
            (r'\.scd\(([^)]+)\)', 'sche'),
            (r'\.ic\(([^)]+)\)', 'instance_cache_policy')
        ]

        for pattern, *keys in config_patterns:
            match = re.search(pattern, self.configstr)
            if match:
                values = match.groups()
                for key, value in zip(keys, values):
                    setattr(self, key, value)
        # self.print_attributes()

    def json(self):
        return {
            # 'configstr': self.configstr,
            'rand_seed': self.rand_seed,
            'request_freq': self.request_freq,
            'dag_type': self.dag_type,
            'cold_start': self.cold_start,
            'fn_type': self.fn_type,
            'scale_num': self.scale_num,
            'scale_down_exec': self.scale_down_exec,
            'scale_up_exec': self.scale_up_exec,
            'sche': self.sche,
            'instance_cache_policy': self.instance_cache_policy,
            'filter': self.filter
        }
    # def print_attributes(self):
    #     attributes = [
    #         'configstr', 'cost_per_req', 'time_per_req', 'score', 'rps', 'filename',
    #         'rand_seed', 'request_freq', 'dag_type', 'cold_start', 'fn_type', 
    #         'scale_num', 'scale_down_exec', 'scale_up_exec', 'sche'
    #     ]
    #     for attr in attributes:
    #         print(f"{attr}={getattr(self, attr)}")


class PackedRecord:
    # configstr.clone().into(),
    # cost_per_req,
    # time_per_req,
    # score,
    # rps.into(),
    # f.time_str.clone().into()

    filename=""
    configstr=""

    cost_per_req=0.0
    time_per_req=0.0
    score=0.0
    rps=0.0
    coldstart_time_per_req=0.0
    waitsche_time_per_req=0.0
    datarecv_time_per_req=0.0
    exe_time_per_req=0.0
    fn_container_cnt=0.0
    
    rand_seed=""
    request_freq=""
    dag_type=""
    cold_start=""
    scale_num=""
    scale_down_exec=""
    scale_up_exec=""
    sche=""
    fn_type=""
    instance_cache_policy=""



class Frame:
    idxs={}
    def __init__(self, frame_line):
        self.frame_line = frame_line
        self.frame = json.loads(frame_line)
        # self.idxs = {}
        with open("../serverless_sim/src/metric.rs", 'r') as f:
            for line in f.readlines():
                if line.find("const FRAME_IDX_")==-1:
                    continue
                # print(line)
                idx_name=line.split()[1][:-1]
                idx_value=int(line.split()[4][:-1])
                self.idxs[idx_name]=idx_value
        # print("idxs",self.idxs)
    def frame_cnt(self):
        return self.frame[self.idxs['FRAME_IDX_FRAME']]
    def running_reqs(self):
        return self.frame[self.idxs['FRAME_IDX_RUNNING_REQS']]
    def nodes(self):
        return self.frame[self.idxs['FRAME_IDX_NODES']]
    def req_done_time_avg(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_DONE_TIME_AVG']]
    def req_done_time_std(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_DONE_TIME_STD']]
    def req_done_time_avg_90p(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_DONE_TIME_AVG_90P']]
    def cost(self):
        return self.frame[self.idxs['FRAME_IDX_COST']]
    def score(self):
        return self.frame[self.idxs['FRAME_IDX_SCORE']]
    def done_req_count(self):
        return self.frame[self.idxs['FRAME_IDX_DONE_REQ_COUNT']]
    def req_wait_sche_time(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_WAIT_SCHE_TIME']]
    def req_wait_coldstart_time(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_WAIT_COLDSTART_TIME']]
    def req_data_recv_time(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_DATA_RECV_TIME']]
    def req_exe_time(self):
        return self.frame[self.idxs['FRAME_IDX_REQ_EXE_TIME']]
    def algo_exe_time(self):
        return self.frame[self.idxs['FRAME_IDX_ALGO_EXE_TIME']]
    def fncontainer_count(self):
        return self.frame[self.idxs['FRAME_IDX_FNCONTAINER_COUNT']]
    

def load_record_from_file(filename):
    # seek to filesize - 1000 
    # read lines
    # parse last third line
    with open(f"../serverless_sim/records/{filename}", 'r') as f:
        f.seek(0, os.SEEK_END)
        
        if f.tell() < 10000:
            f.seek(0)
        else:
            f.seek(f.tell() - 10000)
        lines = f.readlines()
        # print("\nlines",lines[-3].strip())
        # print()
        if len(lines) < 3:
            print("!!! failed to read record from file", filename)
            exit(1)
        line=lines[-3].strip()
        frame=Frame(line)

        record = PackedRecord()
        record.filename = filename
        record.configstr = config_str_of_file(filename)

        # print("configstr",record.configstr)

        record.cost_per_req = frame.done_req_count()/frame.cost()
        record.time_per_req = frame.req_done_time_avg()
        record.score = frame.score()
        record.rps = frame.done_req_count()/frame.frame_cnt()
        record.coldstart_time_per_req = frame.req_wait_coldstart_time()
        record.waitsche_time_per_req = frame.req_wait_sche_time()
        record.datarecv_time_per_req = frame.req_data_recv_time()
        record.exe_time_per_req = frame.req_exe_time()
        record.fn_container_cnt = frame.fncontainer_count()

        config=FlattenConfig(record.configstr)
        record.rand_seed=config.rand_seed
        record.request_freq=config.request_freq
        record.dag_type=config.dag_type
        record.cold_start=config.cold_start
        record.scale_num=config.scale_num
        record.scale_down_exec=config.scale_down_exec
        record.scale_up_exec=config.scale_up_exec
        record.sche=config.sche
        record.fn_type=config.fn_type
        record.instance_cache_policy=config.instance_cache_policy
        record.filter=config.filter
        return record
    
        
def avg_records(records):
    # check confstr is same
    for i in range(1,len(records)):
        if records[i].configstr!=records[0].configstr:
            print("!!! failed to avg records, not same confstr")
            exit(1)
    
    cost_per_req=0.0
    time_per_req=0.0
    score=0.0
    rps=0.0
    coldstart_time_per_req=0.0
    waitsche_time_per_req=0.0
    datarecv_time_per_req=0.0
    exe_time_per_req=0.0
    fn_container_cnt=0.0
    for record in records:
        cost_per_req+=record.cost_per_req
        time_per_req+=record.time_per_req
        score+=record.score
        rps+=record.rps
        coldstart_time_per_req+=record.coldstart_time_per_req
        waitsche_time_per_req+=record.waitsche_time_per_req
        datarecv_time_per_req+=record.datarecv_time_per_req
        exe_time_per_req+=record.exe_time_per_req
        fn_container_cnt+=record.fn_container_cnt
    cost_per_req/=len(records)
    time_per_req/=len(records)
    score/=len(records)
    rps/=len(records)
    coldstart_time_per_req/=len(records)
    waitsche_time_per_req/=len(records)
    datarecv_time_per_req/=len(records)
    exe_time_per_req/=len(records)
    fn_container_cnt/=len(records)

    # copyback 2 first
    records[0].cost_per_req=cost_per_req
    records[0].time_per_req=time_per_req
    records[0].score=score
    records[0].rps=rps
    records[0].coldstart_time_per_req=coldstart_time_per_req
    records[0].waitsche_time_per_req=waitsche_time_per_req
    records[0].datarecv_time_per_req=datarecv_time_per_req
    records[0].exe_time_per_req=exe_time_per_req
    records[0].fn_container_cnt=fn_container_cnt
    return records[0]

        # lines = f.readlines()
        # if len(lines) < 3:
        #     return None
        # record = PackedRecord()
        # record.filename = filename
        # record.raw_record = lines
        # record.configstr = lines[-3].strip()
        # record.cost_per_req = float(lines[-2].split()[1])
        # record.time_per_req = float(lines[-2].split()[3])
        # record.score = float(lines[-2].split()[5])
        # record.rps = float(lines[-2].split()[7])
        # record.coldstart_time_per_req = float(lines[-2].split()[9])
        # record.waitsche_time_per_req = float(lines[-2].split()[11])
        # record.datarecv_time_per_req = float(lines[-2].split()[13])
        # record.exe_time_per_req = float(lines[-2].split()[15])
        # # compute sub values by config str
        # record.parse_configstr()
        # return record