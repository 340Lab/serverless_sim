import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

from records_read import FlattenConfig
import json


AVG_CNT=1


# #gen_tmp_yaml
# read ../serverless_sim/records
# collect each one by prefix
collect_by_config_str={}
for rec in os.listdir("../serverless_sim/records"):
    if rec.find(".UTC_")==-1:
        continue
    prefix=rec.split(".UTC_")[0]
    if prefix not in collect_by_config_str:
        collect_by_config_str[prefix]=1
    # collect_by_prefix[prefix].append(rec)

collect_by_prefix_arr=[]
for k,v in collect_by_config_str.items():
    collect_by_prefix_arr.append(k)

collect_by_prefix_arr.sort(key=lambda x: x)

targets_alias=""
for config_str in collect_by_prefix_arr:
    
    specs=FlattenConfig(config_str).json()
    specs_str=json.dumps(specs,indent=2)
    targets_alias+=f"- [{specs}, '{config_str}']\n"

TMP="""
avg_cnt: {{AVG_CNT}}

## filter with fixed value
filter: 
#   dag_type: single
  cold_start: high
#   fn_type: cpu
#   scale_down_exec: default.
#   # request_freq: low
  
## each group bars
targets_alias:
{{targets_alias}}

## group on x axis:
group: 
  by: cold_start
  types: [high]
  alias: ''
  type_alias: ['']

## y axis
values:
# - {alias: Throughput, trans: throughput}
- {alias: Cost, trans: cost_per_req}
- {alias: Latency(ms), trans: '[waitsche_time_per_req,coldstart_time_per_req,datarecv_time_per_req,exe_time_per_req]'} # convert 10ms to ms
- {alias: Quality-Price Ratio, trans: 'rps/cost_per_req/time_per_req if cost_per_req>0 and time_per_req>0  else 0'}
- {alias: Throuphput, trans: rps*1000}
- {alias: Avg Container Count, trans: fn_container_cnt}
"""

TMP=TMP.replace("{{targets_alias}}",targets_alias)
TMP=TMP.replace("{{AVG_CNT}}",str(AVG_CNT))

with open("fast_draw.yml","w") as f:
    f.write(TMP)


# os.system("python3 draw_bar.py fast_draw.yml")