import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

import requests
from pprint import pprint
import yaml
import re
import matplotlib.pyplot as plt
import numpy as np

### doc: https://fvd360f8oos.feishu.cn/docx/RMjfdhRutoDmOkx4f4Lcl1sjnzd

# class PackedRecord:
#     # configstr.clone().into(),
#     # cost_per_req,
#     # time_per_req,
#     # score,
#     # rps.into(),
#     # f.time_str.clone().into()
#     raw_record=[]

#     configstr=""
#     cost_per_req=0.0
#     time_per_req=0.0
#     score=0.0
#     rps=0.0
#     coldstart_time_per_req=0.0
#     waitsche_time_per_req=0.0
#     datarecv_time_per_req=0.0
#     exe_time_per_req=0.0
    
#     filename=""

#     rand_seed=""
#     request_freq=""
#     dag_type=""
#     cold_start=""
#     scale_num=""
#     scale_down_exec=""
#     scale_up_exec=""
#     fn_type=""
#     instance_cache_policy=""
    

#     def __init__(self, raw_record):
#         if len(raw_record) != 10:
#             raise ValueError("The input list must contain exactly 10 elements.")
#         self.configstr = raw_record[0]
#         self.cost_per_req = raw_record[1]
#         self.time_per_req = raw_record[2]
#         self.score = raw_record[3]
#         self.rps = raw_record[4]
#         self.coldstart_time_per_req=raw_record[5]
#         self.waitsche_time_per_req=raw_record[6]
#         self.datarecv_time_per_req=raw_record[7]
#         self.exe_time_per_req=raw_record[8]
#         self.filename = raw_record[9]
        

#         # compute sub values by config str
#         self.parse_configstr()

#     def parse_configstr(self):
#         config_patterns = [
#             (r'sd(\w+)\.rf', 'rand_seed'),
#             (r'\.rf(\w+)\.', 'request_freq'),
#             (r'\.dt(\w+)\.', 'dag_type'),
#             (r'\.cs(\w+)\.', 'cold_start'),
#             (r'\.ft(\w+)\.', 'fn_type'),
#             (r'\.scl\(([^)]+)\)\(([^)]+)\)\(([^)]+)\)\.', 'scale_num', 'scale_down_exec', 'scale_up_exec'),
#             (r'\.scd\(([^)]+)\)', 'sche'),
#             (r'\.ic\(([^)]+)\)', 'instance_cache_policy')
#         ]

#         for pattern, *keys in config_patterns:
#             match = re.search(pattern, self.configstr)
#             if match:
#                 values = match.groups()
#                 for key, value in zip(keys, values):
#                     setattr(self, key, value)
#         self.print_attributes()

        
#     def print_attributes(self):
#         attributes = [
#             'configstr', 'cost_per_req', 'time_per_req', 'score', 'rps', 'filename',
#             'rand_seed', 'request_freq', 'dag_type', 'cold_start', 'fn_type', 
#             'scale_num', 'scale_down_exec', 'scale_up_exec', 'sche'
#         ]
#         for attr in attributes:
#             print(f"{attr}={getattr(self, attr)}")

import records_read
# {
#     confstr: [files...]
# }
def get_record_filelist(drawconf):
    conf_2_files=records_read.group_by_conf_files()
    # filter out we dont care
    new={}
    for confstr in conf_2_files:
        conf=records_read.FlattenConfig(confstr)
        confjson=conf.json()
        
        # check match draw filter
        for drawfilter in drawconf['filter']:
            if drawfilter in confjson:
                if confjson[drawfilter]!=drawconf['filter'][drawfilter]:
                    continue

        # check match draw targets_alias
        for target in drawconf['targets_alias']:
            for targetkey in target[0]:
                if targetkey not in confjson:
                    print("!!! invalid target alias with key",targetkey)
                    exit(1)
                if confjson[targetkey]!=target[0][targetkey]:
                    continue
        new[confstr]=conf_2_files[confstr]
    return new

# no return
# panic if check failed
def check_first_draw_group_match_avg_cnt(drawconf,conf_2_files):
    avg_cnt=drawconf['avg_cnt']
    if avg_cnt==0:
        print("!!! avg_cnt should not be 0")
        exit(1)
    
    first_group_k=drawconf['group']['by']
    first_group_v=drawconf['group']['types'][0]
    conf_2_files_only_first_group={}
    # filter 
    for confstr in conf_2_files:
        conf=records_read.FlattenConfig(confstr)
        if getattr(conf,first_group_k)==first_group_v:
            conf_2_files_only_first_group[confstr]=conf_2_files[confstr]

    # all group files cnt >= avg_cnt
    for confstr in conf_2_files_only_first_group:
        if len(conf_2_files_only_first_group[confstr])<avg_cnt:
            print("!!!",confstr,"files cnt < avg_cnt")
            exit(1)

# {
#     confstr: PackedRecord
# }
def get_each_group_prev_avg_cnt_file__compute_avg(drawconf,conf_2_files):
    avg_cnt=drawconf['avg_cnt']
    # sort
    for confstr in conf_2_files:
        conf_2_files[confstr].sort()
    # left avg_cnt files
    for confstr in conf_2_files:
        conf_2_files[confstr]=conf_2_files[confstr][:avg_cnt]
    # transform files 2 records
    conf_2_records={}
    for confstr in conf_2_files:
        file_records=[]
        for file in conf_2_files[confstr]:
            file_records.append(records_read.load_record_from_file(file))
        conf_2_records[confstr]=file_records
    # compute avg and transform records 2 one record
    conf_2_avg_record={}
    for confstr in conf_2_files:
        records=conf_2_records[confstr]
        avg_record=records_read.avg_records(records)
        conf_2_avg_record[confstr]=avg_record
    return conf_2_avg_record

# [
#     {
#         group: xxx
#         values:[record]
#     }
# ]
def group_records(records,conf):
    group_by=conf['group']['by']
    group_types=conf['group']['types']
    groups=[{'group':group_type,'records':[]} for group_type in group_types]
    for record in records:
        attribute_value = getattr(record, group_by)
        groups[group_types.index(attribute_value)]['records'].append(record)
        
    # print("groups",groups)

    return groups

# [
#     {
#         value_y: xxx
#         groups:[
#             {
#                 group: xxx
#                 values: [
#                     [alias, value]
#                 ]
#             }
#         ]
#     }
# ]
def to_draw_meta(groups,conf):
    def groups_value(groups,valueconf):
        def spec_values(records):
            def spec_value(record):
                cost_per_req=record.cost_per_req
                time_per_req=record.time_per_req
                waitsche_time_per_req =record.waitsche_time_per_req 
                coldstart_time_per_req=record.coldstart_time_per_req
                datarecv_time_per_req =record.datarecv_time_per_req 
                exe_time_per_req=record.exe_time_per_req
                rps=record.rps
                fn_container_cnt=record.fn_container_cnt
                
                # score=0.0
                # rps=0.0
                # record.
                transs=valueconf['trans']
                if isinstance(transs, list):
                    
                    return [eval(trans) for trans in transs]
                else:
                    return eval(transs)
            def alias(record):
                def match_args(args):
                    for argkey in args:
                        if getattr(record, argkey)!=args[argkey]:
                            # print(argkey,getattr(record, argkey),args[argkey])
                            # record.print_attributes()
                            return False
                    return True
                for target_alias in conf['targets_alias']:
                    if match_args(target_alias[0]):
                        return  target_alias[1]
                print("err!!!!")
                exit(1)
            return [[alias(record),spec_value(record)] for record in records]

        return [{
            'group': group['group'],
            'values': spec_values(group['records'])
        } for group in groups]
    
    values=conf['values']
    res=[
        {
            'value_y': valueconf['alias'],
            'groups':groups_value(groups,valueconf)
        } for valueconf in values
    ]
    return res

def draw_with_draw_meta(drawmeta,conf):
    # ax2 = plt.subplot(2, 3, 1)
    # ax1 = plt.subplot(2, 3, 2)
    # ax = plt.subplot(2,3, 3)
    plotcnt=len(conf['values'])
    fig, plots = plt.subplots(1, plotcnt, figsize=(18, 6))
    # plots=[ax,ax1,ax2]

    plt.subplots_adjust(left=0.1, right=0.9, top=0.85, bottom=0.1)
    fig.set_size_inches(16, 4.5)
    bar_width = 0.1
    index = np.arange(len(conf['group']['types']))
    opacity = 0.4
    error_config = {'ecolor': '0.3'}
    patterns = ('x', '\\', '*', 'o', '.','O')
    colors=["#FC6B05","#FFB62B","#65B017","#99D8DB","#9BB7BB","#32CD32","#228B22","#8A2BE2"]
    
    plotidx=0
    for plot in plots:
        meta=drawmeta[plotidx]
        groups=meta['groups']
        plot.set_xticks(index)
        plot.set_xticklabels(conf['group']['type_alias'])

        plot.set_xlabel(conf['group']['alias'])
        plot.text(-1.26*(len(plots)-plotidx)+1.76, 1.05, meta['value_y'], ha='center', va='center', rotation=0, transform=plt.gca().transAxes)
        # plot.set_ylabel(meta['value_y'],labelpad=10, rotation=0, verticalalignment='top')

        model_value={
            'v':None
        }
        def set_model_value(v):
            print("set_model_value",v)
            if isinstance(v, list):
                model_value['v']=[0 for _ in range(len(v))]
            else:
                model_value['v']=0
        value_idx=0
        for target_alias in conf['targets_alias']:
            # print(model_value)
            values=[]
            value_alias=target_alias[1]

            
            def find_value_in_group(group,value_alias):
                for value in group['values']:
                    if value[0]==value_alias:
                        set_model_value(value[1])
                        return value[1]
                if model_value['v']==None:
                    # print(group,value_alias)
                    print("err!!!!!, at least the first data source should be complete")
                    exit(1)
                return model_value['v']

            #收集对应value alias在不同group里的值
            for group in groups:
                # print(model_value)
                values.append(find_value_in_group(group,value_alias))
            
            
            # values maybe [[a,b,c],[a,b,c]]
            # we need to make it into [a,a][b,b][c,c]
            bars_values=[]
            if isinstance(values[0], list):
                # 前缀和
                for value in values:
                    for i in range(1,len(value)):
                        value[i]=value[i-1]+value[i]

                sub_v_cnt=len(values[0])
                for i in range(sub_v_cnt):
                    bars_values.append([value[i] for value in values])
            else:
                bars_values.append(values)
            
            print(meta['value_y'],value_alias,bars_values)

            level=len(bars_values)
            def leveled_color(color,curlevel):
                def hex_to_rgb(hex_color):
                    # 将十六进制颜色转换为 RGB 元组
                    hex_color = hex_color.lstrip('#')
                    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

                def rgb_to_hex(rgb_color):
                    # 将 RGB 元组转换为十六进制颜色
                    return '#{:02x}{:02x}{:02x}'.format(*rgb_color)

                def adjust_brightness(hex_color, factor):
                    # 确保因子在 0 到 2 的范围内
                    factor = max(min(factor, 2.0), 0.0)
                    # 将十六进制颜色转换为 RGB
                    rgb = hex_to_rgb(hex_color)
                    # 调整亮度
                    new_rgb = tuple(min(int(c * factor), 255) for c in rgb)
                    # 将新的 RGB 值转换为十六进制颜色
                    new_color = rgb_to_hex(new_rgb)
                    return new_color

                return adjust_brightness(color,1-0.15*curlevel)
            for barlevel,bar_values in reversed(list(enumerate(bars_values))):
                plot.bar(index+value_idx*bar_width,bar_values,bar_width,
                    color=(leveled_color(colors[value_idx%len(colors)],barlevel)),
                    label=value_alias,edgecolor="black"
                )
            value_idx+=1
        plotidx+=1
# 调整子图的边距以确保图例不会覆盖图表内容

    
    plt.tight_layout()

    plt.subplots_adjust(top=0.9,right=0.6,wspace=0.25, hspace=0.25)
    # plt.legend(loc='upper right')
    
    # 调整图例位置到图表外
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='xx-small')

    # plt.legend(fontsize='xx-small')
    

    
    plt.show()
    
    
def pipeline():
    import sys
    if len(sys.argv)!=2:
        print("usage: python draw_bar.py <xxx.yaml>")
        exit(1)

    yamlfilepath=sys.argv[1]

    drawconf=yaml.safe_load(open(yamlfilepath, 'r'))

    print("\n\n get_record_filelist")
    conf_2_files=get_record_filelist(drawconf)

    print("\n\n check_first_draw_group_match_avg_cnt")
    check_first_draw_group_match_avg_cnt(drawconf,conf_2_files)

    print("\n\n get_each_group_prev_avg_cnt_file__compute_avg")
    records=get_each_group_prev_avg_cnt_file__compute_avg(drawconf,conf_2_files)

    print("\n\n flatten records")
    records=[records[confstr] for confstr in records]
    for record in records:
        # record.print_attributes()
        print(record.configstr)
    # print([r.configstr for r in records])
    
    print("\n\n group_records")
    groups=group_records(records,drawconf)
    
    print("\n\n to_draw_meta")
    drawmeta=to_draw_meta(groups,drawconf)
    
    print("\n\n")
    pprint(drawmeta)
    draw_with_draw_meta(drawmeta,drawconf)
    # import matplotlib.pyplot as plt
    # from collections import defaultdict


    # groups = defaultdict(list)
    # for record in records:
    #     key_parts = record[0].split(".")
    #     common_part = ".".join(key_parts[1:5])
    #     algorithm = "".join(key_parts[5:len(key_parts) - 1])
    #     algorithm = algorithm.split(")")
    #     algorithm = ")\n".join(algorithm)
    #     record[5] = algorithm
    #     groups[common_part].append(record)


    # for group_name, group_records in groups.items():
    #     data_points = {
    #         'Cost': [row[1] for row in group_records],
    #         'Latency': [row[2] for row in group_records],
    #     }
    #     costs = data_points['Cost']
    #     latencies = data_points['Latency']
    #     value_for_money = [(1 / latency) * 1 / cost if cost != 0 and latency != 0 else float('inf') for latency, cost in zip(latencies, costs)]  # 防止除以零
    #     data_points['Performance_Cost'] = value_for_money

    #     x_ticks = [row[5] for row in group_records]

    #     for key, values in data_points.items():
    #         plt.figure()
    #         bars = plt.bar(range(len(values)), values)
    #         plt.title(f'Comparison of {key} in {group_name}')
    #         plt.xlabel('Experiment')
    #         plt.ylabel(key)
    #         plt.xticks(range(len(values)), x_ticks, fontsize = 9)
    #         plt.subplots_adjust(bottom = 0.21)

    #         for bar in bars:
    #             height = bar.get_height()
    #             plt.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.4f}', ha='center', va='bottom')

    #         plt.show()

pipeline()