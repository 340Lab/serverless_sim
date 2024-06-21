import requests
from pprint import pprint
import yaml
import re
import matplotlib.pyplot as plt
import numpy as np

### doc: https://fvd360f8oos.feishu.cn/docx/RMjfdhRutoDmOkx4f4Lcl1sjnzd

class PackedRecord:
    # configstr.clone().into(),
    # cost_per_req,
    # time_per_req,
    # score,
    # rps.into(),
    # f.time_str.clone().into()
    raw_record=[]

    configstr=""
    cost_per_req=0.0
    time_per_req=0.0
    score=0.0
    rps=0.0
    filename=""

    rand_seed=""
    request_freq=""
    dag_type=""
    cold_start=""
    fn_type=""
    scale_num=""
    scale_down_exec=""
    scale_up_exec=""

    def __init__(self, raw_record):
        if len(raw_record) != 6:
            raise ValueError("The input list must contain exactly 6 elements.")
        self.configstr = raw_record[0]
        self.cost_per_req = raw_record[1]
        self.time_per_req = raw_record[2]
        self.score = raw_record[3]
        self.rps = raw_record[4]
        self.filename = raw_record[5]

        # compute sub values by config str
        self.parse_configstr()

    def parse_configstr(self):
        config_patterns = [
            (r'sd(\w+)\.rf', 'rand_seed'),
            (r'\.rf(\w+)\.', 'request_freq'),
            (r'\.dt(\w+)\.', 'dag_type'),
            (r'\.cs(\w+)\.', 'cold_start'),
            (r'\.ft(\w+)\.', 'fn_type'),
            (r'\.scl\(([^)]+)\)\(([^)]+)\)\(([^)]+)\)\.', 'scale_num', 'scale_down_exec', 'scale_up_exec'),
            (r'\.scd\(([^)]+)\)', 'sche')
        ]

        for pattern, *keys in config_patterns:
            match = re.search(pattern, self.configstr)
            if match:
                values = match.groups()
                for key, value in zip(keys, values):
                    setattr(self, key, value)
        self.print_attributes()

        
    def print_attributes(self):
        attributes = [
            'configstr', 'cost_per_req', 'time_per_req', 'score', 'rps', 'filename',
            'rand_seed', 'request_freq', 'dag_type', 'cold_start', 'fn_type', 
            'scale_num', 'scale_down_exec', 'scale_up_exec', 'sche'
        ]
        for attr in attributes:
            print(f"{attr}={getattr(self, attr)}")

def get_raw_records():
    SIM_URL = "http://127.0.0.1:3000/"
    def get_records():
        return requests.post(SIM_URL+"get_seeds_metrics",json=[""]).json()

    records = get_records()[""]
    return records
    # print(records)

    # pprint(records)

def pack_records(records):
    new_records=[]
    for record in records:
        new_records.append(PackedRecord(record))
    return new_records

def filter_by_conf(records,yamlfile):
    filters = yamlfile['filter']
    # target_alias = yamlfile['targets_alias']
    # group_by = yamlfile['group_by']
    # value_y_configs = yamlfile['values']

    def record_matches_filters(record, filters):
        for key, value in filters.items():
            if getattr(record, key) != value:
                print("drop record",record.configstr)
                return False
        match_one=False
        for target in yamlfile['targets_alias']:
            def match_args(args):
                for argkey in args:
                    if getattr(record, argkey)!=args[argkey]:
                        # print(argkey,getattr(record, argkey),args[argkey])
                        # record.print_attributes()
                        return False
                return True
            if match_args(target[0]):
                match_one=True
                break
        if not match_one:
            return False
        print('record match filter',record.configstr)
        return True

    filtered_records = [record for record in records if record_matches_filters(record, filters)]

    return filtered_records

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
    fig, plots = plt.subplots(1, 3, figsize=(18, 6))
    # plots=[ax,ax1,ax2]

    plt.subplots_adjust(left=0.1, right=0.9, top=0.85, bottom=0.1)
    fig.set_size_inches(12, 4.5)
    bar_width = 0.1
    index = np.arange(len(conf['group']['types']))
    opacity = 0.4
    error_config = {'ecolor': '0.3'}
    patterns = ('x', '\\', '*', 'o', '.','O')
    colors=["#FC6B05","#FFB62B","#65B017","#99D8DB","#9BB7BB"]
    
    plotidx=0
    for plot in plots:
        meta=drawmeta[plotidx]
        groups=meta['groups']
        plot.set_xticks(index)
        plot.set_xticklabels(conf['group']['type_alias'])

        plot.set_xlabel(conf['group']['alias'])
        plot.set_ylabel(meta['value_y'])

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
                    color=(leveled_color(colors[value_idx],barlevel)),
                    label=value_alias,edgecolor="black"
                )
            value_idx+=1
        plotidx+=1
    plt.tight_layout()
    plt.legend(loc='upper right')
    plt.show()
    
def pipeline():
    
    print("\n\n get_raw_records")
    records=get_raw_records()
    
    print("\n\n pack_records")
    records=pack_records(records)
    
    with open("draw.yaml", 'r') as stream:
        yamlfile = yaml.safe_load(stream)
    
    print("\n\n filter_by_conf")
    records=filter_by_conf(records,yamlfile)
    
    print("\n\n group_records")
    groups=group_records(records,yamlfile)
    
    print("\n\n to_draw_meta")
    drawmeta=to_draw_meta(groups,yamlfile)
    
    print("\n\n")
    pprint(drawmeta)
    draw_with_draw_meta(drawmeta,yamlfile)
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