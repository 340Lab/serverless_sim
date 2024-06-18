import requests
from pprint import pprint
SIM_URL = "http://127.0.0.1:3000/"
def get_records():
    return requests.post(SIM_URL+"get_seeds_metrics",json=[""]).json()

records = get_records()[""]
# print(records)



import matplotlib.pyplot as plt
from collections import defaultdict


groups = defaultdict(list)
for record in records:
    key_parts = record[0].split(".")
    common_part = ".".join(key_parts[1:5])
    algorithm = "".join(key_parts[5:len(key_parts) - 1])
    algorithm = algorithm.split(")")
    algorithm = ")\n".join(algorithm)
    record[5] = algorithm
    groups[common_part].append(record)


for group_name, group_records in groups.items():
    data_points = {
        'Cost': [row[1] for row in group_records],
        'Latency': [row[2] for row in group_records],
    }
    costs = data_points['Cost']
    latencies = data_points['Latency']
    value_for_money = [(1 / latency) * 1 / cost if cost != 0 and latency != 0 else float('inf') for latency, cost in zip(latencies, costs)]  # 防止除以零
    data_points['Performance_Cost'] = value_for_money

    x_ticks = [row[5] for row in group_records]

    for key, values in data_points.items():
        plt.figure()
        bars = plt.bar(range(len(values)), values)
        plt.title(f'Comparison of {key} in {group_name}')
        plt.xlabel('Experiment')
        plt.ylabel(key)
        plt.xticks(range(len(values)), x_ticks, fontsize = 9)
        plt.subplots_adjust(bottom = 0.21)

        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.4f}', ha='center', va='bottom')

        plt.show()


# def collect_data(dag_type):
#     algos=[
#         ["upai","scrule","Cejoss"],
#         ["uphpa","scrule","HPA+DECODS"],
#         ["uplass","scrule","LaSS+DECODS"],
#         ["upfnsche","scfnsche","FnSched"],
#         ["upfaasflow","scfaasflow","FaasFlow"],
#     ]

#     cold_start = "cshigh" 
#     fn_type = "ftcpu" 
#     # dag_type="dtdag"

#     def algo_idx(key):
#         idx=0
#         for algo in algos:
#             if key.find(algo[0])!=-1 and key.find(algo[1])!=-1:
#                 return idx
#             idx+=1
#         return -1
#     def rf_idx(key):
#         if key.find("rflow")!=-1:
#             return 0
#         if key.find("rfmiddle")!=-1:
#             return 1
#         if key.find("rfhigh")!=-1:
#             return 2
#         return -1
#     def perform_cost_ratio(latency, cost):
#         return 1/latency/cost

#     collect=[
#     ]
#     for a in algos:
#         collect.append([a[2],0,0,0])

#     def collect_add(key,record,algo_idx_,rf_idx_):
#         collect[algo_idx_][rf_idx_+1]=record
                

#     for r in records:
#         key=r[0]
#         algo_idx_=algo_idx(key)
#         if key.find(cold_start)!=-1 and\
#             key.find(fn_type)!=-1 and\
#             key.find(dag_type)!=-1 and\
#             algo_idx_!=-1:
#             collect_add(key,r,algo_idx_,rf_idx(key))
#     return collect

# collect=collect_data("dtdag")
# collect_=collect_data("dtsingle")
# # pprint(list(map(lambda x: x[1],collect[0][1:])))

# import numpy as np
# import matplotlib.pyplot as plt
# from matplotlib.ticker import MaxNLocator
# from collections import namedtuple

# n_groups = len(collect[0])-1

# each_algo_freqs=[]





# # fig, ax = plt.subplots()
# # fig, (ax2, ax1,ax) = plt.subplots(1, 3, figsize=(12, 4))


# fig, ax = plt.subplots()
# C1="#364CBA"
# C2="#00B3B0"
# C3="#008F51"
# ax2 = plt.subplot(2, 3, 1)
# ax1 = plt.subplot(2, 3, 2)
# ax = plt.subplot(2,3, 3)
# ax2_ = plt.subplot(2, 3, 4)
# ax1_ = plt.subplot(2, 3, 5)
# ax_ = plt.subplot(2,3, 6)
# # def ax_color(ax_,c):
# #     for s in ax_.spines: 
# #         ax_.spines[s].set_color(c)
# # ax_color(ax2,C3)
# # ax_color(ax1,C2)
# # ax_color(ax,C1)
# # ax_color(ax2_,C3)
# # ax_color(ax1_,C2)
# # ax_color(ax_,C1)
# plt.subplots_adjust(left=0.1, right=0.9, top=0.85, bottom=0.1)
# fig.set_size_inches(12, 4.5)

# index = np.arange(n_groups)
# bar_width = 0.1

# opacity = 0.4
# error_config = {'ecolor': '0.3'}

# # rects1 = ax.bar(index, rflow, bar_width,
# #                 alpha=opacity, color='b',
# #                  error_kw=error_config,
# #                 label='Low')


# def update_line(ax,ax1,ax2,collect):
    
#     idx=0
#     patterns = ('x', '\\', '*', 'o', '.','O')
#     colors=["#FC6B05","#FFB62B","#65B017","#99D8DB","#9BB7BB"]
    
#     first_cost=[]
#     first_latency=[]
#     first_pricequality=[]
#     for c in collect:
#         # print(list(map(lambda r:r[1],c[1:])))
#         def pee(f,s,l,c):
#             # print(list(map(l,c)))
#             if len(f)==0:
#                 f+=(list(map(l,c)))
#             else:
#                 # print first percentage exceeded
#                 current=list(map(l,c))
#                 # percentage exceeded
#                 def pe(cur,old):
#                     return (cur-old)/old
#                 print(pe(f[0],current[0]),pe(f[1],current[1]),pe(f[2],current[2]))
        
#         def set_first_cost(f):
#             first_cost=f
#         def set_first_latency(f):
#             first_latency=f
#         def set_first_pricequality(f):
#             first_pricequality=f

#         print(c[0])

#         pee(first_cost,set_first_cost,lambda r:r[1],c[1:])
#         pee(first_latency,set_first_latency,lambda r:r[2],c[1:])
#         pee(first_pricequality,set_first_pricequality,lambda r:1/r[1]/r[2],c[1:])

#         rects1 = ax.bar(index+idx*bar_width, list(map(lambda r:r[1],c[1:])), bar_width,
#                     alpha=1, color=colors[idx],
#                     error_kw=error_config,
#                     label=c[0],edgecolor="black")
#         rects2 = ax1.bar(index+idx*bar_width, list(map(lambda r:r[2],c[1:])), bar_width,
#                     alpha=1, color=colors[idx],
#                     error_kw=error_config,
#                     label=c[0],edgecolor="black")
#         rects3 = ax2.bar(index+idx*bar_width, list(map(lambda r:1/r[1]/r[2],c[1:])), bar_width,
#                     alpha=1, color=colors[idx],
#                     error_kw=error_config,
#                     label=c[0],edgecolor="black")
#         def set_ax(ax_,y,c):
#             ax_.set_xlabel('Request Frequency')
#             ax_.set_ylabel(y)
#             # _ax.set_title('Scores by group and gender')
#             ax_.set_xticks(index + bar_width / 2)
#             ax_.set_xticklabels(('Low', 'Middle', 'High'))
#             # ax_.legend()

#         set_ax(ax,"Cost (100rmb)",C1)
#         set_ax(ax1,"Latency (10ms)",C2)
#         set_ax(ax2,"Quality-Price Ratio",C3)
#         idx+=1

# update_line(ax,ax1,ax2,collect)
# update_line(ax_,ax1_,ax2_,collect_)
# # rects2 = ax.bar(index + bar_width, rfmiddle, bar_width,
# #                 alpha=opacity, color='r',
# #                 error_kw=error_config,
# #                 label='Middle')

# fig.tight_layout()
# plt.subplots_adjust(top=0.9)

# # ax.legend(bbox_to_anchor=(0.1,0.1),borderaxespad=0)
# yoff=0.1
# plt.subplot(2, 3, 1)
# plt.legend(loc='upper center', bbox_to_anchor=(1.7, 1.25+yoff), ncol=5)
# # plt.subplot(2, 3, 2)
# # plt.legend(loc='upper center', bbox_to_anchor=(1.7-1.3, 1.15+yoff), ncol=5)
# # plt.subplot(2, 3, 3)
# # plt.legend(loc='upper center', bbox_to_anchor=(1.7-1.3*2, 1.05+yoff), ncol=5)
# plt.show() 