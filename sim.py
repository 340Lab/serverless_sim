

import numpy as np        


# # 用于记录整体的负载情况，成本情况
# class SimPeriod:
#     # 某个单位时间段所有node的快照
#     node_snapshots
#     # 某个单位时间段对应请求
#     request_snapshots

# # 请求
# # 按照一定的概率 + 基础函数生成
# class Request:     
#     # 调用链路
#     fn_dag

# # 函数dag关系
# # class FnDAG:
    
# # 函数
# class Function:
#     # 单位时间cpu资源
#     cpu
#     # 单位内存资源
#     mem
#     # 依赖的数据单元集合
#     datacells
#     # 访问次数
#     datacells_visit_time
    

# class FunctionInstance:

#     # 运行在的node
#     node
#     # 函数
#     fn
#     # 是否使用缓存

# # 一开始位置是给定的
# # 数据库
# # 数据集合
# class DataBase:
#     # 运行在的node
#     node

# # 函数依赖的数据最小单元
# class DataCell:
#     # DataCell所在的DataBase
#     db
#     # 数据大小
#     data_size

# # 分发请求，调度，扩缩容
# class ServerlessController:
#     sss

# 计算节点
# 节点一加入，生成与现有节点的通信速率，
class Node:
    #数据库容器
    # databases
    # #函数容器
    # functions
    # #serverless总控节点
    # serverless_controller
    #资源限制：cpu, mem
    rsc_limit={
        "cpu":10, #单位时间运算量
        "mem":10 #内存容量
    }

# # class Node2NodeGraph:
# #     # 节点到节点的网速



class SimEnv:
    # node集合，node与node间的关系图
    nodes=[]
    # # 单位时间段
    # periods
    # 节点间网速图
    node2node_graph=np.zeros((0,0),float)
    # # 所有请求的记录
    # requests_record


    def set_speed_btwn(self,n1:int,n2:int,speed:float):
        assert(n1!=n2)
        def _set_speed_btwn(self:SimEnv,nbig:int,nsmall:int,speed:float):
            self.node2node_graph[nbig][nsmall]=speed
        if n1>n2:
            _set_speed_btwn(self,n1,n2,speed)
        else:
            _set_speed_btwn(self,n2,n1,speed)
    
    def get_speed_btwn(self,n1:int,n2:int):
        def _get_speed_btwn(nbig:int,nsmall:int):
            return self.node2node_graph[nbig][nsmall]
        if n1>n2:
            return _get_speed_btwn(n1,n2)
        else:
            return _get_speed_btwn(n2,n1)

    def init(self):
        def _init_one_node(self:SimEnv):
            node=Node()

            self.nodes.append(node)
            nodecnt=len(self.nodes)
            # self.node2node_graph.resize((nodecnt,nodecnt)) 
            # print(self.node2node_graph)
            for i in range(nodecnt-1):
                self.set_speed_btwn(i, nodecnt-1, 1)
            print(self.node2node_graph)

        dim=10
        self.node2node_graph=np.zeros((dim,dim),float)
        for i in range(dim):
            _init_one_node(self)

simenv=SimEnv()
simenv.init()
print(simenv.node2node_graph)