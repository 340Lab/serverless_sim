
class SimEnv:
    # node集合，node与node间的关系图
    nodes
    # 单位时间段
    periods
    # 节点间网速图
    node2node_graph
    
class SimPeriod:
    # 用于记录整体的负载情况，成本情况

    # 某个单位时间段所有node的快照
    node_snapshots

class Request: 
    # 请求
    # 按照一定的概率+基础函数生成
    
    # 会触发的第一个函数
    trigger_function

class Function:
    # 函数

    # 单位时间cpu资源
    cpu
    # 单位内存资源
    mem
    # 依赖的数据单元集合
    datacells
    # 访问次数
    datacells_visit_time
    

class FunctionInstance:

    # 运行在的node
    node
    # 函数
    fn

class DataBase:
    # 数据库
    # 数据集合

    # 运行在的node
    node

class DataCell:
    # 函数依赖的数据最小单元

    # DataCell所在的DataBase
    db

    # 数据大小
    data_size

class ServerlessController:
    # 分发请求，调度，扩缩容

class Node:
    # 数据中心
    # 节点一加入，生成与现有节点的通信速率，

    #数据库容器
    databases

    #函数容器
    functions

    #serverless总控节点
    serverless_controller

class Node2NodeGraph:
    # 节点到节点的网速


def sim_init_env():
