

       
import random
import sim_simenv

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

# 函数
class Function:
    unique_i:int=-1
    #  #运算量/s 一个普通请求处理函数请求的运算量为1，
    cpu = 1
    # 平均时间占用内存资源 mb
    mem = 300
    # 依赖的数据库-整个过程中数据传输量
    databases_2_throughput={}
    # 输出数据量 mb
    out_put_size=100
    # 输出到的函数, 如果为None，则为终止节点
    next_fns=[]

    def rand_fn(unique_i:int):
        fn=Function()
        fn.cpu=random.uniform(0.3,10)
        fn.mem=random.uniform(100,1000)
        fn.out_put_size=random.uniform(0.1,20)
        fn.unique_i=unique_i
        return fn


# 函数dag关系
class FnDAG:
    entry_fn:Function

    def instance_single_fn(env:sim_simenv.SimEnv):
        dag=FnDAG()
        dag.entry_fn=Function.rand_fn(env.alloc_fn_id())
    
    def instance_map_reduce(env:sim_simenv.SimEnv,map_cnt:int):
        dag=FnDAG()
        dag.entry_fn=Function.rand_fn() 
        end_fn=Function.rand_fn()
        end_fn.next_fns=None
        for i in range(map_cnt):
            next=Function.rand_fn(env.alloc_fn_id())
            next.next_fns.append(end_fn)
            dag.entry_fn.next_fns.append(next)

        return dag
    
    # def fn_cnt():
    #     map=dict()
    #     def _fn_cnt(fn:Function):
    #         for next_fn_ in fn.next_fns:
    #             next_fn:Function=next_fn_
    #             map[next_fn.]
    #             cnt+=_fn_cnt(next_fn)
    #         return cnt
    #     return _fn_cnt(self.entry_fn)
    

# class FunctionInstance:
#     # 运行在的node
#     node
#     # 函数
#     fn
#     # 是否使用缓存

# # 一开始位置是给定的
# # 数据库
# # 数据集合
class DataBase:
    # 运行在的node
    node=None
    
    # cpu
    cpu=1

    # 占用内存
    mem=1000
    
class RequestInstance:
    fn_dag_i=-1
    request_timedf=0

# # 函数依赖的数据最小单元
# class DataCell:
#     # DataCell所在的DataBase
#     db
#     # 数据大小
#     data_size

# # 分发请求，调度，扩缩容
# class ServerlessController:
#     sss

simenv=sim_simenv.SimEnv()
simenv.init()
# simenv.start_one_simu()
simenv.step(0)