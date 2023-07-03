import numpy as np 
import sim_node
import random
import sim 

class SimEnv:
    # node集合，node与node间的关系图
    nodes=[]

    # # 单位时间段
    # periods

    # 节点间网速图
    node2node_graph=np.zeros((0,0),float)

    # # 所有请求的记录
    # requests_record

    # 数据库，绑定到节点
    databases=[]

    # dag应用
    dags=[]

    _fn_next_id=0

    def alloc_fn_id(self):
        ret=self._fn_next_id
        self._fn_next_id+=1
        return ret

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
            node=sim_node.Node()

            self.nodes.append(node)
            nodecnt=len(self.nodes)
            print("nodecnt",nodecnt)
            # self.node2node_graph.resize((nodecnt,nodecnt)) 
            # print(self.node2node_graph)
            for i in range(nodecnt-1):
                randspeed=random.uniform(8000,10000)
                self.set_speed_btwn(i, nodecnt-1, randspeed)
            print(self.node2node_graph)

        # init nodes graph
        dim=10
        self.node2node_graph=np.zeros((dim,dim),float)
        for i in range(dim):
            _init_one_node(self)

        # # init databases
        # databases_cnt=5
        # for i in range(databases_cnt):
        #     db=DataBase()
        #     # bind a database to node
        #     while True:
        #         rand_node_i=random.randint(0,dim-1)
        #         if self.nodes[rand_node_i].database==None:
        #             self.nodes[rand_node_i].database=db
        #             db.node=self.nodes[rand_node_i]
        #             break
        #     self.databases.append(db)

        # init dags
        for i in range(50):
            self.dags.append(sim.FnDAG.instance_map_reduce(
                random.randint(2,10)))
        for i in range(50):
            self.dags.append(sim.FnDAG.instance_single_fn())
    
    def start_one_simu(self):
        # 总时间
        ms=60000
        frametime_ms=100
        frames_reqs=[]
        controller = Controller()
        # 100ms为一个仿真帧
        for i in range(ms/frametime_ms):
            reqs=[]
            # 1. 生成该帧请求
            reqcnt=random.randint(70,120)
            for j in range(reqcnt):
                reqs.append(random.randint(0,len(self.dags)))
            frames_reqs.append(reqs)
        
        

        # 2. 每个仿真帧，每个请求，分配到一个节点上
        for i in range(ms/frametime_ms):
            frame_reqs=frames_reqs[i]
            for req in frames_reqs:
                controller.handle_req(req)

    
    
    def step(self,action:int):

        def generate_reqs(self:SimEnv,cnt:int):
            req_map={}
            for _ in range(cnt):
                def generate_one_req():
                    dag_i=random.randint(0,len(self.dags)-1)
                    if dag_i in req_map:
                        req_map[dag_i]+=1
                    else:
                        req_map[dag_i]=1

                generate_one_req()
            map(lambda kv:kv[0],req_map)
            
        req_cnt=random.randint(2,len(self.dags))
        reqs=generate_reqs(self,req_cnt)
        print("reqs",reqs)

        
#         if action ==0:
# #             a rule based行为是选择一个优化空间最大的节点

#         if action ==1:
# #             b 随即行为是选择一个随机节点
#         if action ==2:
# #             c 缩容一个节点（当前没有正在运行的任务）
#         if action ==3:
# #             d 缩容一个随机节点（当前没有正在运行的任务）
# class DAGPut:
#     functions_put_place=[]
#     def __init__(self,fn_dag:FnDAG):
#         self.functions_put_place.resize(len(fn_dag.fn_cnt))
