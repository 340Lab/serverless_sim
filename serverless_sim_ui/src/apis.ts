import axios from "axios"



class GetNetworkTopoRespExist {
    constructor(
        public topo:number[][],
    ){}
}

class GetNetworkTopoRespNotFound {
    constructor(
        public msg:string,
    ){}
}

class GetNetworkTopoResp{
    kernel: any
    private id: number=0
    
    exist():undefined| GetNetworkTopoRespExist{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    not_found():undefined| GetNetworkTopoRespNotFound{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
}


class GetNetworkTopoReq {
    constructor(
        public env_id:string,
        public a:number,
        public b:number,
        public c:boolean,
        public d:number[],
        public e:number[][],
    ){}
}

class ApiCaller {
    async get_network_topo(req:GetNetworkTopoReq):Promise<GetNetworkTopoResp>{
        return await axios.post("/api/get_network_topo", req)
    }
}


