import axios from "axios"



export class GetNetworkTopoRespExist {
    constructor(
        public topo:number[][],
    ){}
}

export class GetNetworkTopoRespNotFound {
    constructor(
        public msg:string,
    ){}
}

export class GetNetworkTopoResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
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


export class GetNetworkTopoReq {
    constructor(
        public env_id:string,
    ){}
}

export namespace apis {
    export async function get_network_topo(req:GetNetworkTopoReq):Promise<GetNetworkTopoResp>{
        let res:any = await axios.post("/api/get_network_topo", req)
        return new GetEnvIdResp(res.data.kernel,res.data.id)
    }
}




export class GetEnvIdRespExist {
    constructor(
        public env_id:string[],
    ){}
}

export class GetEnvIdRespNotFound {
    constructor(
        public msg:string,
    ){}
}

export class GetEnvIdResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    exist():undefined| GetEnvIdRespExist{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    not_found():undefined| GetEnvIdRespNotFound{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
}


export namespace apis {
    export async function get_env_id():Promise<GetEnvIdResp>{
        let res:any = await axios.post("/api/get_env_id", )
        return new GetEnvIdResp(res.data.kernel,res.data.id)
    }
}


