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




export class ResetRespSuccess {
    constructor(
        public env_id:string,
    ){}
}

export class ResetRespInvalidConfig {
    constructor(
        public msg:string,
    ){}
}

export class ResetResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    success():undefined| ResetRespSuccess{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    invalid_config():undefined| ResetRespInvalidConfig{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
}


export class ResetReq {
    constructor(
        public config:any,
    ){}
}

export namespace apis {
    export async function reset(req:ResetReq):Promise<ResetResp>{
        let res:any = await axios.post("/api/reset", req)
        return new GetEnvIdResp(res.data.kernel,res.data.id)
    }
}




export class StepRespSuccess {
    constructor(
        public state:string,
        public score:number,
        public stop:boolean,
        public info:string,
    ){}
}

export class StepRespEnvNotFound {
    constructor(
        public msg:string,
    ){}
}

export class StepResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    success():undefined| StepRespSuccess{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    env_not_found():undefined| StepRespEnvNotFound{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
}


export class StepReq {
    constructor(
        public env_id:string,
        public action:number,
    ){}
}

export namespace apis {
    export async function step(req:StepReq):Promise<StepResp>{
        let res:any = await axios.post("/api/step", req)
        return new GetEnvIdResp(res.data.kernel,res.data.id)
    }
}


