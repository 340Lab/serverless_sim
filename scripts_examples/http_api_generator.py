### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


### read conf
import yaml
with open('http_conf.yaml') as f:
    yamldata = yaml.load(f, Loader=yaml.FullLoader)
BACKEND=yamldata["backend"]
FRONTEND=yamldata["frontend"]
API_LIST=yamldata["api_list"]


### construct logics
import os

def big_camel(name):
    #snake to big camel
    return name.title().replace("_","")

def big_camel_2_snake(camel):
    #big camel to snake
    return "".join(["_"+c.lower() if c.isupper() else c for c in camel]).lstrip("_")

#######################################

def gen_type_ts(type):
    if isinstance(type,list):
        if type[0]=="Array":
            return f"{gen_type_ts(type[1])}[]"
        else:
            exit(f"unknown type {type}")
    else:
        if type=="String":
            return "string"
        elif type=="Int":
            return "number"
        elif type=="Float":
            return "number"
        elif type=="Bool":
            return "boolean"
        else:
            exit(f"unknown type {type}")
def gen_type_rs(type):
    if isinstance(type,list):
        if type[0]=="Array":
            return f"Vec<{gen_type_rs(type[1])}>"
        else:
            exit(f"unknown type {type}")
    else:
        if type=="String":
            return "String"
        elif type=="Int":
            return "i32"
        elif type=="Float":
            return "f64"
        elif type=="Bool":
            return "bool"
        else:
            exit(f"unknown type {type}")

#######################################

def gen_struct_ts(struct_name,desc):
    content=""
    for key, value in desc.items():
        content+=f"        public {key}:{gen_type_ts(value)},\n"
    content=content[:-1]
    return f"""
class {struct_name} {{
    constructor(
{content}
    ){{}}
}}
"""

def gen_struct_body_rs(desc):
    content=""
    for key, value in desc.items():
        content+=f"        {key}:{gen_type_rs(value)},\n"
    content=content[:-1]
    return f"""{{
{content}
}}"""

def gen_struct_rs(struct_name,desc):
    return f"""
#[derive(Debug, Serialize, Deserialize)]
pub struct {struct_name} {gen_struct_body_rs(desc)}
"""

#######################################

def gen_dispatch_ts(name,desc):
    content=""

    # dispatch_types=["undefined"]
    dispatch_fns=[]
    idx=1
    for key, value in desc.items():
        content+=gen_struct_ts(name+key,value)
        dispatch_fns.append(f"""
    {big_camel_2_snake(key)}():undefined| {name+key}{{
        if(this.id=={idx}){{
            return this.kernel
        }}
        return undefined
    }}
    """)
        idx+=1
        

    content+=f"""
class {name}{{
    kernel: any
    private id: number=0
    {"".join(dispatch_fns)}
}}
"""

    return content


def gen_dispatch_rs(name,desc):
    content=""

    # dispatch_types=["undefined"]
    dispatch_fns=[]

    dispatch_types=""
    dispatch_arms=""
    idx=1
    for key, desc in desc.items():
        # content+=gen_struct_rs(name+key,value)
        dispatch_types+=f"    {key}{gen_struct_body_rs(desc)},\n"
        dispatch_arms+=f"    {name}::{key}{{..}}=>{idx},\n"
        dispatch_fns.append(f"""
    fn {big_camel_2_snake(key)}(&self)->Option<&{name+key}> {{
        if self.id=={idx} {{
            return Some(&self.kernel)
        }}
        None
    }}
    """)
        idx+=1
        

    content+=f"""
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum {name}{{
{dispatch_types}
}}

impl {name} {{
    fn id(&self)->u32 {{
        match self {{
            {dispatch_arms}
        }}
    }}
    pub fn serialize(&self)->String {{
        json!({{
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        }}).to_string()
    }}
}}
"""
    return content
    
#######################################

def gen_front_ts():
    apis=f"{FRONTEND['header']}\n\n"
    for api_name, api in API_LIST.items():
        reqtype=big_camel(api_name)+"Req"
        resptype=big_camel(api_name)+"Resp"

        req_struct=gen_struct_ts(reqtype,api["req"])

        resp_content=gen_dispatch_ts(resptype,api["resp_dispatch"])
        
        apis+=f"""
{resp_content}
{req_struct}
class ApiCaller {{
    async {api_name}(req:{reqtype}):Promise<{resptype}>{{
        return {FRONTEND["http_call"].format(api_name)}
    }}
}}


"""
        
    # print(apis)
    os.makedirs(FRONTEND["dir"], exist_ok=True)
    with open(f'{FRONTEND["dir"]}/apis.ts', 'w') as f:
        f.write(apis)



def gen_back_rs():
    apis=f"""
use serde_json::json;
use serde::{{Serialize, Deserialize}};
use axum::{{http::StatusCode, routing::post, Json, Router}};
use async_trait::async_trait;
{BACKEND["header"]}
"""
    handle_traits=[]
    api_registers=[]
    for api_name, api in API_LIST.items():
        reqtype=big_camel(api_name)+"Req"
        resptype=big_camel(api_name)+"Resp"

        req_struct=gen_struct_rs(reqtype,api["req"])

        resp_content=gen_dispatch_rs(resptype,api["resp_dispatch"])

        handle_traits.append(f"""
    async fn handle_{api_name}(&self, req:{reqtype})->{resptype};
            """)
        
        api_registers.append(f"""
    async fn {api_name}(Json(req):Json<{reqtype}>)-> (StatusCode, Json<{resptype}>){{
        (StatusCode::OK, Json(ApiHandlerImpl.handle_get_network_topo(req).await))
    }}
    router=router
        .route("/{api_name}", post({api_name}));
                             """)
        apis+=f"""
{resp_content}
{req_struct}
"""
    apis+=f"""
#[async_trait]
pub trait ApiHandler {{
    {"".join(handle_traits)}
}}


pub fn add_routers(mut router:Router)->Router
{{
    {"".join(api_registers)}
    
    router
}}

"""
    os.makedirs(BACKEND["dir"], exist_ok=True)
    with open(f'{BACKEND["dir"]}/apis.rs', 'w') as f:
        f.write(apis)

#######################################
    
def gen_front():
    if FRONTEND["lan"]=="ts":
        gen_front_ts()
    else:
        exit(f"unknown language {FRONTEND['lan']}")
def gen_back():
    if BACKEND["lan"]=="rs":
        gen_back_rs()
    else:
        exit(f"unknown language {BACKEND['lan']}")

#######################################

gen_front()
gen_back()
