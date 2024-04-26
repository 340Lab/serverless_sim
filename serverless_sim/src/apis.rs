
use serde_json::{json,Value};
use serde::{Serialize, Deserialize};
use axum::{http::StatusCode, routing::post, Json, Router};
use async_trait::async_trait;
use crate::network::ApiHandlerImpl;


#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetNetworkTopoResp{
    Exist{
       topo:Vec<Vec<f64>>,
    },

    NotFound{
       msg:String,
    },

}

impl GetNetworkTopoResp {
    fn id(&self)->u32 {
        match self {
                GetNetworkTopoResp::Exist{..}=>1,
                GetNetworkTopoResp::NotFound{..}=>2,
            }
    }

    pub fn serialize(&self)->Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct GetNetworkTopoReq {
    pub env_id:String,
}



#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetEnvIdResp{
    Exist{
       env_id:Vec<String>,
},
    NotFound{
       msg:String,
},

}

impl GetEnvIdResp {
    fn id(&self)->u32 {
        match self {
                GetEnvIdResp::Exist{..}=>1,
    GetEnvIdResp::NotFound{..}=>2,

        }
    }
    pub fn serialize(&self)->Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}




#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResetResp{
    Success{
       env_id:String,
},
    InvalidConfig{
       msg:String,
},

}

impl ResetResp {
    fn id(&self)->u32 {
        match self {
                ResetResp::Success{..}=>1,
    ResetResp::InvalidConfig{..}=>2,

        }
    }
    pub fn serialize(&self)->Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct ResetReq {
       pub config:Value,
}



#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StepResp{
    Success{
       state:String,
       score:f64,
       stop:bool,
       info:String,
},
    EnvNotFound{
       msg:String,
},

}

impl StepResp {
    fn id(&self)->u32 {
        match self {
                StepResp::Success{..}=>1,
    StepResp::EnvNotFound{..}=>2,

        }
    }
    pub fn serialize(&self)->Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct StepReq {
    pub env_id:String,
    pub action:i32,
}


#[async_trait]
pub trait ApiHandler {
    
    async fn handle_get_network_topo(&self, req:GetNetworkTopoReq)->GetNetworkTopoResp;
            
    async fn handle_get_env_id(&self, )->GetEnvIdResp;
            
    async fn handle_reset(&self, req:ResetReq)->ResetResp;
            
    async fn handle_step(&self, req:StepReq)->StepResp;
            
}


pub fn add_routers(mut router:Router)->Router
{
    
    async fn get_network_topo(Json(req):Json<GetNetworkTopoReq>)-> (StatusCode, Json<Value>){
        (StatusCode::OK, Json(ApiHandlerImpl.handle_get_network_topo(req).await.serialize()))
    }
    router=router
        .route("/get_network_topo", post(get_network_topo));
                             
    async fn get_env_id()-> (StatusCode, Json<Value>){
        (StatusCode::OK, Json(ApiHandlerImpl.handle_get_env_id().await.serialize()))
    }
    router=router
        .route("/get_env_id", post(get_env_id));

    // python脚本文件向前端请求的接口，前端调用的 reset 函数，用于创建模拟环境对象
    async fn reset(Json(req):Json<ResetReq>)-> (StatusCode, Json<Value>){
        (StatusCode::OK, Json(ApiHandlerImpl.handle_reset(req).await.serialize()))
    }
    router=router
        .route("/reset", post(reset));

    // python脚本文件向前端请求的接口，前端调用的 step 函数   
    async fn step(Json(req):Json<StepReq>)-> (StatusCode, Json<Value>){
        (StatusCode::OK, Json(ApiHandlerImpl.handle_step(req).await.serialize()))
    }
    router=router
        .route("/step", post(step));
                             
    
    router
}

