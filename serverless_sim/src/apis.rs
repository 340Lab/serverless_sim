
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



#[async_trait]
pub trait ApiHandler {
    
    async fn handle_get_network_topo(&self, req:GetNetworkTopoReq)->GetNetworkTopoResp;
            
    async fn handle_get_env_id(&self, )->GetEnvIdResp;
            
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
                             
    
    router
}

