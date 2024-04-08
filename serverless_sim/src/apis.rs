
use serde_json::json;
use serde::{Serialize, Deserialize};
use axum::{http::StatusCode, routing::post, Json, Router};
use async_trait::async_trait;
use crate::network::ApiHandlerImpl;


#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetNetworkTopoResp{
    Exist{
        topo:Vec<Vec<i32>>,
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
    pub fn serialize(&self)->String {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        }).to_string()
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct GetNetworkTopoReq {
        env_id:String,
        a:i32,
        b:f64,
        c:bool,
        d:Vec<i32>,
        e:Vec<Vec<i32>>,
}


#[async_trait]
pub trait ApiHandler {
    
    async fn handle_get_network_topo(&self, req:GetNetworkTopoReq)->GetNetworkTopoResp;
            
}


pub fn add_routers(mut router:Router)->Router
{
    
    async fn get_network_topo(Json(req):Json<GetNetworkTopoReq>)-> (StatusCode, Json<GetNetworkTopoResp>){
        (StatusCode::OK, Json(ApiHandlerImpl.handle_get_network_topo(req).await))
    }
    router=router
        .route("/get_network_topo", post(get_network_topo));
                             
    
    router
}

