use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[tokio::main]
pub async fn start() {
    // initialize tracing
    tracing_subscriber::fmt::init();

    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        // .route("/", get(root))
        // `POST /users` goes to `create_user`
        .route("/step", post(step));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// basic handler that responds with a static string
// async fn root() -> &'static str {
//     "Hello, World!"
// }

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref SIM_ENV: Mutex<SimEnv> = Mutex::new(SimEnv::new());
}

async fn step(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<StepReq>,
) -> (StatusCode, Json<StepResp>) {
    let sim_env = SIM_ENV.lock();

    sim_env

    // insert your application logic here
    let resp = StepResp {
        score: todo!(),
        state: todo!(),
    };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(user))
}

// the input to our `create_user` handler
#[derive(Deserialize)]
struct StepReq {
    action: u32,
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct StepResp {
    score: f32,
    state: State,
}
