use axum::{http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::{actions::Action, sim_env::SimEnv, sim_state::State};

pub async fn start() {
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        // .route("/", get(root))
        // `POST /users` goes to `create_user`
        .route("/step", post(step))
        .route("/reset", post(reset))
        .route("/state_score", post(state_score));

    // run our app with hyper, listening globally on port 3000
    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
// async fn root() -> &'static str {
//     "Hello, World!"
// }

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref SIM_ENV: Mutex<SimEnv> = Mutex::new(SimEnv::new());
}

async fn reset() -> (StatusCode, ()) {
    log::info!("Reset sim env");
    let mut sim_env = SIM_ENV.lock().unwrap();
    *sim_env = SimEnv::new();

    (StatusCode::CREATED, ())
}

async fn step(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<StepReq>,
) -> (StatusCode, Json<StepResp>) {
    log::info!("Step sim env");
    let sim_env = SIM_ENV.lock().unwrap();

    sim_env.step(Action::try_from(payload.action).unwrap());

    // insert your application logic here
    let resp = StepResp {
        score: sim_env.score(),
        state: sim_env.state_str(),
        stop: false,
        info: "".to_owned(),
    };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(resp))
}

async fn state_score() -> (StatusCode, Json<StateScoreResp>) {
    let sim_env = SIM_ENV.lock().unwrap();

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (
        StatusCode::CREATED,
        Json(StateScoreResp {
            state: sim_env.state(),
            score: sim_env.score(),
        }),
    )
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
    state: String,
    stop: bool,
    info: String,
}

#[derive(Serialize)]
struct StateScoreResp {
    score: f32,
    state: State,
}
