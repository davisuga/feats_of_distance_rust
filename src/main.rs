use fred::prelude::*;
use ntex::web;
use reqwest;
use scylla::{statement::Consistency, ExecutionProfile, Session, SessionBuilder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod batch;
pub mod db;
pub mod etl;
pub mod fetch;
pub mod parquet;
pub mod task;
pub mod types;

use db::setup_keyspace;
use etl::process_artist;
use fetch::{get_api_key, get_client};
use task::{complete_task, enqueue_tasks, setup_task_table};

struct AppState {
    session: Arc<Session>,
    redis_client: RedisClient,
    http_client: reqwest::Client,
    auth_token: Mutex<String>,
}

#[derive(Deserialize)]
struct ArtistIds {
    ids: Vec<String>,
}

#[derive(Serialize)]
struct ProcessingResult {
    successful: Vec<String>,
    failed: Vec<String>,
}
async fn process_single_artist(
    state: &web::types::State<Arc<AppState>>,
    artist_id: &str,
    retry_count: &mut i32,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let auth_token = state.auth_token.lock().await.clone();
        match process_artist(
            artist_id,
            &state.redis_client,
            &state.session,
            &auth_token,
            &state.http_client,
        )
        .await
        {
            Ok(_) => {
                if let Err(e) = complete_task(&state.session, artist_id).await {
                    return Err(e.into());
                }
                return Ok(());
            }
            Err(e) => {
                if *retry_count == 0 {
                    let _ = refresh_token(state.get_ref().clone()).await;
                    *retry_count += 1;
                } else {
                    if let Err(enqueue_err) =
                        enqueue_tasks(&state.session, vec![artist_id.to_string()]).await
                    {
                        eprintln!("Error re-enqueueing task: {:?}", enqueue_err);
                    }
                    return Err(e);
                }
            }
        }
    }
}

#[web::post("/process_artists")]
async fn process_artists(
    state: web::types::State<Arc<AppState>>,
    artist_ids: web::types::Json<ArtistIds>,
) -> Result<web::HttpResponse, web::Error> {
    let mut successful = Vec::new();
    let mut failed = Vec::new();
    let mut retry_count = 0;

    for artist_id in artist_ids.into_inner().ids.iter() {
        let result = process_single_artist(&state, artist_id, &mut retry_count).await;
        match result {
            Ok(_) => successful.push(artist_id.clone()),
            Err(_) => failed.push(artist_id.clone()),
        }
    }

    Ok(ntex::web::HttpResponse::Ok().json(&ProcessingResult { successful, failed }))
}
#[web::get("/health")]
async fn health(_: web::types::State<Arc<AppState>>) -> web::HttpResponse {
    web::HttpResponse::Ok().body("OK")
}
#[ntex::main]
async fn main() -> std::io::Result<()> {
    let uri = std::env::var("SCYLLA_URI").unwrap();
    let profile = ExecutionProfile::builder()
        .consistency(Consistency::One)
        .request_timeout(None)
        .build();
    let handle = profile.into_handle();
    let session: Session = SessionBuilder::new()
        .default_execution_profile_handle(handle)
        .known_node(uri)
        .build()
        .await
        .expect("Failed to create Scylla session");
    println!("Created Scylla session");
    let config = RedisConfig::from_url(std::env::var("REDIS_URI").unwrap().as_str())
        .expect("Failed to create Redis config");
    let redis_client = fred::types::Builder::from_config(config)
        .build()
        .expect("Failed to create Redis client");
    redis_client.connect();
    redis_client
        .wait_for_connect()
        .await
        .expect("Failed to connect to Redis");
    println!("Connected to Redis");
    let http_client = get_client();
    let initial_token = get_api_key(&http_client)
        .await
        .expect("Failed to get initial API key");
    println!("Got initial token");
    setup_keyspace(&session)
        .await
        .expect("Failed to setup keyspace");
    setup_task_table(&session)
        .await
        .expect("Failed to setup task table");
    println!("Setup keyspace and task table");
    let state = Arc::new(AppState {
        session: Arc::new(session),
        redis_client,
        http_client,
        auth_token: Mutex::new(initial_token),
    });

    web::HttpServer::new(move || {
        web::App::new()
            .state(state.clone())
            .service(process_artists)
            .service(health)
    })
    .bind(("127.0.0.1", 3000))?
    .run()
    .await
}

async fn refresh_token(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3500));
    loop {
        interval.tick().await;
        match get_api_key(&state.http_client).await {
            Ok(new_token) => {
                let mut token = state.auth_token.lock().await;
                *token = new_token;
            }
            Err(e) => {
                eprintln!("Failed to refresh token: {:?}", e);
            }
        }
    }
}
