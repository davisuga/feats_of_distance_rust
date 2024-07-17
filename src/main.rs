use db::{insert_data, setup_keyspace};
use fetch::{fetch_albums_with_tracks, fetch_all_items, get_api_key, get_client, SPOTIFY_API_BASE};
use itertools::Itertools;

pub mod batch;
pub mod db;
pub mod task;
pub mod types;
use redis::AsyncCommands;
use reqwest::{self};
use scylla::SessionBuilder;
use task::{complete_task, dequeue_task, enqueue_tasks, setup_task_table, ArtistTask};
use std::{collections::HashSet, time::{Duration, Instant}};
use tokio::{self, time::interval};
use types::{Album, NormalizedTrack, Track};
pub mod fetch;
use std::error::Error;
async fn process_artist(
    artist_id: &str,
    redis_client: &mut redis::aio::Connection,
    session: &scylla::Session,
    auth_token: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn Error>> {
    println!("Processing artist {:?}", artist_id);
    let lock_key = format!("lock:artist:{}", artist_id);
    let lock_result: bool = redis_client.set_nx(&lock_key, "locked").await?;

    if !lock_result {
        println!("Artist {} is already being processed by another instance", artist_id);
        return Ok(());
    }

    // Set expiration to prevent dead locks
    redis_client.expire(&lock_key, 30).await?;


    // Fetch albums
    let albums_url = format!("{}/artists/{}/albums?limit=50", SPOTIFY_API_BASE, artist_id);
    let albums_raw: Vec<Album> = fetch_all_items(&client, &albums_url, auth_token).await?;
    println!("Fetched albums {:?}", albums_raw.len());

    let all_tracks_base: Vec<Track> = fetch_albums_with_tracks(
        &client,
        albums_raw.iter().map(|a| a.id.as_str()).collect(),
        &auth_token,
    )
    .await?;
    let all_tracks: &Vec<NormalizedTrack> = &all_tracks_base
        .clone()
        .into_iter()
        .filter(|t| t.artists.len() > 1)
        .map(|track| NormalizedTrack {
            id: track.id,
            name: track.name,
            preview_url: track.preview_url,
            artists: track.artists.iter().map(|a| a.id.clone()).collect(),
        })
        .collect();
    let all_artists = &all_tracks_base
        .into_iter()
        .flat_map(|t| t.artists)
        .unique()
        .collect::<Vec<_>>();

    insert_data(&all_tracks, &all_artists, &session).await?;

    println!("Mutated artist {:?}", artist_id);
    let artist_id_set: HashSet<String> = all_artists.into_iter().map(|a| a.id.clone()).collect();
    // // Check processed artists in Redis
    let before = Instant::now();
    let processed_artists: HashSet<String> = redis_client.smembers("processed_artists").await?;
    println!("Fetched processed artists in {:?}", before.elapsed());
    let unprocessed_artists: HashSet<_> = artist_id_set.difference(&processed_artists).collect();
    
    // Send unprocessed artists to RabbitMQ
    let before = Instant::now();
    enqueue_tasks(&session, unprocessed_artists.to_owned().into_iter().cloned().collect()).await?;
    println!("Published artists to process in {:?}", before.elapsed());
    println!(
        "Published artists to process: {:?}",
        unprocessed_artists.len()
    );

    // Mark initial artist as processed in Redis
    let before = Instant::now();
    redis_client.sadd("processed_artists", artist_id).await?;
    redis_client.del(&lock_key).await?;
    println!("Marked artist {:?} as processed in {:?}", artist_id, before.elapsed());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // RabbitMQ setup
    // println!("Connecting to RabbitMQ");
    // let conn = Connection::connect(
    //     std::env::var("RABBITMQ_URI").unwrap().as_str(),
    //     ConnectionProperties::default(),
    // )
    // .await?;
    // println!("Connected to RabbitMQ");
    
    // Redis setup
    let redis_client = redis::Client::open(std::env::var("REDIS_URI").unwrap().as_str())?;
    let mut redis_conn = redis_client.get_async_connection().await?;
    println!("Redis client created");
    // Consume messages

    let uri = std::env::var("SCYLLA_URI").unwrap();

    let session: scylla::Session = SessionBuilder::new().known_node(uri).build().await?;
    let mut last_token_timestamp = Instant::now();
    let http_client = get_client();
    let mut last_token = get_api_key(&http_client).await?;
    println!("Got token {:?}", last_token);
    setup_keyspace(&session).await?;
    println!("Created keyspace");
    setup_task_table(&session).await?;
    println!("Created task table");
    let mut interval = interval(Duration::from_millis(500));
    loop {
        interval.tick().await;
        let unprocessed_artists = dequeue_task(&session).await?;
        if let Some(ArtistTask {artist_id, ..}) = unprocessed_artists {
            let now = Instant::now();
            if now.duration_since(last_token_timestamp).as_secs() > 3500 {
                last_token = get_api_key(&http_client).await?;
                last_token_timestamp = now;
            }

            if let Err(e) = process_artist(&artist_id, &mut redis_conn, &session, &last_token, &http_client).await {
                eprintln!("Error processing artist {}: {:?}", artist_id, e);
                // Requeue the message
                enqueue_tasks(&session, vec![artist_id]).await?;
            } else {
                complete_task(&session, &artist_id).await?;
                println!("processed in {:?}", now.elapsed());
                
            }
        }
    }
}
