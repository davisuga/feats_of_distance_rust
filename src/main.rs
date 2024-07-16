use db::{insert_data, setup_keyspace};
use fetch::{fetch_albums_with_tracks, fetch_all_items, get_api_key, get_client, SPOTIFY_API_BASE};
use itertools::Itertools;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
pub mod db;
pub mod task;
pub mod types;
use redis::AsyncCommands;
use reqwest::{self};
use scylla::SessionBuilder;
use std::{collections::HashSet, time::Instant};
use tokio::{self};
use types::{Album, NormalizedTrack, Track};
pub mod fetch;
use std::error::Error;
async fn process_artist(
    artist_id: &str,
    channel: &Channel,
    redis_client: &mut redis::aio::Connection,
    session: &scylla::Session,
    auth_token: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn Error>> {
    println!("Processing artist {:?}", artist_id);
    

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
    
    for artist_id in &unprocessed_artists {
        channel
            .basic_publish(
                "",
                "artist_queue",
                BasicPublishOptions::default(),
                artist_id.as_bytes().to_vec(),
                BasicProperties::default(),
            )
            .await;
    }
    println!("Published artists to process in {:?}", before.elapsed());
    println!(
        "Published artists to process: {:?}",
        unprocessed_artists.len()
    );

    // Mark initial artist as processed in Redis
    let before = Instant::now();
    redis_client.sadd("processed_artists", artist_id).await?;
    println!("Marked artist {:?} as processed in {:?}", artist_id, before.elapsed());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // RabbitMQ setup
    println!("Connecting to RabbitMQ");
    let conn = Connection::connect(
        std::env::var("RABBITMQ_URI").unwrap().as_str(),
        ConnectionProperties::default(),
    )
    .await?;
    println!("Connected to RabbitMQ");
    let channel = conn.create_channel().await?;
    let _queue = channel
        .queue_declare(
            "artist_queue",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    // Redis setup
    let redis_client = redis::Client::open(std::env::var("REDIS_URI").unwrap().as_str())?;
    let mut redis_conn = redis_client.get_async_connection().await?;
    println!("Redis client created");
    // Consume messages
    let consumer = channel
        .basic_consume(
            "artist_queue",
            "consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    println!("Consuming messages");
    let uri = std::env::var("SCYLLA_URI").unwrap();

    let session: scylla::Session = SessionBuilder::new().known_node(uri).build().await?;
    let mut last_token_timestamp = Instant::now();
    let http_client = get_client();
    let mut last_token = get_api_key(&http_client).await?;
    setup_keyspace(&session).await?;

    while let Some(delivery) = consumer.clone().into_iter().next() {
        let (channel, delivery) = delivery.expect("Error consuming message");
        let artist_id = String::from_utf8_lossy(&delivery.data);
        let now = Instant::now();
        if now.duration_since(last_token_timestamp).as_secs() > 3500 {
            last_token = get_api_key(&http_client).await?;
            last_token_timestamp = now;
        }
        if let Err(e) = process_artist(&artist_id, &channel, &mut redis_conn, &session, &last_token, &http_client).await {
            eprintln!("Error processing artist {}: {:?}", artist_id, e);
            // Requeue the message
            delivery
            .nack(BasicNackOptions::default())
            .await
            .expect("Nack message");
        } else {
            delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("Acknowledge message");
            println!("Acknowledged message");
        }
        
    }
    Ok(())
}
