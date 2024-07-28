use crate::db::insert_data;
use crate::fetch::{fetch_albums_with_tracks, fetch_all_items, SPOTIFY_API_BASE};
use itertools::Itertools;

use crate::task::enqueue_tasks;
use crate::types::{Album, NormalizedTrack, Track};

use reqwest::{self};
use std::{collections::HashSet, time::Instant};

use fred::prelude::*;
use std::error::Error;

pub async fn process_artist(
    artist_id: &str,
    redis_client: &fred::prelude::RedisClient,
    session: &scylla::Session,
    auth_token: &str,
    http_client: &reqwest::Client,
) -> Result<(), Box<dyn Error>> {
    println!("Processing artist {:?}", artist_id);
    let lock_key = format!("lock:artist:{}", artist_id);
    let lock_result: bool = redis_client
        .set(
            &lock_key,
            "locked",
            Some(Expiration::EX(30)),
            Some(SetOptions::NX),
            false,
        )
        .await?;
    println!("Locked artist {:?}", artist_id);

    // if !lock_result {
    //     println!(
    //         "Artist {} is already being processed by another instance",
    //         artist_id
    //     );
    //     return Ok(());
    // }

    let albums_url = format!("{}/artists/{}/albums?limit=50", SPOTIFY_API_BASE, artist_id);
    let albums_raw: Vec<Album> = fetch_all_items(&http_client, &albums_url, auth_token).await?;
    println!("Fetched albums {:?}", albums_raw.len());

    let all_tracks_base: Vec<Track> = fetch_albums_with_tracks(
        &http_client,
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
    enqueue_tasks(
        &session,
        unprocessed_artists
            .to_owned()
            .into_iter()
            .cloned()
            .collect(),
    )
    .await?;
    println!("Published artists to process in {:?}", before.elapsed());
    println!(
        "Published artists to process: {:?}",
        unprocessed_artists.len()
    );

    // Mark initial artist as processed in Redis
    let before = Instant::now();
    redis_client.sadd("processed_artists", artist_id).await?;
    redis_client.del(&lock_key).await?;
    println!(
        "Marked artist {:?} as processed in {:?}",
        artist_id,
        before.elapsed()
    );

    Ok(())
}
