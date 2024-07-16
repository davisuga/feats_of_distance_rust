pub const SPOTIFY_API_BASE: &str = "https://api.spotify.com/v1";
#[derive(Debug)]
enum ApiKeyError {
    RequestError(reqwest::Error),
    RegexError(regex::Error),
    TokenNotFound,
}


impl std::fmt::Display for ApiKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ApiKeyError::RequestError(e) => write!(f, "Request error: {}", e),
            ApiKeyError::RegexError(e) => write!(f, "Regex error: {}", e),
            ApiKeyError::TokenNotFound => write!(f, "Could not find access token"),
        }
    }
}

impl Error for ApiKeyError {}

impl From<reqwest::Error> for ApiKeyError {
    fn from(error: reqwest::Error) -> Self {
        ApiKeyError::RequestError(error)
    }
}

impl From<regex::Error> for ApiKeyError {
    fn from(error: regex::Error) -> Self {
        ApiKeyError::RegexError(error)
    }
}

pub async fn get_api_key(
    client: &reqwest::Client,
) -> Result<String, Box<dyn Error>> {
 
    let text = client
        .get("https://open.spotify.com")
        .send()
        .await?
        .text()
        .await?;
    let re = Regex::new(r#""accessToken":\s*"([^"]+)""#)?;
    // println!("{:?}",text);
    if let Some(caps) = re.captures(&text) {
        if let Some(token) = caps.get(1) {
            return Ok(token.as_str().to_string());
        }
    }
    Err(Box::new(ApiKeyError::TokenNotFound))
}
use std::error::Error;
use futures::{stream, StreamExt};
use regex::Regex;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::types::Track;


const CONCURRENT_REQUESTS: usize = 16;
const TRACKS_LIMIT: usize = 20;

pub async fn fetch_albums_with_tracks(
    client: &Client,
    all_albums: Vec<&str>,
    auth_token: &str,
) -> Result<Vec<Track>, Box<dyn Error>> {
    let album_chunks: Vec<Vec<&str>> = all_albums.chunks(20).map(|chunk| chunk.to_vec()).collect();
    
    let albums_with_tracks = stream::iter(album_chunks)
        .map(|chunk| {
            let client = client.clone();
            let ids = chunk.join(",");
            async move {
                fetch_albums_with_initial_tracks(&client, &ids, auth_token).await
            }
        })
        .buffer_unordered(CONCURRENT_REQUESTS)
        .collect::<Vec<_>>()
        .await;

    let mut all_tracks = Vec::new();
    let mut albums_needing_more_tracks = Vec::new();

    for result in albums_with_tracks {
        match result {
            Ok((albums, tracks)) => {
                all_tracks.extend(tracks);
                for album in albums {
                    if album.total_tracks > TRACKS_LIMIT {
                        albums_needing_more_tracks.push(album);
                    }
                }
            }
            Err(e) => eprintln!("Error fetching albums: {}", e),
        }
    }

    // Fetch remaining tracks for albums with more than 20 tracks
    let additional_tracks = stream::iter(albums_needing_more_tracks)
        .map(|album| {
            let client = client.clone();
            async move {
                fetch_remaining_tracks(&client, &album.id, album.total_tracks, auth_token).await
            }
        })
        .buffer_unordered(CONCURRENT_REQUESTS)
        .collect::<Vec<_>>()
        .await;

    for result in additional_tracks {
        match result {
            Ok(tracks) => all_tracks.extend(tracks),
            Err(e) => eprintln!("Error fetching additional tracks: {}", e),
        }
    }

    Ok(all_tracks)
}

async fn fetch_albums_with_initial_tracks(
    client: &Client,
    ids: &str,
    auth_token: &str,
) -> Result<(Vec<AlbumInfo>, Vec<Track>), Box<dyn Error>> {
    let url = format!("{}/albums?ids={}", SPOTIFY_API_BASE, ids);
    let response: Value = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", auth_token))
        .send()
        .await?
        .json()
        .await?;

    let mut albums = Vec::new();
    let mut tracks = Vec::new();

    if let Some(albums_array) = response["albums"].as_array() {
        for album in albums_array {
            if let (Some(id), Some(total_tracks)) = (album["id"].as_str(), album["total_tracks"].as_u64()) {
                albums.push(AlbumInfo {
                    id: id.to_string(),
                    total_tracks: total_tracks as usize,
                });
            }

            if let Some(items) = album["tracks"]["items"].as_array() {
                tracks.extend(serde_json::from_value::<Vec<Track>>(json!(items))?);
            }
        }
    }

    Ok((albums, tracks))
}

async fn fetch_remaining_tracks(
    client: &Client,
    album_id: &str,
    total_tracks: usize,
    auth_token: &str,
) -> Result<Vec<Track>, Box<dyn Error>> {
    let mut all_tracks = Vec::new();
    let mut offset = TRACKS_LIMIT;

    while offset < total_tracks {
        let url = format!("{}/albums/{}/tracks?offset={}&limit=50", SPOTIFY_API_BASE, album_id, offset);
        let response: Value = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", auth_token))
            .send()
            .await?
            .json()
            .await?;

        if let Some(items) = response["items"].as_array() {
            all_tracks.extend(serde_json::from_value::<Vec<Track>>(json!(items))?);
        }

        offset += 50;
    }

    Ok(all_tracks)
}
#[derive(Debug, Serialize, Deserialize)]

struct AlbumInfo {
    id: String,
    total_tracks: usize,
}

pub async fn fetch_all_items<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    auth_token: &str,
) -> Result<Vec<T>, Box<dyn Error>> {
    let mut all_items = Vec::new();
    let mut next_url = Some(url.to_string());

    while let Some(url) = next_url {
        let response: Value = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", auth_token))
            .send()
            .await?
            .json()
            .await?;

        all_items.extend(response["items"].as_array().unwrap().iter().cloned());
        next_url = response["next"].as_str().map(String::from);
    }

    Ok(serde_json::from_value(json!(all_items))?)
}

pub fn get_client() -> reqwest::Client {
    let mut headers = header::HeaderMap::new();
    headers.insert("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7".parse().unwrap());
    headers.insert("accept-language", "en-GB,en;q=0.9".parse().unwrap());
    headers.insert("cache-control", "no-cache".parse().unwrap());

    headers.insert("pragma", "no-cache".parse().unwrap());
    headers.insert("priority", "u=0, i".parse().unwrap());
    headers.insert(
        "sec-ch-ua",
        "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\""
            .parse()
            .unwrap(),
    );
    headers.insert("sec-ch-ua-mobile", "?0".parse().unwrap());
    headers.insert("sec-ch-ua-platform", "\"macOS\"".parse().unwrap());
    headers.insert("sec-fetch-dest", "document".parse().unwrap());
    headers.insert("sec-fetch-mode", "navigate".parse().unwrap());
    headers.insert("sec-fetch-site", "none".parse().unwrap());
    headers.insert("sec-fetch-user", "?1".parse().unwrap());
    headers.insert("upgrade-insecure-requests", "1".parse().unwrap());
    headers.insert("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36".parse().unwrap());
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    client
}