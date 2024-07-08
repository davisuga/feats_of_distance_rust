use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
use rspotify::http::HttpClient;
use rspotify::{Config, Token};
use rspotify::{model::AlbumId, prelude::*, ClientCredsSpotify, Credentials};

use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};


#[derive(Clone, Debug, Default)]
pub struct CustomSpotifyClient {
    http: HttpClient,

    creds: Credentials,
    config: Config,
}

impl CustomSpotifyClient {
    pub fn new(client_creds_spotify: ClientCredsSpotify) -> Self {
        Self {
            http: client_creds_spotify.get_http().clone(),
            
            creds: client_creds_spotify.creds,
            config: client_creds_spotify.config,
        }
    }

    pub async fn get_token(&self) -> Result<String, reqwest::Error> {
        // Assume this custom implementation exists
        // This is where you'd put your custom token retrieval logic
        unimplemented!("Custom get_token() implementation")
    }
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create an in-memory CozoDB instance
    // let db = DbInstance::new("rocksdb", "db", Default::default())?;
    // let yt = ytmapi_rs::YtMusic::from_cookie_file(cookie_path).await?;

    
    // Ingest data
    // ingest_data(&db)?;

    // Find a path between two artists
    let start_artist = "Febem";
    let end_artist = "Smile";
    // find_path(&db, start_artist, end_artist)?;
    // interactive_cli(&db)?;
    let token = extract_access_token(&fetch_page("https://open.spotify.com").await?).unwrap();
    println!("{:?}",token);
    let spotify = rspotify::ClientCredsSpotify::from_token(
        Token {
            access_token: token,
            ..Default::default()
        }
    );
    let birdy_uri = AlbumId::from_uri("spotify:album:0sNOF9WDwhWunNAHPD3Baj").unwrap();
    let albums = spotify.album(birdy_uri, None).await;
    println!("{:?}",albums);
    BaseClient::auto_reauth(&spotify).await?;
    rspotify::model::artist::CursorPageFullArtists
    Ok(())
}
use regex::Regex;


async fn fetch_page(url: &str) -> Result<String, reqwest::Error> {

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"));
    headers.insert(ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"));

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();


    
    
    let response = client.get(url).send().await?;
    let body = response.text().await?;
    
    Ok(body)

}
fn extract_access_token(input: &str) -> Option<String> {
    let re = Regex::new(r#""accessToken":\s*"([^"]+)""#).unwrap();
    if let Some(caps) = re.captures(input) {
        if let Some(token) = caps.get(1) {
            return Some(token.as_str().to_string());
        }
    }
    None
}
