use scylla::SerializeRow;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Eq, PartialEq, Clone, Hash, Deserialize)]
pub struct Image {
    pub height: Option<u32>,
    pub url: String,
    pub width: Option<u32>,
}

#[derive(Debug, Serialize, Eq, PartialEq, Clone, Hash, Deserialize)]
pub struct Album {
    pub id: String,
    pub name: String,
    pub release_date: String,
    #[serde(rename = "type")]
    pub album_type: String,
    pub images: Vec<Image>,
    #[serde(default)]
    pub tracks: Vec<Track>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash)]
pub struct Track {
    pub id: String,
    pub name: String,
    pub preview_url: Option<String>,
    pub artists: Vec<Artist>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, SerializeRow)]
pub struct Artist {
    pub id: String,
    pub name: String,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, SerializeRow)]
pub struct NormalizedTrack {
    pub id: String,
    pub name: String,
    pub preview_url: Option<String>,
    pub artists: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash)]
pub struct NormalizedArtist {
    pub id: String,
    pub name: String,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash)]
pub struct NormalizedAlbum {
    pub id: String,
    pub name: String,
    pub release_date: String,
    #[serde(rename = "type")]
    pub album_type: String,
    pub tracks: Vec<String>,
}
fn normalize_albums(
    albums: Vec<Album>,
) -> (
    Vec<NormalizedAlbum>,
    Vec<NormalizedTrack>,
    Vec<NormalizedArtist>,
) {
    let mut normalized_albums = Vec::new();
    let mut normalized_tracks = Vec::new();
    let mut normalized_artists = Vec::new();
    let mut artist_ids = std::collections::HashSet::new();

    for album in albums {
        let mut album_tracks = Vec::new();

        for track in album.tracks {
            let normalized_track = NormalizedTrack {
                id: track.id.clone(),
                name: track.name,
                preview_url: track.preview_url,
                artists: track.artists.iter().map(|a| a.id.clone()).collect(),
            };
            album_tracks.push(normalized_track.id.clone());
            normalized_tracks.push(normalized_track);

            for artist in track.artists {
                if artist_ids.insert(artist.id.clone()) {
                    normalized_artists.push(NormalizedArtist {
                        id: artist.id,
                        name: artist.name,
                    });
                }
            }
        }

        normalized_albums.push(NormalizedAlbum {
            id: album.id,
            name: album.name,
            release_date: album.release_date,
            album_type: album.album_type,
            tracks: album_tracks,
        });
    }

    (normalized_albums, normalized_tracks, normalized_artists)
}
