use std::{error::Error, future::IntoFuture, time::Instant, vec};

// use dgraph_tonic::{ClientVariant, Mutate, Mutation};
use futures::{
    future::{join, join_all},
    FutureExt, TryFutureExt,
};
use scylla::{batch::Batch, serialize::batch::BatchValues, statement::Consistency, SessionBuilder};
use serde_json::json;

use crate::types::{Artist, NormalizedTrack, Track};

pub async fn insert_data(
    // album: Vec<NormalizedAlbum>,
    tracks: &Vec<NormalizedTrack>,
    artists: &Vec<Artist>,
    session: &scylla::Session,
) -> Result<(), Box<dyn Error>> {
    // let album_json = json!({
    //     "uid": "_:album",
    //     "dgraph.type": "Album",
    //     "id": album.id,
    //     "name": album.name,
    //     "release_date": album.release_date,
    //     "type": album.album_type,
    //     "tracks": album.tracks.iter().map(|track_id| json!({"uid": format!("_:{}", track_id)})).collect::<Vec<_>>(),
    // });
    // let albums_json = album.iter().map(|album| {
    //     json!({
    //         "uid": format!("_:{}", album.id),
    //         "dgraph.type": "Album",
    //         "id": album.id,
    //         "name": album.name,
    //         "release_date": album.release_date,
    //         "type": album.album_type,
    //         "tracks": album.tracks.iter().map(|track_id| json!({"uid": format!("_:{}", track_id)})).collect::<Vec<_>>(),
    //     })
    // }).collect::<Vec<_>>();

    // let tracks_json = tracks.iter().map(|track| {
    //     json!({
    //         "uid": format!("_:{}", track.id),
    //         "dgraph.type": "Track",
    //         "id": track.id,
    //         "name": track.name,
    //         "preview_url": track.preview_url,
    //         "artists": track.artists.iter().map(|artist| json!({"uid": format!("_:{}", artist)})).collect::<Vec<_>>(),
    //     })
    // }).collect::<Vec<_>>();

    // let artists_json = artists.iter().map(|artist| {
    //     json!({
    //         "uid": format!("_:{}", artist.id),
    //         "dgraph.type": "Artist",
    //         "id": artist.id,
    //         "name": artist.name,
    //     })
    // }).collect::<Vec<_>>();

    // let mutation = json!({
    //     "set": {

    //         "tracks": tracks_json,
    //         "artists": artists_json,
    //     }
    // });

    // let mut mu = Mutation::new();
    // mu.set_set_json(&mutation.to_string())?;
    // let mut txn = client.new_mutated_txn();
    // txn.mutate(mu).await?;
    // txn.commit().await?;

    // println!("Data inserted successfully {:}", json!({
    //     "set": {

    //         "tracks": tracks_json,
    //         "artists": artists_json,
    //     }
    // }));
    // Ok(())

    // Creates the keyspace if it doesn't exist

    let before = Instant::now();

    let mut batch: Batch = Default::default();
    let mut tracks_batch: Batch = Default::default();
    // let insert_track = session
    //     .prepare("INSERT INTO music.tracks (id, name, preview_url, artists) VALUES (?, ?, ?, ?)")
    //     .await?;
    for _ in tracks {
        tracks_batch.append_statement(
            "INSERT INTO music.tracks (id, name, preview_url, artists) VALUES (?, ?, ?, ?)",
        )
    }
    for _ in artists {
        batch.append_statement("INSERT INTO music.artists (id, name) VALUES (?, ?)")
    }

    match join(
        session.batch(&mut tracks_batch, tracks),
        session.batch(&mut batch, artists),
    )
    .await
    {
        (Ok(_), Ok(_)) => {}
        (Err(e), _) => return Err(Box::new(e)),
        (_, Err(e)) => return Err(Box::new(e)),
    };
    // println!("{:?}",session.query("select * from music.tracks  ", &[]).await?.rows().unwrap().len());
    println!("Insertion took {:?}", before.elapsed());

    // insert_track.set_consistency(Consistency::Any);

    // let insert_artist = session
    //     .prepare("INSERT INTO music.artists (id, name) VALUES (?, ?)")
    //     .await?;
    // insert_artist.set_consistency(Consistency::Any);

    // // Insert tracks
    // let track_futures = tracks.iter().map(|track| {
    //     session.execute(
    //         &insert_track,
    //         (&track.id, &track.name, &track.preview_url, &track.artists),
    //     )
    // });

    // // Insert artists
    // let artist_futures = artists
    //     .iter()
    //     .map(|artist| session.execute(&insert_artist, (&artist.id, &artist.name)));

    // // Combine futures and execute in parallel
    // let combined_futures = track_futures.chain(artist_futures);
    // futures::stream::iter(combined_futures)
    //     .buffer_unordered(100) // Adjust concurrency as needed
    //     .collect::<Vec<_>>()
    //     .await;

    Ok(())
}
pub async fn setup_keyspace(session: &scylla::Session) -> Result<(), Box<dyn Error>> {
    let mut  prepared = session.prepare("CREATE KEYSPACE IF NOT EXISTS music WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}").await?;
    prepared.set_consistency(Consistency::All);
    session.execute(&prepared, ()).await?;
    // Create the music.tracks table
    let mut prepared = session.prepare("CREATE TABLE IF NOT EXISTS music.tracks (id text, name text, preview_url text, artists list<text>, PRIMARY KEY (id))").await?;
    prepared.set_consistency(Consistency::All);
    session.execute(&prepared, ()).await?;
    // Create the music.artists table
    let mut prepared = session
        .prepare("CREATE TABLE IF NOT EXISTS music.artists (id text, name text, PRIMARY KEY (id))")
        .await?;
    prepared.set_consistency(Consistency::All);
    session.execute(&prepared, ()).await?;
    Ok(())
}
