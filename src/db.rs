use std::{error::Error, future::IntoFuture, time::Instant, vec};

use futures::future::join;
use scylla::{batch::Batch, serialize::batch::BatchValues, statement::Consistency, SessionBuilder};

use crate::{
    batch::chunked_parallel_batch,
    types::{Artist, NormalizedTrack},
};

pub async fn insert_data(
    tracks: &Vec<NormalizedTrack>,
    artists: &Vec<Artist>,
    session: &scylla::Session,
) -> Result<(), Box<dyn Error>> {
    let before = Instant::now();

    match join(
        chunked_parallel_batch(&session, "INSERT INTO music.tracks (id, name, preview_url, artists) VALUES (?, ?, ?, ?)", &tracks),
        chunked_parallel_batch(&session, "INSERT INTO music.artists (id, name) VALUES (?, ?)", &artists),
    )
    .await
    {
        (Ok(_), Ok(_)) => {}
        (Err(e), _) => return Err(Box::new(e)),
        (_, Err(e)) => return Err(Box::new(e)),
    };

    println!("Insertion took {:?}", before.elapsed());

    Ok(())
}
pub async fn setup_keyspace(session: &scylla::Session) -> Result<(), Box<dyn Error>> {
    let mut  prepared = session.prepare("CREATE KEYSPACE IF NOT EXISTS music WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}").await?;
    prepared.set_consistency(Consistency::All);
    session.execute(&prepared, ()).await?;
    // Create the music.tracks table
    let mut prepared = session.prepare("CREATE TABLE IF NOT EXISTS music.tracks (id text, created_at timestamp, name text, preview_url text, artists list<text>, PRIMARY KEY (id))").await?;
    prepared.set_consistency(Consistency::All);
    session.execute(&prepared, ()).await?;
    // Create the music.artists table
    let mut prepared = session
        .prepare("CREATE TABLE IF NOT EXISTS music.artists (id text, created_at timestamp, name text, PRIMARY KEY (id))")
        .await?;
    prepared.set_consistency(Consistency::All);
    session.execute(&prepared, ()).await?;
    Ok(())
}
