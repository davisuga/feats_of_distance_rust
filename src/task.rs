
use scylla::{batch::Batch, IntoTypedRows, Session, SessionBuilder};

// Updated struct to represent our task in ScyllaDB
#[derive(Debug)]
pub struct ArtistTask {
    pub artist_id: String,
    pub status: String,
}

pub async fn setup_task_table(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    session.query(
        "CREATE TABLE IF NOT EXISTS music.artist_tasks (
            artist_id text PRIMARY KEY,
            status text
        )",
        &[],
    ).await?;
    Ok(())
}

pub async fn enqueue_tasks(session: &Session, artist_ids: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let mut batch = Batch::default();
    for _ in artist_ids.clone() {
        batch.append_statement("INSERT INTO music.artist_tasks (artist_id, status) VALUES (?, ?)"); 
    }
    let statuses = artist_ids.iter().map(|artist_id| (artist_id,"pending")).collect::<Vec<_>>();
    session.batch(&mut batch, statuses).await?;
    Ok(())
}

pub async fn dequeue_task(session: &Session) -> Result<Option<ArtistTask>, Box<dyn std::error::Error>> {
    let result = session.query(
        "SELECT artist_id, status FROM music.artist_tasks WHERE status = ? LIMIT 1 ALLOW FILTERING",
        ("pending",),
    ).await?;
    
    if let Some(rows) = result.rows {
        for row in rows.into_typed::<(String, String)>() {
            let (artist_id, status) = row?;
            // Mark the task as processing
            session.query(
                "UPDATE music.artist_tasks SET status = ? WHERE artist_id = ? IF status = ?",
                ("processing", &artist_id, "pending"),
            ).await?;
            return Ok(Some(ArtistTask { artist_id, status }));
        }
    }
    Ok(None)
}

pub async fn complete_task(session: &Session, artist_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    session.query(
        "DELETE FROM music.artist_tasks WHERE artist_id = ?",
        (artist_id,),
    ).await?;
    Ok(())
}
