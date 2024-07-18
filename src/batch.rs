use futures::future::join_all;
use itertools::Itertools;
use scylla::{
    batch::{Batch, BatchStatement}, serialize::{batch::BatchValues, row::SerializeRow}, statement::Consistency, transport::errors::QueryError, QueryResult, Session
};

pub async fn chunked_parallel_batch<T, S>(
    session: &Session,
    statement: S,
    values: &Vec<T>,
) -> Result<Vec<QueryResult>, QueryError>
where
    T: SerializeRow + Sync + Send + Clone,
    S: Into<BatchStatement>+Copy
{
    const CHUNK_SIZE: usize = 700;

    let chunks: Vec<_> = values.chunks(CHUNK_SIZE).collect();
    let consistency = std::env::var("CONSISTENCY").unwrap_or("any".to_string());
    let consistency = match consistency.as_str() {
        "any" => Consistency::Any,
        "one" => Consistency::One,
        "two" => Consistency::Two,
        "three" => Consistency::Three,
        "quorum" => Consistency::Quorum,
        "all" => Consistency::All,
        "local_quorum" => Consistency::LocalQuorum,
        "each_quorum" => Consistency::EachQuorum,
        "local_one" => Consistency::LocalOne,
        _ => Consistency::Any,
    };
    let futures = chunks.into_iter().map(|chunk| {
        let mut batch = Batch::default();
        batch.set_consistency(consistency);
        for _ in chunk {
            batch.append_statement(statement);
        }
        async move { session.batch(&batch, chunk).await }
    });

    let results = join_all(futures).await;

    // Collect errors, if any
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    if !errors.is_empty() {
        // Return the first error encountered
        return Err(errors[0].clone());
    }

    // If no errors, unwrap all results
    Ok(results.into_iter().map(|r| r.unwrap()).collect())
}
