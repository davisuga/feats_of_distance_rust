// use polars::prelude::*;
// use serde::{Serialize, Deserialize};

// use crate::types::NormalizedTrack;

// // }


// fn write_to_parquet<T: Serialize + Clone+IntoSeries>(data: &Vec<NormalizedTrack>, file_path: &str) -> Result<(), PolarsError> {
//     data.iter().for_each(|t| {
//         println!("{:?}", t);
//     });
//     // Convert the data to a DataFrame
//     let df = DataFrame::new(struct_to_dataframe!(data, [id, name, preview_url, artists]))?;
    
//     // Write the DataFrame to a Parquet file
//     let mut file = std::fs::File::create("docs/data/path.parquet").unwrap();
//     ParquetWriter::new(&mut file).finish(&mut df).unwrap();
    
    
//     Ok(())
// }

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let artists = vec![
//         Artist { id: "1".to_string(), name: "Artist 1".to_string() },
//         Artist { id: "2".to_string(), name: "Artist 2".to_string() },
//     ];

//     let tracks = vec![
//         NormalizedTrack {
//             id: "1".to_string(),
//             name: "Track 1".to_string(),
//             preview_url: Some("http://example.com/1".to_string()),
//             artists: vec!["1".to_string(), "2".to_string()],
//         },
//         NormalizedTrack {
//             id: "2".to_string(),
//             name: "Track 2".to_string(),
//             preview_url: None,
//             artists: vec!["2".to_string()],
//         },
//     ];

//     write_to_parquet(&artists, "artists.parquet")?;
//     write_to_parquet(&tracks, "tracks.parquet")?;

//     println!("Parquet files written successfully.");

//     Ok(())
// }