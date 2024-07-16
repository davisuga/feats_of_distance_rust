
fn interactive_cli(db: &DbInstance) -> Result<(), Box<dyn Error>> {
    loop {
        use std::io::{stdin, stdout, Write};
        let mut s = String::new();
        print!("> ");
        let _ = stdout().flush();
        stdin()
            .read_line(&mut s)
            .expect("Did not enter a correct string");
        if let Some('\n') = s.chars().next_back() {
            s.pop();
        }
        if let Some('\r') = s.chars().next_back() {
            s.pop();
        }
        println!("{:?}",db.run_script(&s, Default::default(), ScriptMutability::Mutable));
    }

}

fn run_query(db: &DbInstance, query: &str) -> Result<(), Box<dyn Error>> {
    db.run_script(query, Default::default(), ScriptMutability::Mutable)?;
    Ok(())
}
fn run_and_forget(db: &DbInstance, query: &str) {
    db.run_script(query, Default::default(), ScriptMutability::Mutable)
        .map(|_| ())
        .unwrap_or(());
}
fn ingest_data(db: &DbInstance) -> Result<(), Box<dyn Error>> {
    println!("Creating and populating relations");
    run_and_forget(
        db,
        "
    :create features {song_id: String, artist_id: String}",
    );
    run_and_forget(
        db,
        "
        :create artists {id: String => name: String}",
    );
    run_and_forget(
        db,
        "
        :create tracks {id: String => name: String}",
    );
    // Ingest features.csv
    run_query(
        db,
        "
        res[song_id, artist_id] <~ CsvReader(
            url: 'file://./features.csv',
            has_headers: true,
            types: ['String', 'String']
        )
        :create features {song_id: String, artist_id: String}
        ?[song_id, artist_id] := res[song_id, artist_id]
        :put features
    ",
    )?;
    println!("Ingested features");

    // Ingest artists.csv
    run_query(
        db,
        "
        res[name, id] <~ CsvReader(
            url: 'file://./artists.csv',
            has_headers: true,
            separator: '\t',
            types: ['String', 'String']
        )
        :create artists {id: String => name: String}
        ?[id, name] := res[name, id]
        :put artists
    ",
    )?;
    println!("Ingested artists");

    // Ingest tracks.csv
    run_query(
        db,
        "
        res[name, id] <~ CsvReader(
            url: 'file://./tracks.csv',
            has_headers: true,
            types: ['String', 'String']
        )
        :create tracks {id: String => name: String}
        ?[id, name] := res[name, id]
        :put tracks
    ",
    )?;
    println!("Ingested tracks");

    Ok(())
}

fn find_path(db: &DbInstance, start_artist: &str, end_artist: &str) -> Result<(), Box<dyn Error>> {
    let query = format!(
        "
        path[artist1, artist2] := *features[song, artist1], *features[song, artist2], artist1 != artist2
        start[id, name] := *artists[id, name], name = '{}'
        end[id, name] := *artists[id, name], name = '{}'

        ?[starting, goal, distance, path] <~ ShortestPathDijkstra(path[], start[], end[])
        ",
        start_artist, end_artist
    );
    println!("Query: {}", query);

    let result = db.run_script(&query, Default::default(), ScriptMutability::Immutable)?;
    println!("Result: {:?}", result);
    if let Some(path) = result.rows.first() {
        println!("Path found between {} and {}:", start_artist, end_artist);
        if let DataValue::Str(path_str) = &path[0] {
            let path_vec: Vec<&str> = path_str.split(", ").collect();
            for artist_id in path_vec {
                let artist_query =
                    format!("?[name, id] := artists[id, name], id = '{}'", artist_id);
                let artist_result = db.run_script(
                    &artist_query,
                    Default::default(),
                    ScriptMutability::Immutable,
                )?;
                if let Some(artist) = artist_result.rows.first() {
                    if let (DataValue::Str(name), DataValue::Str(id)) = (&artist[0], &artist[1]) {
                        println!("- {} (ID: {})", name, id);
                    }
                }
            }
        }
    } else {
        println!("No path found between {} and {}", start_artist, end_artist);
    }

    Ok(())
}
