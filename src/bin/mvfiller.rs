use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = "127.0.0.1:9042".to_string();
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build().
        await?;

    // create keyspace and table
    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
        {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}",
        &[],
    ).await?;
    session.query("CREATE TABLE IF NOT EXISTS ks.cf (a int primary key, b int, c int)",
        &[],
    ).await?;
    session.query("CREATE MATERIALIZED VIEW IF NOT EXISTS ks.mv AS SELECT * FROM ks.cf WHERE b IS NOT NULL PRIMARY KEY (b, a)",
        &[],
    ).await?;

    // Insert values
    let prepared = session.prepare("INSERT INTO ks.cf (a, b, c) VALUES(?, ?, ?)").await?;
    for i in 0..1000 {
        session.execute(&prepared, (i,i/5,i)).await?;
    }
    // if let Some(rows) = session.query("SELECT a FROM ks.mv WHERE b = ?", (v,)).await?.rows {
    //     // Parse each row as a tuple containing a single i32
    //     for row in rows.into_typed::<(i32,)>() {
    //         let read_row: (i32,) = row?;
    //         println!("Read a value from row: {}", read_row.0);
    //     }
    // }

    Ok(())
}
