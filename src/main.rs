// mvchecker is tool which connects to a specific Scylla node and checks the
// consistency of the data it holds for both base tables and view tables
// against the paired replica.
//
// Usage: mvcheck [node_address[:nodeport]]

use futures::StreamExt;
use scylla::{SessionBuilder, Session};
use scylla::statement::Consistency;
use scylla::transport::topology::Keyspace;
use scylla::transport::host_filter::AllowListHostFilter;
use std::error::Error;
use std::collections::HashMap;
use std::vec::Vec;
use std::sync::Arc;

/// Find token ranges that according to this keyspace's replication
/// strategy, belong to the given node.
/// When only_primary_range is set to false, we will scan all token
/// ranges for which this node holds data. Setting only_primary_range
/// to true will limit the scan only to this nodes *primary* ranges,
/// i.e., token ranges for which the selected node is its primary
/// owner. This is useful if running mvcheck *after a repair* of
/// both base table (first) and view table (second), so it can be
/// assumed that a base-view inconsistency can be checked on just
/// one base-view pair and doesn't need to checked RF times for
/// each base-view pair.
/// "node" should be the IP address representing a node, that the REST
/// API uses when listing that node.
async fn find_token_ranges(only_primary_range: bool, keyspace_name: &str, node: &str)
        -> Vec<(Option<i64>,Option<i64>)> {
    let rest_url = format!("http://{}:10000/storage_service/range_to_endpoint_map/{}",
        node, keyspace_name);
    let resp = reqwest::get(rest_url)
        .await.unwrap()
        .json::<Vec<HashMap<String, Vec<String>>>>()
        .await.unwrap();
    let mut ranges: Vec<(Option<i64>,Option<i64>)> = Vec::new();
    for m in resp {
        let range = &m["key"];
        let replicas = &m["value"];
        if (only_primary_range && replicas[0] == node) ||
           (!only_primary_range && replicas.contains(&node.to_string())) {
            ranges.push((range[0].parse::<i64>().ok(), range[1].parse::<i64>().ok()));
        }
    }
    ranges
}

// Take a node name which may be some alias of the node's IP address (e.g.,
// "localhost") and translate it to the node's official, internal IP address
// which is used in REST API responses listing this node.
async fn find_internal_address(node: &str) -> String {
    // TODO: Is there a less convoluted way to do this, perhaps with just
    // one REST API call?
    let hostid = reqwest::get(format!("http://{node}:10000/storage_service/hostid/local",))
        .await.unwrap()
        .json::<String>()
        .await.unwrap();
    let hostids = reqwest::get(format!("http://{node}:10000/storage_service/host_id",))
        .await.unwrap()
        .json::<Vec<HashMap<String,String>>>()
        .await.unwrap();
    for map in hostids {
        if map["value"] == hostid {
            return map["key"].clone();
        }
    }
    // Not found, just return the original name unchanged
    return node.to_string();
}

#[derive(Debug)]
struct ViewDescription {
    name: String,
    partition_key: Vec<String>
}

/// Make a list of tables in the given keyspace that have materialized views,
/// with the list of views for each.
fn find_tables_with_views(keyspace_info: &Keyspace) -> HashMap<String, Vec<ViewDescription>> {
    let mut tables_with_views = HashMap::new();
    for (view_name, view) in &keyspace_info.views {
        // TODO: view.view_metdata is a Table object with the definition of the view
        let base_table = &view.base_table_name;
        tables_with_views
            .entry(base_table.clone()).or_insert_with(Vec::new)
            .push(ViewDescription {
                name: view_name.clone(),
                partition_key: view.view_metadata.partition_key.clone()
            });
    }
    tables_with_views
}

/// Create a CQL session that connects *only* to a single node as the
/// coordinator. It still doesn't guarantee this node doesn't decide to pass
/// reads to other replicas (e.g., for HWLB reasons).
/// TODO: Figure out how to force it. See https://github.com/scylladb/scylladb/issues/9042
async fn single_node_session(node: &str, port: i16) -> Session {
    // Create a session to connect *only* the given node.
    let uri = node.to_string() + ":" + &port.to_string();
    SessionBuilder::new().known_node(&uri)
        .host_filter(Arc::new(AllowListHostFilter::new([&uri]).unwrap()))
        .build().await.unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Connect to user-specified address or port, or defaulting to
    // 127.0.0.1:9042 if no arguments.
    let mut uri = std::env::args().nth(1).unwrap_or("127.0.0.1:9042".to_string());
    if !uri.contains(':') {
        uri.push_str(":9042");
    }
    let (node, port) = uri.split_once(':').unwrap();
    let port = port.parse::<i16>().unwrap();
    let node = find_internal_address(node).await;
    println!("Chosen node is {node}. Port is {port}");

    let session = SessionBuilder::new().known_node(&uri).build().await.unwrap();

    session.refresh_metadata().await?;
    let cluster_data = &session.get_cluster_data();


    // Create a session to connect *only* the given node.
    let single_node_session = single_node_session(&node, port).await;

    // Find tables that have materialized views. We actually have the opposite
    // list - list of views in each keyspace.
    for (keyspace_name, keyspace_info) in cluster_data.get_keyspace_info() {
        let tables_with_views = find_tables_with_views(keyspace_info);

        // If there are no views in this keyspace, we can skip it entirely
        if tables_with_views.is_empty() {
            continue;
        }

        let only_primary_range = false;
        let ranges = find_token_ranges(only_primary_range, &keyspace_name, &node).await;
        println!("Found {} ranges", &ranges.len());

        for (base_table, views) in &tables_with_views {
            let base_table_info = &keyspace_info.tables[&base_table.to_string()];
            // Partition key of the base table separated by commas, e.g.,
            // "p1,p2", to be used in token() queries
            let base_table_pk = &base_table_info.partition_key.join(";");

            // Scan the base rows belonging to this node, in a given table, and for each
            // base row, check that its corresponding view row is in the view table; The
            // view row is read *only* from the paired replica.
            // TODO: make sure the read happens only from the given node and doesn' switch
            // to another node automatically. Botond once needed to read from a specific
            // replica and found the most reliable way was mutation_fragments().
            println!("Scanning base table {base_table} with views {:?}:", views);

            // Separate queries giving both ends of a range, or just one end.
            // TODO: Can we avoid the ugliness of three queries?
            // Note that Cassandra token ranges are start-exclusive and end-inclusive
            let mut prepared_both = single_node_session.prepare(
                    "SELECT * FROM ".to_string() + &keyspace_name + "." + &base_table
                    + " WHERE token(" + &base_table_pk + ") > ? AND token(" + &base_table_pk + ") <= ?")
                .await?;
            prepared_both.set_consistency(Consistency::One);
            let mut prepared_left = single_node_session.prepare(
                "SELECT * FROM ".to_string() + &keyspace_name + "." + &base_table
                + " WHERE token(" + &base_table_pk + ") <= ?")
            .await?;
            prepared_left.set_consistency(Consistency::One);
            let mut prepared_right = single_node_session.prepare(
                "SELECT * FROM ".to_string() + &keyspace_name + "." + &base_table
                + " WHERE token(" + &base_table_pk + ") > ?")
            .await?;
            prepared_right.set_consistency(Consistency::One);

            // TODO: can parallelize different ranges,
            for (start_token, end_token) in &ranges {
                println!("scanning range {:?}", (start_token, end_token));
                // The clone() is a workaround for https://github.com/scylladb/scylla-rust-driver/issues/811
                let mut rows_stream = match (start_token, end_token) {
                    (Some(s), Some(e)) => single_node_session.execute_iter(prepared_both.clone(), (s, e)).await?,
                    (Some(s), None) => single_node_session.execute_iter(prepared_right.clone(), (s,)).await?,
                    (None, Some(e)) => single_node_session.execute_iter(prepared_left.clone(), (e,)).await?,
                    _ => panic!()
                };
                while let Some(next_row) = rows_stream.next().await {
                    //.into_typed::<(i32, i32)>();
                    //let (a, b): (i32, i32) = next_row_res?;
                    //println!("a, b: {}, {}", a, b);
                    println!("{:?}", next_row);
                }
            }

            // TODO: scan view table rows
        }

       //println!("\tTables with views: {:#?}", tables_with_views);
        //println!("\tUDTs: {:#?}", keyspace_info.user_defined_types);
    }

    // TODO: maybe use https://docs.rs/scylla/latest/scylla/transport/struct.ClusterData.html for tokens

    // if let Some(rows) = session.query("SELECT a FROM ks.mv WHERE b = ?", (v,)).await?.rows {
    //     // Parse each row as a tuple containing a single i32
    //     for row in rows.into_typed::<(i32,)>() {
    //         let read_row: (i32,) = row?;
    //         println!("Read a value from row: {}", read_row.0);
    //     }
    // }

    Ok(())
}
