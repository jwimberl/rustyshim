use std::env;
use std::io;
use std::io::Write;
use std::time::{Instant};
use rustyshim::SciDBConnection;
use datafusion::prelude::*;
use tokio; // 0.3.5
use serde_yaml;
use serde::{Serialize, Deserialize};

// Configuration format //

#[derive(Serialize, Deserialize, Debug)]
struct SciDBArray {
    name: String,
    afl: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ShimConfig {
    arrays: Vec<SciDBArray>
}

use clap::Parser;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Args {
    /// The path to the YAML config file to read
    config: std::path::PathBuf,
}

// Main function //

#[tokio::main]
async fn main() {
    // Parse CLI arguments and read YAML config
    let args = Args::parse();
    let conff = std::fs::File::open(&args.config).unwrap();
    let config: ShimConfig = serde_yaml::from_reader(conff).unwrap();

    // SciDB connection config...
    let hostname = match env::var("SCIDB_HOST") { Err(_) => String::from("localhost"), Ok(host) => host };
    let username = match env::var("SCIDB_USER") { Err(_) => String::from("scidbadmin"), Ok(user) => user };
    let password = match env::var("SCIDB_PASSWORD") { Err(_) => String::from(""), Ok(passwd) => passwd };
    let scidbport = match env::var("SCIDB_PORT") {Err(_) => 1239, Ok(port) => port.parse::<i32>().unwrap() };

    // Connect to SciDB...
    let mut conn = SciDBConnection::new(&hostname, &username, &password, scidbport);
    if let SciDBConnection::Closed(status) = conn {
        panic!("Connection to SciDB failed! status code {status}");
    }

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Run queries and register as DataFusion tables
    for arr in config.arrays {
        let res = conn.execute_aio_query(&arr.afl);
        match res {
            Err(error) => println!(
                "Error code {} in executing query:\n\n{}",
                error.code, error.explanation
            ),
            Ok(aio) => {
                println!(
                    "Executed SciDB query {}.{}",
                    aio.qid.coordinatorid, aio.qid.queryid
                );
                // at this point data is still on-disk in buffer file
                let data = aio.to_batches().unwrap(); // consumes buffer file, data lives in memory
                // todo: should check that array length is > 0
                let record_batch = arrow::compute::concat_batches(&data[0].schema(), &data).unwrap();
                let reg = ctx.register_batch(&arr.name, record_batch);
                if let Err(error) = reg {
                    println!(
                        "Error while loading SciDB query results into DataFusion:\n\n{}",
                        error
                    )
                }
            }
        }
    }

    // Loop and run dataFusion queries...
    loop {
        print!(">> ");
        let _ = io::stdout().flush();

        // Get user query from STDIN
        // Example: "SELECT ex1.i, ex1.j, ex2.i, ex2.j FROM ex1 INNER JOIN ex2 ON ex1.value = ex2.value"
        let mut query = String::new();
        io::stdin()
            .read_line(&mut query)
            .expect("Failed to read line");

        // Run and time the query
        let start = Instant::now();
        let results = ctx.sql(&query).await;
        let duration = start.elapsed();

        // Print results
        match results {
            Err(error) => println!(
                "Error while running SQL via DataFusion:\n\n{}",
                error
            ),
            Ok(df) => {
                df.show().await.unwrap();
            }
        }
        println!("Query time is: {:?}", duration);

    }
}
