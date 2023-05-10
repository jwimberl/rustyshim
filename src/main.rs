use std::env;
use rustyshim::SciDBConnection;
use datafusion::prelude::*;
use tokio; // 0.3.5
use serde_yaml;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct SciDBArray {
    name: String,
    afl: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ShimConfig {
    arrays: Vec<SciDBArray>
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    // Config...
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
    let example_yaml = "
      arrays:
        - name: ex1
          afl: apply(build(<value:int64> [i=0:10:0:10;j=0:10:0:10],i*j),i,i,j,j)
        - name: ex2
          afl: apply(build(<value:int64> [i=0:10:0:10;j=0:10:0:10],i+j),i,i,j,j)
    ";
    let config: ShimConfig = serde_yaml::from_str(example_yaml).unwrap();

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

    // Run a DataFusion query...
    let results = ctx.sql("SELECT ex1.i, ex1.j, ex2.i, ex2.j FROM ex1 INNER JOIN ex2 ON ex1.value = ex2.value").await;
    match results {
        Err(error) => println!(
            "Error while running SQL via DataFusion:\n\n{}",
            error
        ),
        Ok(df) => {
            df.show().await.unwrap();
        }
    }
}
