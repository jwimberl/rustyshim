use rustyshim::SciDBConnection;
use datafusion::prelude::*;
use tokio; // 0.3.5

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    // Config...
    let hostname = "localhost";
    let username = "scidbadmin";
    let password = ""; // does not matter in trust mode
    let scidbport = 1239;

    // Connect to SciDB...
    let mut conn = SciDBConnection::new(hostname, username, password, scidbport);
    if let SciDBConnection::Closed(status) = conn {
        panic!("Connection to SciDB failed! status code {status}");
    }

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Run a query...
    let query = "apply(build(<value:int64> [i=0:10:0:10;j=0:10:0:10],i*j),i,i,j,j)";
    let res = conn.execute_aio_query(&query);
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
            let record_batch = arrow::compute::concat_batches(&data[0].schema(), &data).unwrap();
            let reg = ctx.register_batch("example", record_batch);
            match reg {
                Err(error) => println!(
                    "Error while loading batch into DataFusion:\n\n{}",
                    error
                ),
                _ => ()
            }
        }
    }

    // Run a DataFusion query...
    let results = ctx.sql("SELECT value, MIN(i), MIN(j) FROM example GROUP BY value LIMIT 100").await;
    match results {
        Err(error) => println!(
            "Error while running SQL via DataFusion:\n\n{}",
            error
        ),
        Ok(df) => {
            df.show().await;
        }
    }
}
