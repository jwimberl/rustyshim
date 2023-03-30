use rustyshim::SciDBConnection;

use arrow::util::pretty::print_batches;

fn main() {
    println!("Hello, world!");

    // Config...
    let hostname = "localhost";
    let username = "scidbadmin";
    let password = ""; // does not matter in trust mode
    let scidbport = 1239;

    // Connect...
    let mut conn = SciDBConnection::new(hostname, username, password, scidbport);
    if let SciDBConnection::Closed(status) = conn {
        panic!("Connection to SciDB failed! status code {status}");
    }

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
            for batch in data {
                print_batches(&[batch.unwrap()]).unwrap();
            }
        }
    }
}
