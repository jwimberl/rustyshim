use rustyshim::SciDBConnection;

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
        Ok(qid) => {
            println!("Executed SciDB query {}.{}", qid.coordinatorid, qid.queryid)
        }
    }
}
