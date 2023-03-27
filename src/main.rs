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
        println!("Connection to SciDB failed! status code {status}");
    }

    // Run a query...
    let querystr = "aio_save(list('instances'),'/tmp/rusttest',format:'tdv')";
    let query = conn.execute_query(querystr);
    match query {
        Err(error) => println!(
            "Error code {} in executing query:\n\n{}",
            error.code, error.explanation
        ),
        Ok(qid) => {
            println!("Executed SciDB query {}.{}", qid.coordinatorid, qid.queryid)
        }
    }
}
