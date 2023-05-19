use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use datafusion::prelude::*;
use rustyshim::flight::{FusionFlightAuthenticator, FusionFlightService};
use rustyshim::scidb::SciDBConnection;
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::io::Write;
use std::time::Instant;
use tokio; // 0.3.5
use tonic::transport::Server;

//////////////////////////
// Configuration format //
//////////////////////////

// Configuration file format
#[derive(Serialize, Deserialize, Debug)]
struct SciDBArray {
    name: String,
    afl: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ShimConfig {
    arrays: Vec<SciDBArray>,
}

// Command line arguments

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The SciDB hostname
    #[arg(long, default_value = "localhost")]
    hostname: String,

    /// The SciDB port
    #[arg(long, default_value_t = 1239)]
    port: i32,

    /// The SciDB admin username
    #[arg(short, long)]
    username: Option<String>,

    /// The SciDB admin password
    #[arg(short, long)]
    password: Option<String>,

    /// Flag to read the SciDB admin password from TTY
    #[arg(long, action)]
    password_stdin: bool,

    /// The path to the YAML config file to read
    #[arg(short, long)]
    config: std::path::PathBuf,
}

// Authenticator class //
#[derive(Clone)]
struct SciDBAuthenticator {
    hostname: String,
    port: i32,
}

#[tonic::async_trait]
impl FusionFlightAuthenticator for SciDBAuthenticator {
    fn authenticate(&self, username: &String, password: &String) -> bool {
        let conn = SciDBConnection::new(&self.hostname, username, password, self.port);
        match conn {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

// Main function //

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI arguments and read YAML config
    let args = Args::parse();
    // Logic:
    // -- must not have both --password or --password_stdin argument
    // -- if have either, must not have username
    if args.password.is_some() && args.password_stdin {
        panic!("You may not choose both the --password and --password_stdin argument");
    }
    if !args.username.is_some() && (args.password.is_some() || args.password_stdin) {
        panic!("You may not supply a password via the arugments without a username");
    }
    // Parse/prompt for needed credentials
    let username = match args.username {
        Some(provided) => provided,
        None => {
            let mut prompted = String::new();
            print!("SciDB username: ");
            let _ = std::io::stdout().flush();
            std::io::stdin()
                .read_line(&mut prompted)
                .expect("Invalid username");
            prompted.trim().to_string()
        }
    };
    let password = if args.password_stdin {
        let mut prompted = String::new();
        std::io::stdin()
            .read_line(&mut prompted)
            .expect("Invalid password via stdin");
        prompted.trim().to_string()
    } else {
        match args.password {
            Some(provided) => provided,
            None => rpassword::prompt_password("SciDB password: ")?,
        }
    };

    let conff = std::fs::File::open(&args.config)?;
    let config: ShimConfig = serde_yaml::from_reader(conff)?;

    // Connect to SciDB...
    let mut conn = SciDBConnection::new(&args.hostname, &username, &password, args.port)?;

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Run queries and register as DataFusion tables
    let db_start = Instant::now();
    for arr in config.arrays {
        let q_start = Instant::now();
        let aio = conn.execute_aio_query(&arr.afl)?;
        println!(
            "Executed SciDB query {}.{}",
            aio.qid.coordinatorid, aio.qid.queryid
        );
        let q_duration = q_start.elapsed();
        println!("Elapsed SciDB query duration: {:?}", q_duration);
        // at this point data is still on-disk in buffer file
        let data = aio.to_batches()?; // consumes buffer file, data lives in memory
                                      // todo: should check that array length is > 0
        let record_batch = datafusion::arrow::compute::concat_batches(&data[0].schema(), &data)?;
        let reg = ctx.register_batch(&arr.name, record_batch);
        if let Err(error) = reg {
            println!(
                "Error while loading SciDB query results into DataFusion:\n\n{}",
                error
            )
        }
    }
    let db_duration = db_start.elapsed();
    println!("Elapsed database construction duration: {:?}", db_duration);

    // Create SciDBAuthenticator //
    let auth = SciDBAuthenticator {
        hostname: args.hostname,
        port: args.port,
    };

    // Launch Flight server //

    let addr = "127.0.0.1:50051".parse()?;
    let service = FusionFlightService::new(ctx, Box::new(auth)).await;
    let svc = FlightServiceServer::new(service);
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
