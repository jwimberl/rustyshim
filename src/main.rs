use rustyshim::scidbconnect as c_scidbconnect;
use rustyshim::executeQuery as c_executeQuery;

use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;
use std::os::raw::c_void;

const MAX_VARLEN : usize = 4096;

#[derive(Debug)]
enum SciDBConnection {
    Open(*mut c_void), // content is non-null C void*
    Closed(i32)        // content is error status code
}

enum SciDBQuery {
    Success(u64),      // content is query ID
    Error(String)      // content is error string
}

impl SciDBConnection {

    fn new(hostname : &str, username : &str, password : &str, scidbport : i32) -> SciDBConnection {
        scidb_connect(hostname, username, password, scidbport)
    }

    fn execute_query(&mut self, query : &str) -> SciDBQuery {
        execute_query(self, query)
    }
}

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
        SciDBQuery::Error(error) => println!("Error in executing query:\n\n{error}"),
        SciDBQuery::Success(qid) => println!("Excecuting SciDB query {qid}"),
    }
}

fn scidb_connect(hostname : &str, username : &str, password : &str, scidbport : i32) -> SciDBConnection {
    let mut status : i32 = 0;
    let sp = &mut status as *mut i32;
    let chostname = CString::new(hostname).unwrap();
    let cusername = CString::new(username).unwrap();
    let cpassword = CString::new(password).unwrap();
    let c_conn = unsafe {
        c_scidbconnect(chostname.as_ptr(),scidbport,cusername.as_ptr(),cpassword.as_ptr(),0,sp)
    };
    if status == 0 && c_conn != 0 as *mut c_void {
        return SciDBConnection::Open(c_conn);
    } else {
        return SciDBConnection::Closed(status);
    }
}

fn execute_query(conn : &mut SciDBConnection, query : &str) -> SciDBQuery {
    match conn {
        SciDBConnection::Open(c_conn) => {
            let cquery = CString::new(query).unwrap();
            let mut output_buffer : [c_char; MAX_VARLEN] = [0; MAX_VARLEN];
            let (qid, error) = unsafe {
                let qid = c_executeQuery(c_conn.clone(),cquery.as_ptr() as *mut c_char,1,output_buffer.as_mut_ptr());
                let error = CStr::from_ptr(output_buffer.as_mut_ptr());
                (qid, error)
            };
            let error : String = String::from_utf8_lossy(error.to_bytes()).to_string();
            if !error.is_empty() || qid == 0 {
                SciDBQuery::Error(error)
            } else {
                SciDBQuery::Success(qid)
            }
        },
        SciDBConnection::Closed(_) => {
            SciDBQuery::Error(String::from("SciDB connection not open"))
        }
    }
}
