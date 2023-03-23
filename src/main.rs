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

impl SciDBConnection {
    fn execute_query(&self, query : &str) -> (u64, String) {
        match self {
            SciDBConnection::Open(c_conn) => execute_query(c_conn.clone(), query),
            SciDBConnection::Closed(_) => (0,String::from(""))
        }
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
    let conn = scidb_connect(hostname, username, password, scidbport);
    dbg!(&conn);
    if let SciDBConnection::Closed(status) = conn {
        println!("Connection to SciDB failed! status code {status}");
        std::process::exit(1);
    }

    // Run a query...
    let query = "aio_save(list(\'instances\'),\'/tmp/rusttest\',format:\'tdv\');";
    let (qid, error) = conn.execute_query(query);
    println!("Executing SciDB query {qid} -- error string {error}");
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

fn execute_query(conn : *mut c_void, query : &str) -> (u64, String) {
    let cquery = CString::new(query).unwrap();
    let mut output_buffer : [c_char; MAX_VARLEN] = [0; MAX_VARLEN];
    let (qid, error) = unsafe {
        let qid = c_executeQuery(conn,cquery.as_ptr() as *mut c_char,0,output_buffer.as_mut_ptr());
        let error = CStr::from_ptr(output_buffer.as_mut_ptr());
        (qid, error)
    };
    let error : String = String::from_utf8_lossy(error.to_bytes()).to_string();
    (qid, error)
}
