use rustyshim::scidbconnect as c_scidbconnect;
use rustyshim::executeQuery as c_executeQuery;

use std::os::raw::c_char;
use std::os::raw::c_void;

fn main() {
    println!("Hello, world!");

    // Config...
    let hostname = "localhost";
    let username = "scidbadmin";
    let password = ""; // does not matter in trust mode
    let scidbport = 1239;

    // Connect...
    let mut status : i32 = 0;
    let conn = scidbconnect(hostname, username, password, scidbport, &mut status);
    dbg!(conn);
    dbg!(status);
    if status != 0 {
        println!("Connection to SciDB failed! status code {status}");
        std::process::exit(1);
    }

    // Run a query...
    let query = "aio_save(list(\'instances\'),\'/tmp/rusttest\',format:\'tdv\');";
    let (qid, error) = executeQuery(conn, query);
    println!("Executing SciDB query {qid} -- error string {error}");
}

fn scidbconnect(hostname : &str, username : &str, password : &str, scidbport : i32, status : &mut i32) -> *mut c_void {
    let sp = status as *mut i32;
    let mut h = String::from(hostname);
    h.push('\0');
    let mut u = String::from(username);
    u.push('\0');
    let mut p = String::from(password);
    p.push('\0');
    unsafe {
        let chostname = (&h[..]).as_bytes().as_ptr() as *mut c_char;
        let cusername = (&u[..]).as_bytes().as_ptr() as *mut c_char;
        let cpassword = (&p[..]).as_bytes().as_ptr() as *mut c_char;
        c_scidbconnect(chostname,scidbport,cusername,cpassword,0,sp)
    }
}

fn executeQuery(conn : *mut c_void, query : &str) -> (u64, String) {
    let mut q = String::from(query);
    q.push('\0');
    let mut output : [c_char; 1024] = [0; 1024];
    let (qid, error) = unsafe {
        let cquery = (&q[..]).as_bytes().as_ptr() as *mut c_char;
        let output_ptr = &mut output[0] as *mut c_char;
        let qid = c_executeQuery(conn,cquery,0,output_ptr);
        let error = std::ffi::CStr::from_ptr(&mut output[0] as *mut c_char);
        (qid, error)
    };
    let error : String = String::from_utf8_lossy(error.to_bytes()).to_string();
    (qid, error)
}
