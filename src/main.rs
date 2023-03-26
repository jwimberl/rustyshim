use rustyshim::QueryID;

use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_void;

const MAX_VARLEN: usize = 4096;

/////////////////////////////////////
// Rust bindings for the SciDB API //
/////////////////////////////////////

/* The C++ API in SciDBApi.h includes a class representing a SciDB
 * connection, with methods
 * - prepareQuery
 * - executeQuery
 * - completeQuery
 * that take some parameters by const reference or value, and have
 * no return values but can throw exceptions.
 *
 * The public Rust bindings can have much the same interface, save
 * that instead of exceptions they will return optional optional
 * errors, consisting of an integer error code and a full error string.
 *
 * The binding mechanism will go through a base C layer defined in the
 * associated client.h and client.cpp files
 */

// SciDBConnection

#[derive(Debug)]
enum SciDBConnection {
    Open(*mut c_void), // content is non-null pointer to be disconnected at drop
    Closed(i32),       // content is error status code
}

impl SciDBConnection {
    fn new(hostname: &str, username: &str, password: &str, scidbport: i32) -> SciDBConnection {
        let mut status: i32 = 0;
        let sp = &mut status as *mut i32;
        let chostname = CString::new(hostname).unwrap();
        let cusername = CString::new(username).unwrap();
        let cpassword = CString::new(password).unwrap();
        let c_conn = unsafe {
            rustyshim::c_scidb_connect(
                chostname.as_ptr(),
                scidbport,
                cusername.as_ptr(),
                cpassword.as_ptr(),
                0,
                sp,
            )
        };
        if status == 0 && c_conn != 0 as *mut c_void {
            return SciDBConnection::Open(c_conn);
        } else {
            return SciDBConnection::Closed(status);
        }
    }
}

impl Drop for SciDBConnection {
    fn drop(&mut self) {
        match self {
            SciDBConnection::Open(ptr) => unsafe {
                rustyshim::c_scidb_disconnect(ptr.clone());
            },
            SciDBConnection::Closed(_) => (),
        }
    }
}

// QueryResult

struct QueryResult {
    ptr: *mut c_void, // content is non-null pointer to C++ object to be deleted at Drop
}

impl QueryResult {
    fn new() -> QueryResult {
        let qr = unsafe { rustyshim::c_init_query_result() };
        assert!(qr != 0 as *mut c_void); // not attempting to recover from memory allocation errors
        QueryResult { ptr: qr }
    }

    fn id(&self) -> QueryID {
        unsafe { rustyshim::c_query_result_to_id(self.ptr.clone()) }
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe {
            rustyshim::c_free_query_result(self.ptr.clone());
        }
    }
}

// Query exection methods on SciDBConnection

struct QueryError {
    code: i32,
    explanation: String,
}

enum QueryExecutionResult {
    Success(QueryID),
    Failure(QueryError),
}

impl SciDBConnection {
    // Preparation step
    fn prepare_query(&mut self, query: &str, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let cquery = CString::new(query).unwrap();
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code = unsafe {
                    rustyshim::c_prepare_query(
                        c_conn.clone(),
                        cquery.as_ptr(),
                        result.ptr,
                        errbufptr,
                    )
                };
                let error = unsafe { CStr::from_ptr(errbufptr) };
                let error: String = String::from_utf8_lossy(error.to_bytes()).to_string();
                if code == 0 && error.is_empty() {
                    None
                } else {
                    Some(QueryError {
                        code: code,
                        explanation: error,
                    })
                }
            }
            SciDBConnection::Closed(_) => Some(QueryError {
                code: rustyshim::SHIM_NO_SCIDB_CONNECTION,
                explanation: String::from("SciDB connection not open"),
            }),
        }
    }

    // Post-preparation execution
    fn execute_prepared_query(&mut self, query: &str, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let cquery = CString::new(query).unwrap();
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code = unsafe {
                    rustyshim::c_execute_prepared_query(
                        c_conn.clone(),
                        cquery.as_ptr(),
                        result.ptr,
                        errbufptr,
                    )
                };
                let error = unsafe { CStr::from_ptr(errbufptr) };
                let error: String = String::from_utf8_lossy(error.to_bytes()).to_string();
                if code == 0 && error.is_empty() {
                    None
                } else {
                    Some(QueryError {
                        code: code,
                        explanation: error,
                    })
                }
            }
            SciDBConnection::Closed(_) => Some(QueryError {
                code: rustyshim::SHIM_NO_SCIDB_CONNECTION,
                explanation: String::from("SciDB connection not open"),
            }),
        }
    }

    // Completion
    fn complete_query(&mut self, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code =
                    unsafe { rustyshim::c_complete_query(c_conn.clone(), result.ptr, errbufptr) };
                let error = unsafe { CStr::from_ptr(errbufptr) };
                let error: String = String::from_utf8_lossy(error.to_bytes()).to_string();
                if code == 0 && error.is_empty() {
                    None
                } else {
                    Some(QueryError {
                        code: code,
                        explanation: error,
                    })
                }
            }
            SciDBConnection::Closed(_) => Some(QueryError {
                code: rustyshim::SHIM_NO_SCIDB_CONNECTION,
                explanation: String::from("SciDB connection not open"),
            }),
        }
    }

    // All-in-one method
    fn execute_query(&mut self, query: &str) -> QueryExecutionResult {
        let mut qr = QueryResult::new();

        // Prep
        let error = self.prepare_query(&query, &mut qr);
        if let Some(error) = error {
            return QueryExecutionResult::Failure(error);
        }

        // Execute
        let error = self.execute_prepared_query(&query, &mut qr);
        if let Some(error) = error {
            return QueryExecutionResult::Failure(error);
        }

        // Complete
        let error = self.complete_query(&mut qr);
        if let Some(error) = error {
            return QueryExecutionResult::Failure(error);
        }

        QueryExecutionResult::Success(qr.id())
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
        QueryExecutionResult::Failure(error) => println!(
            "Error code {} in executing query:\n\n{}",
            error.code, error.explanation
        ),
        QueryExecutionResult::Success(qid) => {
            println!("Executed SciDB query {}.{}", qid.coordinatorid, qid.queryid)
        }
    }
}
