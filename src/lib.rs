include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

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

pub struct SciDBConnectionPtr {
    c_ptr : *mut c_void
}

pub enum SciDBConnection {
    Open(SciDBConnectionPtr), // content is non-null pointer to be disconnected at drop
    Closed(i32),       // content is error status code
}

impl SciDBConnection {
    pub fn new(hostname: &str, username: &str, password: &str, scidbport: i32) -> SciDBConnection {
        let mut status: i32 = 0;
        let sp = &mut status as *mut i32;
        let chostname = CString::new(hostname).unwrap();
        let cusername = CString::new(username).unwrap();
        let cpassword = CString::new(password).unwrap();
        let c_conn = unsafe {
            c_scidb_connect(
                chostname.as_ptr(),
                scidbport,
                cusername.as_ptr(),
                cpassword.as_ptr(),
                0,
                sp,
            )
        };
        if status == 0 && c_conn != 0 as *mut c_void {
            return SciDBConnection::Open(SciDBConnectionPtr{c_ptr : c_conn});
        } else {
            return SciDBConnection::Closed(status);
        }
    }
}

impl Drop for SciDBConnection {
    fn drop(&mut self) {
        match self {
            SciDBConnection::Open(ptr) => unsafe {
                c_scidb_disconnect(ptr.c_ptr.clone());
            },
            SciDBConnection::Closed(_) => (),
        }
    }
}

// QueryResult

pub struct QueryResult {
    ptr: *mut c_void, // content is non-null pointer to C++ object to be deleted at Drop
}

impl QueryResult {
    fn new() -> QueryResult {
        let qr = unsafe { c_init_query_result() };
        assert!(qr != 0 as *mut c_void); // not attempting to recover from memory allocation errors
        QueryResult { ptr: qr }
    }

    fn id(&self) -> QueryID {
        unsafe { c_query_result_to_id(self.ptr.clone()) }
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe {
            c_free_query_result(self.ptr.clone());
        }
    }
}

// Query exection methods on SciDBConnection

pub struct QueryError {
    pub code: i32,
    pub explanation: String,
}

impl SciDBConnection {
    // Preparation step
    pub fn prepare_query(&mut self, query: &str, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let cquery = CString::new(query).unwrap();
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code = unsafe {
                    c_prepare_query(
                        c_conn.c_ptr.clone(),
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
                code: SHIM_NO_SCIDB_CONNECTION,
                explanation: String::from("SciDB connection not open"),
            }),
        }
    }

    // Post-preparation execution
    pub fn execute_prepared_query(&mut self, query: &str, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let cquery = CString::new(query).unwrap();
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code = unsafe {
                    c_execute_prepared_query(
                        c_conn.c_ptr.clone(),
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
                code: SHIM_NO_SCIDB_CONNECTION,
                explanation: String::from("SciDB connection not open"),
            }),
        }
    }

    // Completion
    pub fn complete_query(&mut self, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code =
                    unsafe { c_complete_query(c_conn.c_ptr.clone(), result.ptr, errbufptr) };
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
                code: SHIM_NO_SCIDB_CONNECTION,
                explanation: String::from("SciDB connection not open"),
            }),
        }
    }

    // All-in-one method
    pub fn execute_query(&mut self, query: &str) -> Result<QueryID,QueryError> {
        let mut qr = QueryResult::new();

        // Prep
        let error = self.prepare_query(&query, &mut qr);
        if let Some(error) = error {
            return Err(error);
        }

        // Execute
        let error = self.execute_prepared_query(&query, &mut qr);
        if let Some(error) = error {
            return Err(error);
        }

        // Complete
        let error = self.complete_query(&mut qr);
        if let Some(error) = error {
            return Err(error);
        }

        Ok(qr.id())
    }
}
