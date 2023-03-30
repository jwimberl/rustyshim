include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_void;

use arrow::error::ArrowError;
use arrow::ipc;
use arrow::record_batch::RecordBatch;

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

/////////////////////
// SciDBConnection //
/////////////////////

pub struct SciDBConnectionPtr {
    c_ptr: *mut c_void,
}

pub enum SciDBConnection {
    Open(SciDBConnectionPtr), // content is non-null pointer to be disconnected at drop
    Closed(i32),              // content is error status code
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
            return SciDBConnection::Open(SciDBConnectionPtr { c_ptr: c_conn });
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

/////////////////
// QueryResult //
/////////////////

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

///////////////////////////////////////////////
// Query exection methods on SciDBConnection //
///////////////////////////////////////////////

#[derive(Debug)]
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
                    c_prepare_query(c_conn.c_ptr.clone(), cquery.as_ptr(), result.ptr, errbufptr)
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
                explanation: "SciDB connection not open".to_owned(),
            }),
        }
    }

    // Post-preparation execution
    pub fn execute_prepared_query(
        &mut self,
        query: &str,
        result: &QueryResult,
    ) -> Option<QueryError> {
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
                explanation: "SciDB connection not open".to_owned(),
            }),
        }
    }

    // Completion
    pub fn complete_query(&mut self, result: &QueryResult) -> Option<QueryError> {
        match self {
            SciDBConnection::Open(c_conn) => {
                let mut errbuf = vec![0; MAX_VARLEN];
                let errbufptr = errbuf.as_mut_ptr() as *mut i8;
                let code = unsafe { c_complete_query(c_conn.c_ptr.clone(), result.ptr, errbufptr) };
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
                explanation: "SciDB connection not open".to_owned(),
            }),
        }
    }

    // All-in-one method
    pub fn execute_query(&mut self, query: &str) -> Result<QueryID, QueryError> {
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

////////////////
// AioQuery //
////////////////

/* Wrap a generic query with AIO to save its output
 * to a temporary file in arrow format;
 */

impl From<std::io::Error> for QueryError {
    fn from(e: std::io::Error) -> Self {
        Self {
            code: SHIM_IO_ERROR,
            explanation: e.to_string(),
        }
    }
}

impl From<ArrowError> for QueryError {
    fn from(e: ArrowError) -> Self {
        Self {
            code: SHIM_ARROW_ERROR,
            explanation: e.to_string(),
        }
    }
}

pub struct AioQuery {
    pub qid: QueryID,
    buffer_path: tempfile::TempPath,
}

impl AioQuery {
    pub fn new() -> Result<AioQuery, QueryError> {
        let buffer = tempfile::NamedTempFile::new()?;
        let path = buffer.into_temp_path(); // consumes and closes buffer();
        return Ok(AioQuery {
            qid: QueryID {
                queryid: 0,
                coordinatorid: 0,
            },
            buffer_path: path,
        });
    }

    pub fn query_str(&self, query: &str) -> Option<String> {
        let pathstr = self.buffer_path.to_str()?;
        let mut aio_query = "aio_save(".to_owned();
        aio_query.push_str(query);
        aio_query.push_str(", '");
        aio_query.push_str(pathstr);
        aio_query.push_str("', format:'arrow')");
        Some(aio_query)
    }

    pub fn to_batches(self) -> Result<Vec<RecordBatch>, QueryError> {
        self.into()
    }
}

impl Into<Result<Vec<RecordBatch>, QueryError>> for AioQuery {
    fn into(self) -> Result<Vec<RecordBatch>, QueryError> {
        let pathstr = self.buffer_path.to_str().ok_or(QueryError {
            code: SHIM_IO_ERROR,
            explanation: "cannot convert path to string".to_owned(),
        })?;
        let file = std::fs::File::open(&pathstr)?;
        let ipc_reader = ipc::reader::StreamReader::try_new(file, None)?;
        let batches: Vec<_> = ipc_reader.collect();
        let mut filtered_batches: Vec<RecordBatch> = vec![];
        for batch in batches {
            let goodbatch = batch?;
            filtered_batches.push(goodbatch);
        }

        Ok(filtered_batches)
    }
}

impl SciDBConnection {
    pub fn execute_aio_query(&mut self, query: &str) -> Result<AioQuery, QueryError> {
        // Create AioQuery buffer and get path
        let mut aio = AioQuery::new()?;

        // Wrap AFL to save it to the buffer file in arrow format
        let aio_query = aio.query_str(query).ok_or(QueryError {
            code: SHIM_IO_ERROR,
            explanation: "cannot convert path to string".to_owned(),
        })?;

        // Execute the SciDB query, saving data to the buffer file
        aio.qid = self.execute_query(&aio_query)?;

        // Return QueryID result
        Ok(aio)
    }
}
