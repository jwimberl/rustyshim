# rustyshim

## Overview

`rustyshim` is a utility based on the Apache Arrow, DataFusion, and Flight projects, which allows SQL queries
of a database caching the results AFL queries to a SciDB backend.

## Compilation

This package is currently only tested on the RedHat-based CentOS 7 and Rocky 8 distributions. `rustyshim` has several system dependencies:
* SciDB and the SciDB client headers and library
* the `rust` ecosystem, most easily installed via [https://rustup.rs/](https://rustup.rs/)
* at minimum `llvm` and `llvm-devel` are required for compiling Rust code; other packages may need to be installed as well
* GCC 8.5.0+ or an equivalent C++ compiler is required to compile the C/C++ wrapper around `scidb::SciDBClient`
for which Rust bindings are generated; on CentOS 7 this may installed and activated using `devtoolset-8`

With these system dependencies met, `rustyshim` and its Rust crate dependencies module can be compiled using `cargo build`.
Two environment variables must be set to point the build script to the SciDB and PostgreSQL headers and libraries;
assumed to reside in default version-specific directories. For example, to compile against SciDB 22.5:
```
SCIDB_VER=22.5 POSTGRES_VER=9.3 cargo build
```

## Usage

### Server

#### Options

`rustyshim` has several CLI options for specifying the SciDB admin credentials with which it will connect to the SciDB
instance on the same machine:
```
$ cargo run -- -h
    Finished dev [unoptimized + debuginfo] target(s) in 0.22s
warning: the following packages contain code that will be rejected by a future version of Rust: nom v5.1.2
note: to see what the problems were, use the option `--future-incompat-report`, or run `cargo report future-incompatibilities --id 1`
     Running `target/debug/rustyshim -h`
Usage: rustyshim [OPTIONS] --config <CONFIG>

Options:
      --hostname <HOSTNAME>  The SciDB hostname [default: localhost]
      --port <PORT>          The SciDB port [default: 1239]
  -u, --username <USERNAME>  The SciDB admin username
  -p, --password <PASSWORD>  The SciDB admin password
      --password-stdin       Flag to read the SciDB admin password from TTY
  -c, --config <CONFIG>      The path to the YAML config file to read
  -h, --help                 Print help
  -V, --version              Print version
```

Neither the username nor password is required in interactive usage:
```
$ cargo run -- -c examples/config.yaml 
    Finished dev [unoptimized + debuginfo] target(s) in 0.23s
warning: the following packages contain code that will be rejected by a future version of Rust: nom v5.1.2
note: to see what the problems were, use the option `--future-incompat-report`, or run `cargo report future-incompatibilities --id 1`
     Running `target/debug/rustyshim -c examples/config.yaml`
SciDB username: scidbadmin
SciDB password: 
...
```

For service usage, it is more secure to send the password to the server via STDIN
so that it does not appear in cleartext in shell history or process tables:
```
SCIDB_VER=22.5 POSTGRES_VER=9.3 cargo build --release
cat passfile | LD_LIBRARY_PATH=/opt/scidb/22.5/lib/ ./target/release/rustyshim -u scidbadmin --password-stdin
 -c examples/config.yaml
```
*Note*: `cargo run` is configured to load the SciDB library path by default, based on the SciDB version
specified during building, but when running a release build manually this must be added to the library lookup
path.

#### YAML config file

The configuration file for `rustyshim` is a YAML file presenting a list of table names
and the AFL with which to generate them. The [./examples](./examples) directory contains one
trivial example defining two tables `ex1` and `ex2`, containing a value column equal to the
product or sum, respectively, of dimension columns ranging from 0 to 10:
```
arrays:
  - name: ex1
    afl: apply(build(<value:int64> [i=0:10:0:10;j=0:10:0:10],i*j),i,i,j,j)
  - name: ex2
    afl: apply(build(<value:int64> [i=0:10:0:10;j=0:10:0:10],i+j),i,i,j,j)
```
This configuration file is processed when the server is started, but is also re-read and
applied whenever an adminitrator invokes the `REFRESH_CONTEXT` action.

*Note*: to include an array's dimensions in the generated table, they must be added
as array attributes via the `apply(...)` operator, as shown above.

### Python client

An example, functional Python client is provided at [./examples/rustyshim_client.py](./examples/rustyshim_client.py).
This client depends on `pyarrow` and associated Python libraries. The python module provides a method
`rustyshim_connect` with parameters
* `host`: `rustyshim` server hostname 
* `username`: SciDB username
* `password`: SciDB password
* `request_admin`: whether to ask for admin privileges; False by default
* `port`: port on hostname where `rustyshim` is listening; 50551 by default

The object returned by `rustyshim_connect` has several methods:
* `get_sql("SELECT ...")` runs the given SQL query and returns a stream of Flight data, which can be read into a pyarrow table or pandas DataFrame
* `list_actions()`: [**admin only**] lists the available administrator actions, which are `REFRESH_CONTEXT` and `CLEAR_EXPIRED_ITEMS`
* `refresh_context()`: [**admin only**] rereads the configuration file and regenerates tables via SciDB queries 
* `clear_expired_items()`: [**admin only**] clears client session tokens and tickets more than 24 hours old

Example usage:
```
>>> import rustyshim_client as rc
>>> import getpass
>>> db = rc.rustyshim_connect("localhost","scidbadmin",getpass.getpass(),request_admin=True)
Password: 
>>> db.list_actions()
[ActionType(type='REFRESH_CONTEXT', description='Re-generate the tables by querying SciDB'), ActionType(type='CLEAR_EXPIRED_ITEMS', description='Clear all sessions and tokens greater than 86400s secs old')]
>>> db.refresh_context()
['SUCCESS']
>>> db.clear_expired_items()
['SUCCESS', 'REMOVED 0 EXPIRED SESSION TOKENS', 'REMOVED 0 EXPIRED TICKETS']
>>> db.get_sql("SELECT ex1.i AS i1, ex1.j AS j1, ex2.i AS i2, ex2.j AS j2 FROM ex1 INNER JOIN ex2 ON ex1.value = ex2.value").read_pandas()
     i1  j1  i2  j2
0     2   7  10   4
1     7   2  10   4
2     2   7   4  10
3     7   2   4  10
4     1   6   0   6
..   ..  ..  ..  ..
306   6   2   8   4
307   2   6   9   3
308   3   4   9   3
309   4   3   9   3
310   6   2   9   3

[311 rows x 4 columns]
```

### R client

An example, functional R client is provided at [./examples/rustyshim_client.R](./examples/rustyshim_client.R).
This client depends on `pyarrow` and associated Python libraries, as well as the R `reticulate` and `arrow` packages.
This R file provides a method `rustyshim_connect` with identical parameters to the Python method of the same name,
returning an R6 object with equivalent methods:
* `get_sql("SELECT ...")` runs the given SQL query and returns an Arrow table
* `list_actions()`: [**admin only**] lists the available administrator actions, which are `REFRESH_CONTEXT` and `CLEAR_EXPIRED_ITEMS`
* `refresh_context()`: [**admin only**] rereads the configuration file and regenerates tables via SciDB queries 
* `clear_expired_items()`: [**admin only**] clears client session tokens and tickets more than 24 hours old

Example usage:
```
> reticulate::use_condaenv("...") # ensure the use of a python environment including pyarrow, as necessary
> source("rustyshim_client.R")
> db <- rustyshim_connect("localhost","scidbadmin",askpass::askpass(),request_admin=TRUE)
Please enter your password:  
🔑  OK
> db$get_sql("SELECT ex1.i AS i1, ex1.j AS j1, ex2.i AS i2, ex2.j AS j2 FROM ex1 INNER JOIN ex2 ON ex1.value = ex2.value")
pyarrow.Table
i1: int64
j1: int64
i2: int64
j2: int64
----
i1: [[2,4,8,2,4,...,4,8,2,4,8],[2,3,6,9,2,...,4,2,3,6,9],...,[1,1],[2,3,4,6,2,...,6,2,3,4,6]]
j1: [[8,4,2,8,4,...,4,2,8,4,2],[9,6,3,2,9,...,1,9,6,3,2],...,[1,1],[6,4,3,2,6,...,2,6,4,3,2]]
i2: [[10,10,10,6,6,...,8,8,9,9,9],[10,10,10,10,8,...,4,9,9,9,9],...,[0,1],[10,10,10,10,2,...,8,9,9,9,9]]
j2: [[6,6,6,10,10,...,8,8,7,7,7],[8,8,8,8,10,...,0,9,9,9,9],...,[1,0],[2,2,2,2,10,...,4,3,3,3,3]]
> # Binding this pyarrow.Table as a native R object requires loading the R arrow package
> require("arrow")
> require("magrittr")
> require("tibble")
> db$get_sql("SELECT ex1.i AS i1, ex1.j AS j1, ex2.i AS i2, ex2.j AS j2 FROM ex1 INNER JOIN ex2 ON ex1.value = ex2.value")
Table
311 rows x 4 columns
$i1 <int64>
$j1 <int64>
$i2 <int64>
$j2 <int64>
> db$get_sql("SELECT ex1.i AS i1, ex1.j AS j1, ex2.i AS i2, ex2.j AS j2 FROM ex1 INNER JOIN ex2 ON ex1.value = ex2.value") %>% as.tibble
# A tibble: 311 × 4
      i1    j1    i2    j2
   <int> <int> <int> <int>
 1     2     8    10     6
 2     4     4    10     6
 3     8     2    10     6
 4     2     8     6    10
 5     4     4     6    10
 6     8     2     6    10
 7     2     8     7     9
 8     4     4     7     9
 9     8     2     7     9
10     2     8     8     8
# ℹ 301 more rows
# ℹ Use `print(n = ...)` to see more rows
```