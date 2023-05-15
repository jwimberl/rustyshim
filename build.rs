extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // Build client.o
    let scidb_ver = env::var("SCIDB_VER").unwrap();
    let mut scidb_path = String::from("/opt/scidb/");
    scidb_path.push_str(&scidb_ver);
    scidb_path.push_str("/include");
    let mut scidb_lpath = String::from("/opt/scidb/");
    scidb_lpath.push_str(&scidb_ver);
    scidb_lpath.push_str("/lib");
    let mut project_root = String::from("\"");
    project_root.push_str(&scidb_path);
    project_root.push_str("\"");
    let postgres_ver = env::var("POSTGRES_VER").unwrap();
    let mut postgres_path = String::from("/usr/pgsql-");
    postgres_path.push_str(&postgres_ver);
    postgres_path.push_str("/include");
    cc::Build::new()
        .cpp(true)
        .define("PROJECT_ROOT", &project_root[..])
        .define("SCIDB_CLIENT", "1")
        .include(&scidb_path[..])
        .include(&postgres_path[..])
        .include("/usr/include/boost169")
        .include("extern")
        .file("src/client.cpp")
        .flag("-std=c++17")
        .flag("-Wno-unused-parameter")
        .flag("-Wno-unused-variable")
        .compile("client");

    // Make Rust bindings for client.h
    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    // Linking scidb
    // Our libclient.a will be linked automatically
    println!("cargo:rustc-link-search=native={}", scidb_lpath);
    println!("cargo:rustc-link-lib=scidbclient");
}
