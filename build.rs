extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // Build client.o
    cc::Build::new()
        .cpp(true)
        .define("PROJECT_ROOT","\"/opt/scidb/22.5\"")
        .define("SCIDB_CLIENT", "1")
        .include("/opt/scidb/22.5/include")
        .include("include")
        .include("/usr/pgsql-9.3/include")
        .include("/usr/include/boost169")
        .file("include/client.h")
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
    println!("cargo:rustc-link-search=native=/opt/scidb/22.5/lib");
    println!("cargo:rustc-link-lib=scidbclient");
}
