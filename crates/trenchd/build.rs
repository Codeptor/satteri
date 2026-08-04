use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

const ROOT_INPUTS: &[&str] = &["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"];
const CRATE_EXTENSIONS: &[&str] = &["rs", "toml", "sql"];
const SCHEMA_EXTENSIONS: &[&str] = &["json"];

fn main() {
    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets manifest directory"),
    );
    let workspace = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("trenchd is nested under the workspace root");
    let mut files = ROOT_INPUTS
        .iter()
        .map(|name| workspace.join(name))
        .collect::<Vec<_>>();
    collect_allowed_files(&workspace.join("crates"), CRATE_EXTENSIONS, &mut files);
    collect_allowed_files(&workspace.join("schemas"), SCHEMA_EXTENSIONS, &mut files);
    files.sort();

    let mut hasher = blake3::Hasher::new_derive_key("trench.workspace.build.v1");
    for file in files {
        let relative = file
            .strip_prefix(workspace)
            .expect("workspace input stays beneath root")
            .to_str()
            .expect("workspace input names must be UTF-8");
        let bytes = fs::read(&file).expect("versioned workspace input must remain readable");
        hasher.update(&(relative.len() as u64).to_be_bytes());
        hasher.update(relative.as_bytes());
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
        println!("cargo:rerun-if-changed={}", file.display());
    }
    println!(
        "cargo:rustc-env=TRENCH_WORKSPACE_BUILD_DIGEST=b3:{}",
        hasher.finalize().to_hex()
    );
}

fn collect_allowed_files(directory: &Path, extensions: &[&str], files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(directory).expect("versioned workspace input directory must remain readable");
    for entry in entries {
        let entry = entry.expect("workspace input directory entry must remain readable");
        let path = entry.path();
        let metadata =
            fs::symlink_metadata(&path).expect("workspace input metadata must remain readable");
        if metadata.file_type().is_symlink() {
            panic!(
                "workspace build digest refuses symlinked input: {}",
                path.display()
            );
        }
        if metadata.is_dir() {
            collect_allowed_files(&path, extensions, files);
        } else if metadata.is_file()
            && extensions
                .iter()
                .any(|extension| path.extension() == Some(OsStr::new(extension)))
        {
            files.push(path);
        }
    }
}
