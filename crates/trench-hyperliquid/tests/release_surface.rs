use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn cargo_check(project: &Path, target: &Path, source: &str, release: bool) -> Output {
    fs::write(project.join("src/main.rs"), source).expect("write compile probe");
    let mut command = Command::new(env!("CARGO"));
    command
        .arg("check")
        .arg("--offline")
        .arg("--quiet")
        .current_dir(project)
        .env("CARGO_TARGET_DIR", target);
    if release {
        command.arg("--release");
    }
    command.output().expect("run nested cargo check")
}

#[test]
fn public_api_is_closed_and_loopback_hook_is_absent_in_release() {
    let directory = tempfile::tempdir().expect("temporary compile probe");
    let project = directory.path();
    fs::create_dir(project.join("src")).expect("create source directory");
    fs::write(
        project.join("Cargo.toml"),
        format!(
            r#"[package]
name = "info-api-compile-probe"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
trench-hyperliquid = {{ path = {:?} }}
"#,
            env!("CARGO_MANIFEST_DIR")
        ),
    )
    .expect("write probe manifest");
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/info-api-compile-probe");

    for (method, source) in [
        (
            "GapRecoveryRequest::new",
            r#"fn main() {
    let _ = trench_hyperliquid::GapRecoveryRequest::new();
}
"#,
        ),
        (
            "post_json",
            r#"fn main() {
    let client = trench_hyperliquid::InfoClient::new("https://api.hyperliquid.xyz/info").unwrap();
    let _ = client.post_json(&());
}
"#,
        ),
        (
            "action",
            r#"fn main() {
    let client = trench_hyperliquid::InfoClient::new("https://api.hyperliquid.xyz/info").unwrap();
    let _ = client.action();
}
"#,
        ),
        (
            "signer",
            r#"fn main() {
    let client = trench_hyperliquid::InfoClient::new("https://api.hyperliquid.xyz/info").unwrap();
    let _ = client.signer();
}
"#,
        ),
        (
            "wallet",
            r#"fn main() {
    let client = trench_hyperliquid::InfoClient::new("https://api.hyperliquid.xyz/info").unwrap();
    let _ = client.wallet();
}
"#,
        ),
    ] {
        let output = cargo_check(project, &target, source, false);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "`{method}` unexpectedly public");
        assert!(
            stderr.contains(method),
            "compiler failure did not identify `{method}`: {stderr}"
        );
    }

    let output = cargo_check(
        project,
        &target,
        r#"fn main() {
    let _ = trench_hyperliquid::InfoClient::new_loopback_for_test(
        "http://127.0.0.1:32123/info",
    );
}
"#,
        true,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "loopback constructor unexpectedly compiled in release"
    );
    assert!(
        stderr.contains("new_loopback_for_test"),
        "release compiler failure did not identify the test hook: {stderr}"
    );

    for (method, source) in [
        (
            "new_for_test",
            r#"fn main() {
    let config = trench_hyperliquid::WsConfig::new(Vec::new()).unwrap();
    let _ = trench_hyperliquid::WsClient::new_for_test(config, "ws://127.0.0.1:32123".to_owned());
}
"#,
        ),
        (
            "post",
            r#"fn main() {
    let config = trench_hyperliquid::WsConfig::new(Vec::new()).unwrap();
    let client = trench_hyperliquid::WsClient::new(config);
    let _ = client.post();
}
"#,
        ),
        (
            "action",
            r#"fn main() {
    let config = trench_hyperliquid::WsConfig::new(Vec::new()).unwrap();
    let client = trench_hyperliquid::WsClient::new(config);
    let _ = client.action();
}
"#,
        ),
        (
            "new",
            r#"fn main() {
    let config = trench_hyperliquid::WsConfig::new(Vec::new()).unwrap();
    let _ = trench_hyperliquid::WsClient::new(config, "wss://example.invalid/ws");
}
"#,
        ),
    ] {
        let output = cargo_check(project, &target, source, true);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "`{method}` unexpectedly public");
        assert!(
            stderr.contains(method),
            "release compiler failure did not identify `{method}`: {stderr}"
        );
    }
}
