//! End-to-end tests for RecordSession.
//!
//! Each test generates a temporary YAML dataflow with TestSource and
//! TestSink (in record_mode), runs it via RecordSession, and verifies
//! the recording output.

use dora_test_utils::record::RecordSession;
use serial_test::serial;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// Check whether `dora` CLI is available.
fn dora_available() -> bool {
    let dora = find_dora_binary();
    Command::new(&dora)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Require the `dora` CLI before running an e2e test.
///
/// In CI (`CI=true`), panics if unavailable — a missing CLI means the
/// workflow is broken, and skipping would report a green run that tested
/// nothing. Locally, warns and returns `false` so the caller can skip.
fn require_dora() -> bool {
    if dora_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!(
            "dora CLI not found on PATH — required by e2e tests in CI.\n\
             The CI workflow should install dora before running these tests."
        );
    }
    eprintln!(
        "⚠️  SKIP: dora CLI not found on PATH — e2e tests will be skipped.\n\
         Install dora or run `cargo test --lib` for unit tests only."
    );
    false
}

fn find_dora_binary() -> PathBuf {
    for profile in &["debug", "release"] {
        let local = Path::new("dora/target").join(profile).join("dora");
        if local.exists() {
            return local;
        }
    }
    PathBuf::from("dora")
}

fn bin_path(name: &str) -> PathBuf {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    Path::new(&target_dir).join(profile).join(name)
}

fn build_binaries() {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let mut args = vec![
            "build",
            "--bin",
            "test-source",
            "--bin",
            "test-sink",
            "--bin",
            "echo-node",
        ];
        if !cfg!(debug_assertions) {
            args.push("--release");
        }
        let status = Command::new("cargo")
            .args(&args)
            .status()
            .expect("cargo build should succeed");
        assert!(status.success(), "cargo build failed");
    });
}

/// Generate a record-mode echo pipeline YAML in `tmp_dir` and return the
/// YAML path + sink output file path.
fn generate_record_echo_yaml(tmp_dir: &std::path::Path) -> (PathBuf, PathBuf) {
    let source_file = tmp_dir.join("source.json");
    let sink_output = tmp_dir.join("record_output.json");

    // Write source data.
    let source_data = serde_json::json!({
        "data": [42, 99, -1],
        "data_type": "Int64"
    });
    std::fs::write(
        &source_file,
        serde_json::to_string_pretty(&source_data).unwrap(),
    )
    .unwrap();

    let source_bin = bin_path("test-source");
    let echo_bin = bin_path("echo-node");
    let sink_bin = bin_path("test-sink");

    let yaml = format!(
        r#"nodes:
  - id: test-source
    path: {source_bin}
    args: "--output-id data --data-file {source_file}"
    outputs:
      - data

  - id: echo-node
    path: {echo_bin}
    inputs:
      data: test-source/data
    outputs:
      - data

  - id: test-sink
    path: {sink_bin}
    inputs:
      data: echo-node/data
    args: "--output-file {sink_output} --record-mode"
"#,
        source_bin = source_bin.display(),
        echo_bin = echo_bin.display(),
        sink_bin = sink_bin.display(),
        source_file = source_file.display(),
        sink_output = sink_output.display(),
    );

    let yaml_file = tmp_dir.join("echo-record.yml");
    std::fs::write(&yaml_file, yaml).unwrap();
    (yaml_file, sink_output)
}

// ── Tests ────────────────────────────────────────────────────────────

#[test]
#[serial]
fn record_echo_pipeline() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (yaml_path, sink_output) = generate_record_echo_yaml(tmp.path());

    let recording = RecordSession::attach(&yaml_path)
        .expect("attach should succeed")
        .record_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(15))
        .run()
        .expect("record run should succeed");

    // Verify metadata.
    assert!(
        recording.metadata.dataflow_yaml.contains("echo-record.yml"),
        "metadata should reference the YAML file"
    );
    assert!(recording.metadata.recorded_at_unix > 0);
    assert!(
        (recording.metadata.timeout_secs - 15.0).abs() < f64::EPSILON,
        "timeout should be 15.0 seconds"
    );

    // Verify sink data.
    let sink_data = recording
        .sinks
        .get("test-sink")
        .expect("should have test-sink output");
    assert_eq!(sink_data["count"], serde_json::json!(3));
    assert!(sink_data["data"].is_array());
    assert_eq!(sink_data["data"].as_array().unwrap().len(), 3);
}

#[test]
#[serial]
fn record_save_and_load_roundtrip() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (yaml_path, sink_output) = generate_record_echo_yaml(tmp.path());

    let recording = RecordSession::attach(&yaml_path)
        .expect("attach should succeed")
        .record_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(15))
        .run()
        .expect("record run should succeed");

    // Save to file.
    let save_path = tmp.path().join("recording.json");
    recording.save(&save_path).expect("save should succeed");
    assert!(save_path.exists(), "recording file should exist");

    // Load from file.
    let loaded = dora_test_utils::Recording::load(&save_path).expect("load should succeed");

    // Verify round-trip fidelity.
    assert_eq!(
        loaded.metadata.timeout_secs,
        recording.metadata.timeout_secs
    );
    assert_eq!(
        loaded.metadata.dora_version,
        recording.metadata.dora_version
    );
    assert_eq!(loaded.sinks.len(), recording.sinks.len());
    assert_eq!(
        loaded.sinks.get("test-sink").unwrap()["count"],
        recording.sinks.get("test-sink").unwrap()["count"]
    );
}

#[test]
fn record_dataflow_not_found() {
    let result = RecordSession::attach("/nonexistent/path/dataflow.yml");
    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("not found") || err_msg.contains("nonexistent"),
        "error should mention file not found, got: {err_msg}"
    );
}

#[test]
fn record_no_sinks_configured() {
    // Create a minimal valid YAML file.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let yaml_path = tmp.path().join("empty.yml");
    std::fs::write(&yaml_path, "nodes: []\n").unwrap();

    let result = RecordSession::attach(&yaml_path)
        .expect("attach should succeed")
        .run();

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("No sinks configured") || err_msg.contains("no sinks"),
        "error should mention no sinks, got: {err_msg}"
    );
}
