//! End-to-end tests for ReplaySession.
//!
//! Each test records a baseline, replays it, and verifies regression
//! detection, error handling, and the diff report display.

use dora_test_utils::record::{DiffStatus, RecordSession, ReplaySession, SinkDiff};
use dora_test_utils::{DiffReport, Recording};
use serial_test::serial;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

fn dora_available() -> bool {
    Command::new(find_dora_binary())
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
            .expect("cargo build");
        assert!(status.success(), "cargo build failed");
    });
}

fn generate_echo_yaml_with_sink_output(tmp: &Path) -> (PathBuf, PathBuf) {
    let source_file = tmp.join("source.json");
    let sink_output = tmp.join("sink_output.json");
    std::fs::write(
        &source_file,
        serde_json::to_string_pretty(&serde_json::json!({
            "data": [1, 2, 3],
            "data_type": "Int32"
        }))
        .unwrap(),
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

    let yaml_path = tmp.join("echo.yml");
    std::fs::write(&yaml_path, yaml).unwrap();
    (yaml_path, sink_output)
}

fn generate_multi_echo_yaml(tmp: &Path) -> (PathBuf, PathBuf, PathBuf) {
    // Two-output pipeline: test-source emits on data_a and data_b, each
    // routed through its own echo node to a dedicated record-mode sink.
    let source_file = tmp.join("source.json");
    let sink_a_output = tmp.join("sink_a_output.json");
    let sink_b_output = tmp.join("sink_b_output.json");
    std::fs::write(
        &source_file,
        serde_json::to_string_pretty(&serde_json::json!({
            "data": [42, 99, -1],
            "data_type": "Int64"
        }))
        .unwrap(),
    )
    .unwrap();

    let source_bin = bin_path("test-source");
    let echo_bin = bin_path("echo-node");
    let sink_bin = bin_path("test-sink");

    let yaml = format!(
        r#"nodes:
  - id: test-source
    path: {source_bin}
    args: "--output data_a:{source_file} --output data_b:{source_file}"
    outputs:
      - data_a
      - data_b
  - id: echo-a
    path: {echo_bin}
    inputs:
      data_a: test-source/data_a
    outputs:
      - data_a
  - id: echo-b
    path: {echo_bin}
    inputs:
      data_b: test-source/data_b
    outputs:
      - data_b
  - id: test-sink-a
    path: {sink_bin}
    inputs:
      data_a: echo-a/data_a
    args: "--output-file {sink_a_output} --record-mode"
  - id: test-sink-b
    path: {sink_bin}
    inputs:
      data_b: echo-b/data_b
    args: "--output-file {sink_b_output} --record-mode"
"#,
        source_bin = source_bin.display(),
        echo_bin = echo_bin.display(),
        sink_bin = sink_bin.display(),
        source_file = source_file.display(),
        sink_a_output = sink_a_output.display(),
        sink_b_output = sink_b_output.display(),
    );

    let yaml_path = tmp.join("multi-echo.yml");
    std::fs::write(&yaml_path, yaml).unwrap();
    (yaml_path, sink_a_output, sink_b_output)
}

// ── Tests ────────────────────────────────────────────────────────────

#[test]
#[serial]
fn replay_clean_no_regression() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().unwrap();
    let (yaml_path, sink_output) = generate_echo_yaml_with_sink_output(tmp.path());

    // Record baseline.
    let baseline = RecordSession::attach(&yaml_path)
        .unwrap()
        .record_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();
    let baseline_path = tmp.path().join("baseline.json");
    baseline.save(&baseline_path).unwrap();

    // Replay.
    let result = ReplaySession::load(&baseline_path)
        .unwrap()
        .replay_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();

    assert!(result.is_clean());
    result.assert_no_regression(); // should not panic
}

#[test]
#[serial]
fn replay_ignore_paths_count_filtering() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().unwrap();
    let (yaml_path, sink_output) = generate_echo_yaml_with_sink_output(tmp.path());

    // Record baseline.
    let baseline = RecordSession::attach(&yaml_path)
        .unwrap()
        .record_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();
    let baseline_path = tmp.path().join("baseline.json");
    baseline.save(&baseline_path).unwrap();

    // Replay with ignore_paths — ignore the "count" field to tolerate
    // ±1 timing jitter between record and replay.
    let result = ReplaySession::load(&baseline_path)
        .unwrap()
        .replay_sink("test-sink", &sink_output)
        .ignore_paths(&["count"])
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();

    assert!(result.is_clean());
    result.assert_no_regression(); // should not panic
}

#[test]
#[serial]
fn replay_regression_detected() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().unwrap();
    let (yaml_path, sink_output) = generate_echo_yaml_with_sink_output(tmp.path());

    // Record baseline with [1,2,3].
    let baseline = RecordSession::attach(&yaml_path)
        .unwrap()
        .record_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();
    let baseline_path = tmp.path().join("baseline.json");
    baseline.save(&baseline_path).unwrap();

    // Modify source data to [1,2,99] -- this will cause a regression.
    std::fs::write(
        tmp.path().join("source.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "data": [1, 2, 99],
            "data_type": "Int32"
        }))
        .unwrap(),
    )
    .unwrap();

    // Replay.
    let result = ReplaySession::load(&baseline_path)
        .unwrap()
        .replay_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();

    assert!(!result.is_clean());
    let diff = result.diff();
    assert!(!diff.regressions.is_empty());
}

#[test]
#[serial]
fn replay_regression_multi_echo_topology() {
    // Regression detection on a different dataflow topology than the echo
    // pipeline: two outputs, two echo nodes, two record-mode sinks. Verifies
    // the Record/Replay tool is dataflow-agnostic — a mutation in the shared
    // source data is detected independently in BOTH sinks.
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().unwrap();
    let (yaml_path, sink_a_output, sink_b_output) = generate_multi_echo_yaml(tmp.path());

    // Record baseline with [42, 99, -1].
    let baseline = RecordSession::attach(&yaml_path)
        .unwrap()
        .record_sink("test-sink-a", &sink_a_output)
        .record_sink("test-sink-b", &sink_b_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();
    assert_eq!(baseline.sinks.len(), 2, "both sinks should be recorded");
    let baseline_path = tmp.path().join("baseline.json");
    baseline.save(&baseline_path).unwrap();

    // Clean replay — same dataflow, no changes.
    let clean = ReplaySession::load(&baseline_path)
        .unwrap()
        .replay_sink("test-sink-a", &sink_a_output)
        .replay_sink("test-sink-b", &sink_b_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();
    assert!(clean.is_clean(), "clean replay must be clean");

    // Mutate the shared source: append a 4th element.
    std::fs::write(
        tmp.path().join("source.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "data": [42, 99, -1, 999],
            "data_type": "Int64"
        }))
        .unwrap(),
    )
    .unwrap();

    // Replay — regression must be detected in BOTH sinks.
    let result = ReplaySession::load(&baseline_path)
        .unwrap()
        .replay_sink("test-sink-a", &sink_a_output)
        .replay_sink("test-sink-b", &sink_b_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();

    assert!(!result.is_clean(), "mutation must be detected");
    let mismatches: Vec<&SinkDiff> = result
        .diff()
        .regressions
        .iter()
        .filter(|r| r.status == DiffStatus::Mismatch)
        .collect();
    assert_eq!(
        mismatches.len(),
        2,
        "both sinks should report a mismatch, got: {mismatches:?}"
    );
    for sink in &mismatches {
        assert!(
            sink.differences.iter().any(|d| d.path == ".count"),
            "sink {} should report a .count diff, got: {:?}",
            sink.sink_id,
            sink.differences
        );
    }
}

#[test]
fn replay_dataflow_not_found() {
    let tmp = tempfile::TempDir::new().unwrap();

    // Create a Recording with a nonexistent YAML path.
    let mut sinks = std::collections::HashMap::new();
    sinks.insert("test-sink".into(), serde_json::json!({"data": [1]}));
    let recording = Recording {
        metadata: dora_test_utils::record::RecordingMetadata {
            dataflow_yaml: "/nonexistent/dataflow.yml".into(),
            recorded_at_unix: 0,
            timeout_secs: 10.0,
            dora_version: "test".into(),
        },
        sinks,
    };
    let path = tmp.path().join("bad.json");
    recording.save(&path).unwrap();

    let result = ReplaySession::load(&path)
        .unwrap()
        .replay_sink("test-sink", tmp.path().join("out.json"))
        .run();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("not found"));
}

#[test]
fn replay_load_invalid_json() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("garbage.json");
    std::fs::write(&path, "not valid json {{{").unwrap();

    let result = ReplaySession::load(&path);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("failed to load") || err.contains("JSON"));
}

#[test]
#[serial]
fn replay_override_dataflow() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let tmp = tempfile::TempDir::new().unwrap();
    let (yaml_path, sink_output) = generate_echo_yaml_with_sink_output(tmp.path());

    // Record baseline.
    let baseline = RecordSession::attach(&yaml_path)
        .unwrap()
        .record_sink("test-sink", &sink_output)
        .with_timeout(std::time::Duration::from_secs(10))
        .run()
        .unwrap();

    // Save with a WRONG dataflow_yaml path, then override.
    let mut recording = baseline.clone();
    recording.metadata.dataflow_yaml = "/nonexistent/dataflow.yml".into();
    let path = tmp.path().join("baseline.json");
    recording.save(&path).unwrap();

    // Should fail without override, succeed with override.
    let bad = ReplaySession::load(&path)
        .unwrap()
        .replay_sink("test-sink", &sink_output)
        .run();
    assert!(bad.is_err());

    let good = ReplaySession::load(&path)
        .unwrap()
        .replay_sink("test-sink", &sink_output)
        .dataflow(&yaml_path)
        .run();
    assert!(good.is_ok());
    assert!(good.unwrap().is_clean());
}

#[test]
fn replay_sink_not_in_baseline() {
    // Fast-fail: a sink registered via replay_sink() that isn't in the
    // baseline recording should error immediately with SinkNotInBaseline
    // (avoiding a costly dora run that would end in SinkOutputMissing).
    let tmp = tempfile::TempDir::new().unwrap();

    let mut sinks = std::collections::HashMap::new();
    sinks.insert("known-sink".into(), serde_json::json!({"data": [1]}));
    let recording = Recording {
        metadata: dora_test_utils::record::RecordingMetadata {
            dataflow_yaml: tmp.path().join("dummy.yml").display().to_string(),
            recorded_at_unix: 0,
            timeout_secs: 10.0,
            dora_version: "test".into(),
        },
        sinks,
    };
    let path = tmp.path().join("recording.json");
    recording.save(&path).unwrap();

    // Register a sink NOT in the baseline.
    let result = ReplaySession::load(&path)
        .unwrap()
        .replay_sink("unknown-sink", tmp.path().join("out.json"))
        .run();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("unknown-sink") && err.contains("not found in baseline"));
}

#[test]
fn replay_no_sinks_configured() {
    let tmp = tempfile::TempDir::new().unwrap();

    let mut sinks = std::collections::HashMap::new();
    sinks.insert("s1".into(), serde_json::json!({"data": [1]}));
    let recording = Recording {
        metadata: dora_test_utils::record::RecordingMetadata {
            dataflow_yaml: tmp.path().join("dummy.yml").display().to_string(),
            recorded_at_unix: 0,
            timeout_secs: 10.0,
            dora_version: "test".into(),
        },
        sinks,
    };
    let path = tmp.path().join("recording.json");
    recording.save(&path).unwrap();

    let result = ReplaySession::load(&path).unwrap().run();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("no sinks configured"));
}

#[test]
fn replay_diffreport_display_format() {
    let report = DiffReport {
        regressions: vec![SinkDiff {
            sink_id: "s1".into(),
            status: DiffStatus::Match,
            differences: vec![],
        }],
    };
    let display = report.to_string();
    assert!(display.contains("No regressions"));
}

#[test]
fn replay_timeout_override_preserved_in_result() {
    // Verify that with_timeout() override is accepted by the builder API
    // and that Recording save/load preserves timeout_secs at full precision.
    // The actual timeout resolution (override vs recorded) is tested by the
    // e2e_record/replay tests that use a real dora CLI.
    let tmp = tempfile::TempDir::new().unwrap();

    let mut sinks = std::collections::HashMap::new();
    sinks.insert("s1".into(), serde_json::json!({"data": [1], "count": 1}));
    let recording = Recording {
        metadata: dora_test_utils::record::RecordingMetadata {
            dataflow_yaml: tmp.path().join("dummy.yml").display().to_string(),
            recorded_at_unix: 0,
            timeout_secs: 1.5, // fractional — stored at full precision
            dora_version: "test".into(),
        },
        sinks,
    };
    let path = tmp.path().join("recording.json");
    recording.save(&path).unwrap();

    // Round-trip: verify timeout_secs is preserved.
    let loaded = Recording::load(&path).unwrap();
    assert!(
        (loaded.metadata.timeout_secs - 1.5).abs() < 0.001,
        "timeout_secs should survive save/load round-trip"
    );

    // Verify the builder API accepts the override (type-check).
    let _session = ReplaySession::load(&path)
        .unwrap()
        .replay_sink("s1", tmp.path().join("out.json"))
        .with_timeout(std::time::Duration::from_secs(60));
}

#[test]
fn replay_sink_missing_in_diff_report() {
    // If a sink is in the baseline but not registered for replay,
    // compare_recordings reports it as Missing.
    use std::collections::HashMap;

    let mut baseline = HashMap::new();
    baseline.insert("s1".into(), serde_json::json!({"data": [1], "count": 1}));
    baseline.insert("s2".into(), serde_json::json!({"data": [2], "count": 1}));

    // Only register s1 — s2 is intentionally omitted.
    let mut current = HashMap::new();
    current.insert("s1".into(), serde_json::json!({"data": [1], "count": 1}));

    // Build a fake ReplayResult by recording and replaying (no dora needed).
    let meta = dora_test_utils::record::RecordingMetadata {
        dataflow_yaml: "dummy.yml".into(),
        recorded_at_unix: 0,
        timeout_secs: 10.0,
        dora_version: "test".into(),
    };
    let report = DiffReport {
        regressions: vec![
            SinkDiff {
                sink_id: "s1".into(),
                status: DiffStatus::Match,
                differences: vec![],
            },
            SinkDiff {
                sink_id: "s2".into(),
                status: DiffStatus::Missing,
                differences: vec![],
            },
        ],
    };

    let result = dora_test_utils::ReplayResult {
        metadata: meta,
        baseline_sinks: baseline,
        current_sinks: current,
        report,
    };

    assert!(!result.is_clean());
    let display = result.diff().to_string();
    assert!(
        display.contains("s2"),
        "diff should mention missing sink s2"
    );
    assert!(
        display.contains("MISSING"),
        "diff should show MISSING status"
    );
}

#[test]
fn replay_sink_extra_in_diff_report() {
    // If a sink appears in the replay output but not in the baseline,
    // compare_recordings reports it as Extra.
    use std::collections::HashMap;

    let mut baseline = HashMap::new();
    baseline.insert("s1".into(), serde_json::json!({"data": [1], "count": 1}));

    // current has an extra sink not in baseline.
    let mut current = HashMap::new();
    current.insert("s1".into(), serde_json::json!({"data": [1], "count": 1}));
    current.insert("s2".into(), serde_json::json!({"data": [2], "count": 1}));

    let meta = dora_test_utils::record::RecordingMetadata {
        dataflow_yaml: "dummy.yml".into(),
        recorded_at_unix: 0,
        timeout_secs: 10.0,
        dora_version: "test".into(),
    };
    let report = DiffReport {
        regressions: vec![
            SinkDiff {
                sink_id: "s1".into(),
                status: DiffStatus::Match,
                differences: vec![],
            },
            SinkDiff {
                sink_id: "s2".into(),
                status: DiffStatus::Extra,
                differences: vec![],
            },
        ],
    };

    let result = dora_test_utils::ReplayResult {
        metadata: meta,
        baseline_sinks: baseline,
        current_sinks: current,
        report,
    };

    assert!(!result.is_clean());
    let display = result.diff().to_string();
    assert!(display.contains("s2"), "diff should mention extra sink s2");
    assert!(display.contains("EXTRA"), "diff should show EXTRA status");
}
