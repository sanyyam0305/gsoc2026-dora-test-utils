//! End-to-end test for the dora-test-utils harness.
//!
//! Exercises the full input pipeline: send_input → tick → receive events.

use dora_node_api::Event;
use dora_test_utils::NodeHarness;

/// Full input pipeline: inject Input + Stop, then tick through both events.
///
/// Pipeline:
/// 1. Create harness
/// 2. Inject Input event with integer data, then Stop
/// 3. Tick — receive Input, verify id and data present
/// 4. Tick — receive Stop, verify stream ends
#[test]
fn e2e_receive_input_and_stop() {
    let mut harness = NodeHarness::new().expect("NodeHarness::new should succeed");

    // ── Send Input event with integer data ────────────────────────
    harness.send_data("numbers", serde_json::json!([1, 2, 3]));

    // ── Send Stop (preload so the daemon thread doesn't block) ────
    harness.send_stop();

    // ── Tick 1: should receive Input ──────────────────────────────
    let event = harness.tick().expect("tick should return an event");
    match event {
        dora_node_api::Event::Input { id, data, .. } => {
            assert_eq!(id.to_string(), "numbers");
            // Data should be non-empty (3-element Int32 array)
            assert!(
                !data.as_array().is_empty(),
                "input data should be non-empty"
            );
        }
        other => panic!("expected Input event, got {other:?}"),
    }

    // ── Tick 2: should receive Stop ───────────────────────────────
    let event = harness.tick().expect("tick should return Stop");
    assert!(
        matches!(event, dora_node_api::Event::Stop(..)),
        "expected Stop event, got {event:?}"
    );

    // ── Stream should be exhausted after Stop ─────────────────────
    assert!(
        harness.tick().is_none(),
        "stream should be exhausted after Stop"
    );
}

/// Output path: send_output → recv_output (no tick needed).
///
/// send_output() automatically calls close_input() to unblock the daemon
/// thread, so the caller does not need to manage the input channel lifecycle.
#[test]
fn e2e_send_output_and_recv() {
    let mut harness = NodeHarness::new().expect("NodeHarness::new should succeed");

    // Send an output via the harness (delegates to DoraNode::send_output).
    // send_output() auto-closes the input channel, preventing deadlock.
    let output_id = "test_output";
    let array = arrow::array::Int32Array::from(vec![10, 20, 30]);
    harness
        .send_output(output_id, array)
        .expect("send_output should succeed");

    // Retrieve the output.
    let outputs = harness
        .recv_output(output_id)
        .expect("should have captured output for 'test_output'");
    assert_eq!(outputs.len(), 1, "expected one output message");
    assert!(
        outputs[0].contains_key("data"),
        "output should contain data"
    );
    assert!(outputs[0].contains_key("id"), "output should contain id");
    let output_id_value = outputs[0].get("id").and_then(|v| v.as_str());
    assert_eq!(
        output_id_value,
        Some("test_output"),
        "output id should match"
    );
}

/// run_to_completion: pre-load Input, verify all events returned (no manual Stop needed).
///
/// run_to_completion() auto-injects a Stop event and auto-calls close_input(),
/// so the caller only needs to send the inputs they want to test.
#[test]
fn e2e_run_to_completion_returns_events() {
    let mut harness = NodeHarness::new().expect("NodeHarness::new should succeed");

    // Pre-load an Input event with data.
    harness.send_data("step1", serde_json::json!([42]));

    // No need to call send_stop() — run_to_completion handles it.
    // Run to completion.
    let events = harness.run_to_completion();

    // Should have received both Input and Stop.
    assert!(
        events.len() >= 2,
        "expected at least 2 events (Input + Stop), got {}",
        events.len()
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::Input { .. })),
        "should contain an Input event"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::Stop(..))),
        "should contain a Stop event"
    );

    // After run_to_completion(), send_output should work (close_input was called).
    let array = arrow::array::Int32Array::from(vec![99]);
    harness
        .send_output("post_run", array)
        .expect("send_output should succeed after run_to_completion");

    let outputs = harness.recv_output("post_run");
    assert!(outputs.is_some(), "should have captured output after run");
}

/// Full pipeline: send_input → run_to_completion → send_output → recv_output.
///
/// Verifies that both input and output paths work in the same harness
/// lifecycle.  run_to_completion() auto-injects Stop and auto-closes input.
#[test]
fn e2e_full_pipeline_input_to_output() {
    let mut harness = NodeHarness::new().expect("NodeHarness::new should succeed");

    // Phase 1: Send input, drive to completion (no manual Stop needed).
    harness.send_data("data_in", serde_json::json!([1, 2, 3, 4, 5]));

    let events = harness.run_to_completion();
    // Verify both Input and Stop were received.
    assert!(
        events.iter().any(|e| matches!(e, Event::Input { .. })),
        "should have received Input"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::Stop(..))),
        "should have received Stop"
    );

    // Phase 2: After completion, send outputs (close_input was called).
    let array1 = arrow::array::Float64Array::from(vec![1.1, 2.2, 3.3]);
    harness
        .send_output("results", array1)
        .expect("send_output should succeed after run_to_completion");

    let array2 = arrow::array::Float64Array::from(vec![4.4, 5.5]);
    harness
        .send_output("results", array2)
        .expect("second send_output should also succeed");

    // Phase 3: Retrieve all outputs for "results".
    let outputs = harness
        .recv_output("results")
        .expect("should have captured outputs for 'results'");
    assert_eq!(outputs.len(), 2, "expected 2 output messages for 'results'");
    for output in &outputs {
        assert!(
            output.contains_key("data"),
            "each output should contain 'data'"
        );
    }
}

/// send_data with Arrow ArrayData: verify Arrow->JSON->Input round-trip.
#[test]
fn e2e_send_data_arrow_input() {
    use arrow::array::{Array, Int32Array};

    let mut harness = NodeHarness::new().expect("NodeHarness::new should succeed");

    // Inject Arrow data via the convenience method.
    let array = Int32Array::from(vec![100, 200, 300]).into_data();
    harness.send_data("arrow_numbers", array);
    harness.send_stop();

    // Tick — verify the data was received with correct values.
    let event = harness.tick().expect("should receive Input");
    match event {
        Event::Input { id, data, .. } => {
            assert_eq!(id.to_string(), "arrow_numbers");
            // Verify content: data arrived (non-empty). The Arrow→JSON→Arrow
            // round-trip may change the concrete Arrow type (e.g. Int32→Struct),
            // but the values and element count must be preserved.
            let len = data.as_array().len();
            assert!(!data.as_array().is_empty(), "data should be non-empty");
            assert_eq!(len, 3, "expected 3 elements after round-trip");
            // Check that values are recognizable in the debug output.
            let debug_str = format!("{data:?}");
            assert!(
                debug_str.contains("100"),
                "missing value 100 in {debug_str}"
            );
            assert!(
                debug_str.contains("200"),
                "missing value 200 in {debug_str}"
            );
            assert!(
                debug_str.contains("300"),
                "missing value 300 in {debug_str}"
            );
        }
        other => panic!("expected Input, got {other:?}"),
    }
}
