//! dora-test-utils Demo — NodeHarness unit testing (Layer 1).
//!
//! Shows the recommended structure for testing a DORA node:
//!
//!   1. Node logic lives in a LIBRARY module (below, `node_logic`) — this
//!      represents the `lib.rs` of the node's own crate.
//!   2. Pure logic is tested with plain asserts — no harness needed.
//!   3. The event-loop integration (receiving events, emitting outputs) is
//!      tested with NodeHarness — the harness drives the loop, and the loop
//!      calls THE SAME logic functions. Nothing is copied.
//!
//! Scenario: a joint-limit safety monitor for a Realman GEN72 robot arm
//! (7 joints).  It receives a joint configuration (7 angles, degrees) per
//! event and emits an emergency stop with the offending joint number when
//! any joint leaves its allowed range.
//!
//! Limits from the Realman developer docs (develop.realman-robotics.com,
//! "GEN72 Series Parameters and D-H Model") — note J4 and J6 are
//! asymmetric:
//!   J1 ±172° · J2 ±105° · J3 ±172° · J4 −165°…+55° ·
//!   J5 ±172° · J6 −90°…+120° · J7 ±172°
//!
//! Run: cargo run --example harness_demo
//! (No dora CLI needed — this demo is fully in-memory.)

use arrow::array::{Array, Float64Array, Int64Array};
use dora_node_api::Event;
use dora_test_utils::NodeHarness;

// ── 1. The node's logic ──────────────────────────────────────
// In a real project this module lives in the node's own crate
// (e.g. `gen72-safety/src/lib.rs`) and is shared by the binary and tests.

mod node_logic {
    /// GEN72 joint angle limits in degrees, (min, max) for J1..J7.
    ///
    /// Source: Realman developer docs — GEN72 Series Parameters and D-H
    /// Model (develop.realman-robotics.com).
    pub const JOINT_LIMITS: [(f64, f64); 7] = [
        (-172.0, 172.0), // J1 — base
        (-105.0, 105.0), // J2 — shoulder
        (-172.0, 172.0), // J3 — shoulder
        (-165.0, 55.0),  // J4 — elbow (asymmetric)
        (-172.0, 172.0), // J5 — wrist
        (-90.0, 120.0),  // J6 — wrist (asymmetric)
        (-172.0, 172.0), // J7 — wrist
    ];

    /// Business logic: the first joint (0-based index) whose angle is
    /// outside its allowed range, or `None` if the configuration is safe.
    pub fn first_joint_violation(angles: &[f64]) -> Option<usize> {
        JOINT_LIMITS
            .iter()
            .zip(angles)
            .position(|(&(min, max), &angle)| angle < min || angle > max)
    }

    /// Deliberately broken variant — simulates a developer typo (J4 max
    /// limit 55 typed as 200).  Used only by Part C to show what a
    /// failing test looks like; a real project would never ship this.
    pub fn buggy_first_joint_violation(angles: &[f64]) -> Option<usize> {
        let mut limits = JOINT_LIMITS;
        limits[3].1 = 200.0; // the bug: J4's +55° limit became +200°
        limits
            .iter()
            .zip(angles)
            .position(|(&(min, max), &angle)| angle < min || angle > max)
    }
}

fn section(title: &str) {
    println!("\n═══ {} ═══\n", title);
}
fn step(msg: &str) {
    println!("▸ {msg}");
}
fn ok(msg: &str) {
    println!("  ✅ {msg}");
}
/// Print an error signal in red.
fn err(msg: &str) {
    println!("  \x1b[0;31m\x1b[1m❌ {msg}\x1b[0m");
}
fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("  ❌ {msg}");
    std::process::exit(1);
}

/// Read joint angles out of an incoming Arrow payload.
///
/// The harness wraps injected data in a single-column RecordBatch, so the
/// payload arrives as a one-field StructArray ("data" column).
fn read_angles(arr: &arrow::array::ArrayRef) -> Vec<f64> {
    use arrow::array::StructArray;
    let s = match arr.as_any().downcast_ref::<StructArray>() {
        Some(s) => s,
        None => fail(format!("joints payload type: {:?}", arr.data_type())),
    };
    let col = s
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("joints column should be Float64");
    col.iter().map(|v| v.unwrap_or_default()).collect()
}

fn main() {
    section("dora-test-utils — NodeHarness Demo (Layer 1: unit testing)");
    println!("  Realman GEN72 joint-limit safety monitor (7 joints)");
    println!("  Recommended structure: logic in a library module,");
    println!("  tested directly AND through the harness — no copying.");
    println!();

    // ── Part A: pure-logic tests (no harness needed) ─────────
    section("Part A — Test the logic directly (plain asserts)");

    step("safe configuration (all joints at 0°)");
    let safe = vec![0.0; 7];
    println!(
        "    first_joint_violation = {:?}",
        node_logic::first_joint_violation(&safe)
    );
    if node_logic::first_joint_violation(&safe).is_some() {
        fail("all-zero configuration should be safe");
    }
    ok("correct");

    step("boundary: J4 exactly at its +55° limit (inclusive)");
    let boundary = vec![0.0, 0.0, 0.0, 55.0, 0.0, 0.0, 0.0];
    if node_logic::first_joint_violation(&boundary).is_some() {
        fail("angle exactly AT the limit must not trip the alarm");
    }
    ok("correct");

    step("J4 at +55.5° (0.5° past the asymmetric elbow limit)");
    let elbow_over = vec![0.0, 0.0, 0.0, 55.5, 0.0, 0.0, 0.0];
    match node_logic::first_joint_violation(&elbow_over) {
        Some(3) => ok("correct — reports joint 3 (0-based)"),
        other => fail(format!("expected Some(3), got {other:?}")),
    }

    step("J2 at −106° (below the ±105° shoulder limit)");
    let shoulder_under = vec![0.0, -106.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    match node_logic::first_joint_violation(&shoulder_under) {
        Some(1) => ok("correct — reports joint 1 (0-based)"),
        other => fail(format!("expected Some(1), got {other:?}")),
    }

    step("J6 at −91° (below the asymmetric −90° wrist limit)");
    let wrist_under = vec![0.0, 0.0, 0.0, 0.0, 0.0, -91.0, 0.0];
    match node_logic::first_joint_violation(&wrist_under) {
        Some(5) => ok("correct — reports joint 5 (0-based)"),
        other => fail(format!("expected Some(5), got {other:?}")),
    }

    println!();
    println!("  (These asserts call the SAME function the node uses in");
    println!("   production — nothing is rewritten for the test.)");

    // ── Part B: event-loop integration (NodeHarness) ─────────
    section("Part B — Test the event loop with NodeHarness");

    step("Create harness (no daemon started)");
    let mut harness = NodeHarness::new().expect("harness creation failed");
    ok("harness ready");

    step("Inject 2 joint configurations (Arrow Float64Array, degrees)");
    // Configuration 1: a realistic safe pose (degrees).
    let safe_pose = Float64Array::from(vec![30.0, -35.0, 109.0, -17.0, 46.0, 66.0, 0.0]);
    harness.send_data("joints", safe_pose.into_data());
    // Configuration 2: same pose but J4 at 62° (limit +55°) — dangerous.
    let bad_pose = Float64Array::from(vec![30.0, -35.0, 109.0, 62.0, 46.0, 66.0, 0.0]);
    harness.send_data("joints", bad_pose.into_data());
    harness.send_stop();
    ok("2 configurations + Stop buffered");

    step("Drive the event loop (the node's shell, adapted to the harness)");
    loop {
        match harness.tick() {
            Some(Event::Input { id, data, .. }) => {
                assert_eq!(id.as_str(), "joints", "unexpected input id");
                // The node's event handler — calling THE SAME logic function.
                let angles = read_angles(&data.0);
                match node_logic::first_joint_violation(&angles) {
                    Some(joint) => {
                        println!("    joint {joint} out of range → emergency stop");
                        let joint_no = Int64Array::from(vec![(joint + 1) as i64]);
                        harness
                            .send_output("emergency_stop", joint_no)
                            .expect("send_output failed");
                    }
                    None => println!("    all joints within limits"),
                }
            }
            Some(Event::Stop(..)) => {
                println!("    stop event — event stream drained");
                break;
            }
            Some(other) => println!("    (other event: {other:?})"),
            None => break,
        }
    }
    ok("node processed both configurations through the same logic function");

    step("Capture the output and assert");
    let outputs = harness
        .recv_output("emergency_stop")
        .ok_or("no output captured")
        .unwrap_or_else(|e| fail(e));
    println!("    captured: {outputs:?}");

    // Captured shape: {"id": ..., "data": Array [Number(..)], "data_type": ...}
    let stopped_joints: Vec<i64> = outputs
        .iter()
        .filter_map(|m| m.get("data"))
        .filter_map(|v| v.as_array())
        .filter_map(|a| a.first())
        .filter_map(|v| v.as_i64())
        .collect();
    if stopped_joints != vec![4] {
        fail(format!(
            "assertion failed: emergency stops for joints {stopped_joints:?}, \
             expected [4] — exactly one stop, from J4"
        ));
    }
    ok("assertion passed: exactly one emergency stop, joint 4 (1-based)");

    // ── Part C: catching a bug (the point of testing) ─────
    section("Part C — Catch a bug: what a failing test looks like");

    step("Simulate a developer typo: J4 max limit 55° typed as 200°");
    step("(node_logic::buggy_first_joint_violation — a deliberately broken copy)");
    step("The test asserts the CORRECT behavior — J4 at 62° must trip the alarm:");
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_eq!(
            node_logic::buggy_first_joint_violation(&[0.0, 0.0, 0.0, 62.0, 0.0, 0.0, 0.0]),
            Some(3),
            "J4 at 62° exceeds the +55° limit"
        );
    }));
    match caught {
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "(non-string panic message)".to_string());
            err(&format!("test failed: {msg}"));
            ok("the bug was caught by the test — this is what tests are for");
        }
        Ok(()) => fail("the buggy logic PASSED — the test would NOT have caught it"),
    }

    // ── Summary ─────────────────────────────────────────
    section("Summary");
    println!("  ✅ node_logic module        — the GEN72 limit-check logic (its lib.rs)");
    println!("  ✅ Part A                   — pure logic tested with plain asserts,");
    println!("                                 including the asymmetric J4/J6 limits");
    println!("  ✅ Part B                   — event loop driven by NodeHarness, calling");
    println!("                                 the SAME logic functions (no copy)");
    println!("  ✅ Part C                   — a simulated bug caught by the assertions");
    println!();
    println!("  This is the recommended structure for testing a DORA node:");
    println!("    lib.rs  — the logic, written once");
    println!("    main.rs — the shell: real daemon in, logic called, outputs out");
    println!("    tests   — plain asserts on the logic + NodeHarness for the loop");
}
