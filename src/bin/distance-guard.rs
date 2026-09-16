//! Distance-guard node — end-effector proximity safety for a robot arm.
//!
//! Receives Float64 distance readings (meters) from a proximity sensor
//! and emits a safety flag per reading: 0 = safe, 1 = stop (closer than
//! the safety threshold).  The safety-stop logic every real arm runs
//! between its tool and the environment.
//!
//! Used in the Layer 2 integration-testing demo (demo-final.sh).

use arrow::array::{Array, Float64Array, Int64Array};
use dora_node_api::{DoraNode, Event, MetadataParameters};

/// Default minimum allowed distance in meters — closer than this
/// triggers a stop.
const DEFAULT_SAFETY_DISTANCE_M: f64 = 0.5;

/// Parse the optional `--safety-distance <m>` CLI argument.
fn parse_safety_distance() -> f64 {
    let args: Vec<String> = std::env::args().collect();
    let mut distance = DEFAULT_SAFETY_DISTANCE_M;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--safety-distance" {
            i += 1;
            distance = args
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_SAFETY_DISTANCE_M);
        }
        i += 1;
    }
    distance
}

fn main() -> eyre::Result<()> {
    let safety_distance = parse_safety_distance();
    let (mut node, mut events) =
        DoraNode::init_from_env().map_err(|e| eyre::eyre!("distance-guard: {e}"))?;

    while let Some(event) = events.recv() {
        match event {
            Event::Input { data, .. } => {
                let Some(array) = data.as_array().as_any().downcast_ref::<Float64Array>() else {
                    eprintln!("distance-guard: expected Float64 input");
                    continue;
                };
                for i in 0..array.len() {
                    let distance = array.value(i);
                    let flag: i64 = if distance < safety_distance { 1 } else { 0 };
                    let output = Int64Array::from(vec![flag]);
                    node.send_output(
                        "safety"
                            .parse()
                            .map_err(|e| eyre::eyre!("invalid output_id 'safety': {e}"))?,
                        MetadataParameters::default(),
                        output,
                    )
                    .map_err(|e| eyre::eyre!("send_output(safety) failed: {e}"))?;
                }
            }
            Event::Stop(_) => break,
            Event::InputClosed { .. } => {}
            _ => {}
        }
    }
    Ok(())
}
