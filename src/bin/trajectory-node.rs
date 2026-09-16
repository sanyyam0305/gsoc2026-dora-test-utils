//! Trajectory-node — GEN72 joint-space trajectory interpolation.
//!
//! Receives target configurations on the "target" input — each target is
//! 7 consecutive Float64 scalars (J1..J7, degrees).  On every 7th value
//! the node linearly interpolates all 7 joints from the current
//! configuration to the target in `--steps` interpolation steps and
//! emits each step as 7 consecutive Float64 scalars on "trajectory".
//!
//! Pure computation — no sensors, no real-time loop — so the output is
//! fully deterministic: the Record/Replay regression demo detects the
//! trajectory change when `--steps` is altered (a real motion-control
//! regression: coarser interpolation resolution).
//!
//! Used in the Layer 3 regression-testing demo (demo_replay.rs).

use arrow::array::{Array, Float64Array};
use dora_node_api::{DoraNode, Event, MetadataParameters};

/// GEN72 joint count.
const JOINTS: usize = 7;

/// Parse the `--steps <n>` CLI argument (default 10).  Fails on
/// `--steps 0` (or a missing/unparseable value) — zero steps would
/// silently emit no trajectory at all.
fn parse_steps() -> eyre::Result<usize> {
    let args: Vec<String> = std::env::args().collect();
    let mut steps = 10usize;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--steps" {
            i += 1;
            let raw = args
                .get(i)
                .ok_or_else(|| eyre::eyre!("--steps requires a value"))?;
            steps = raw
                .parse()
                .map_err(|e| eyre::eyre!("invalid --steps value '{raw}': {e}"))?;
        }
        i += 1;
    }
    if steps == 0 {
        eyre::bail!("--steps must be >= 1");
    }
    Ok(steps)
}

fn main() -> eyre::Result<()> {
    let steps = parse_steps()?;
    let (mut node, mut events) =
        DoraNode::init_from_env().map_err(|e| eyre::eyre!("trajectory-node: {e}"))?;

    // Current configuration (start: all-zero home position).
    let mut current = [0.0f64; JOINTS];
    // Buffer for the 7 scalars of the incoming target.
    let mut buffer: Vec<f64> = Vec::with_capacity(JOINTS);

    while let Some(event) = events.recv() {
        match event {
            Event::Input { data, .. } => {
                let Some(array) = data.as_array().as_any().downcast_ref::<Float64Array>() else {
                    eprintln!("trajectory-node: expected Float64 input");
                    continue;
                };
                for i in 0..array.len() {
                    buffer.push(array.value(i));
                    if buffer.len() < JOINTS {
                        continue;
                    }
                    // 7 values collected — one full target configuration.
                    let target: [f64; JOINTS] = buffer[..JOINTS]
                        .try_into()
                        .map_err(|_| eyre::eyre!("trajectory-node: buffer error"))?;
                    buffer.clear();

                    // Linear interpolation: `steps` points per move,
                    // including the endpoint (t = 1).
                    for s in 1..=steps {
                        let t = s as f64 / steps as f64;
                        let mut out = [0.0f64; JOINTS];
                        for (j, out_j) in out.iter_mut().enumerate() {
                            *out_j = current[j] + (target[j] - current[j]) * t;
                        }
                        // Emit this step as 7 consecutive scalars.
                        for value in out {
                            let array = Float64Array::from(vec![value]);
                            node.send_output(
                                "trajectory".parse().map_err(|e| {
                                    eyre::eyre!("invalid output_id 'trajectory': {e}")
                                })?,
                                MetadataParameters::default(),
                                array,
                            )
                            .map_err(|e| eyre::eyre!("send_output(trajectory) failed: {e}"))?;
                        }
                    }
                    current = target;
                }
            }
            Event::Stop(_) => break,
            Event::InputClosed { .. } => {}
            _ => {}
        }
    }

    // Linger before exiting so the daemon can deliver every emitted
    // trajectory value to the sink — the same in-flight-message race
    // test-source guards against (see src/source.rs).
    std::thread::sleep(std::time::Duration::from_secs(2));

    Ok(())
}
