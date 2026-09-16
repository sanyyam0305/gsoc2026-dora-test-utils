//! Classifier node — receives Int64 values and classifies each as
//! "high" (above threshold, default 50) or "low".
//!
//! Used in integration tests with the dora-test-utils framework.

use arrow::array::{Array, Int64Array};
use dora_node_api::{DoraNode, Event, MetadataParameters};

fn main() -> eyre::Result<()> {
    let (mut node, mut events) =
        DoraNode::init_from_env().map_err(|e| eyre::eyre!("classifier-node: {e}"))?;

    let threshold: i64 = std::env::var("CLASSIFIER_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    while let Some(event) = events.recv() {
        match event {
            Event::Input { id: _id, data, .. } => {
                // Downcast to Int64Array
                let Some(array) = data.as_array().as_any().downcast_ref::<Int64Array>() else {
                    eprintln!("classifier: expected Int64 input");
                    continue;
                };
                for i in 0..array.len() {
                    let val = array.value(i);
                    let label = if val > threshold { "high" } else { "low" };
                    // Build array from single value
                    let mut builder = Int64Array::builder(1);
                    builder.append_value(val);
                    let output = builder.finish();
                    node.send_output(
                        label
                            .parse()
                            .map_err(|e| eyre::eyre!("invalid output_id '{label}': {e}"))?,
                        MetadataParameters::default(),
                        output,
                    )
                    .map_err(|e| eyre::eyre!("send_output({label}) failed: {e}"))?;
                }
            }
            Event::Stop(_) => break,
            Event::InputClosed { .. } => {}
            _ => {}
        }
    }
    Ok(())
}
