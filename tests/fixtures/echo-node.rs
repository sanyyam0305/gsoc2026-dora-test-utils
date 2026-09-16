//! Echo node — receives Input events and sends them back as Output
//! events verbatim.  Used as a pass-through in integration test
//! dataflows so that the pipeline `test-source → echo → test-sink`
//! exercises the full DORA routing machinery.

use dora_node_api::{DoraNode, Event, MetadataParameters};

fn main() -> eyre::Result<()> {
    let (mut node, mut events) = DoraNode::init_from_env()
        .map_err(|e| eyre::eyre!("echo-node: failed to init DORA node: {e}"))?;

    while let Some(event) = events.recv() {
        match event {
            Event::Input { id, data, .. } => {
                node.send_output(id, MetadataParameters::default(), data)
                    .map_err(|e| eyre::eyre!("echo-node: send_output failed: {e}"))?;
            }
            Event::Stop(_) => break,
            // InputClosed means one source has closed — don't break,
            // because there may still be buffered Input events from
            // that source in the pipeline.  Only Stop ends the loop.
            // `dora run --stop-after` guarantees Stop is always sent;
            // in production dataflows the coordinator sends Stop on
            // shutdown, so an infinite loop is not reachable.
            Event::InputClosed { .. } => {}
            _ => {}
        }
    }

    Ok(())
}
