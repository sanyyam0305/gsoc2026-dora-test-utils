//! The [`NodeHarness`] — a self-contained unit-test driver for DORA nodes.
//!
//! ## Design
//!
//! `NodeHarness` wraps a DORA node created via
//! [`DoraNode::init_testing()`][init] and drives it with programmatic
//! inputs.  Unlike the file-based `IntegrationTestInput` workflow, this
//! harness works inside a standard `#[test]` function with zero external
//! setup (NOT `#[tokio::test]` — see [`tick`](NodeHarness::tick)).
//!
//! **Inputs** are buffered as [`TimedIncomingEvent`]s and fed to DORA
//! as a batch via [`TestingInput::Input`] when the harness is first
//! driven (`tick` / `run_to_completion` / `send_output`).  This
//! "deferred init" pattern avoids the need for a live runtime channel
//! and eliminates the daemon-thread deadlock that a live channel
//! introduces (the daemon blocks on `next_event()` waiting for input
//! that hasn't been sent yet, while `DoraNode::drop()` waits for the
//! daemon to reply — dora-rs/dora#2855).
//!
//! **Outputs** are captured through [`TestingOutput::ToChannel`].
//!
//! [init]: https://docs.rs/dora-node-api/latest/dora_node_api/struct.DoraNode.html#method.init_testing
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────┐                         ┌──────────────────┐
//! │   Test code      │  buffer events          │  DORA node       │
//! │  send_data()     │ ──────▶ Vec ──────▶     │  (the thing      │
//! │  send_stop()     │         (deferred)       │   under test)    │
//! │  tick()          │ ◀─────────────────────── │                  │
//! │  recv_output() ◀─│── tokio mpsc (output) ───│                  │
//! └──────────────────┘                         └──────────────────┘
//! ```

use std::collections::HashMap;

use dora_node_api::{
    integration_testing::{
        drain_outputs, integration_testing_format::TimedIncomingEvent, output_channel,
        IntegrationTestInput, OutputReceiver, OutputSender, TestingInput, TestingOptions,
        TestingOutput,
    },
    DoraArray, DoraNode, Event, EventStream, NodeError,
};

/// The main unit-test harness for a single DORA node.
///
/// # Example
///
/// ```ignore
/// use dora_test_utils::NodeHarness;
/// use dora_node_api::integration_testing::integration_testing_format::{
///     IncomingEvent, TimedIncomingEvent,
/// };
///
/// #[test]
/// fn test_classifier_node() {
///     let mut harness = NodeHarness::new()
///         .expect("failed to create harness");
///
///     // Buffer an input event (deferred — node not created yet).
///     harness.send_input(TimedIncomingEvent {
///         time_offset_secs: 0.0,
///         event: IncomingEvent::Stop,
///     });
///
///     // First tick: node is created lazily with all buffered events.
///     harness.tick();
///
///     // Assert outputs.
///     let outputs = harness.recv_output("label");
///     assert!(outputs.is_some());
/// }
/// ```
pub struct NodeHarness {
    /// Buffered input events — pushed by `send_data`/`send_stop`/`send_input`,
    /// consumed by [`ensure_init`](Self::ensure_init) when the node is first
    /// driven.
    pending_events: Vec<TimedIncomingEvent>,
    /// Output channel sender.  Created eagerly in [`new`](Self::new) so
    /// `recv_output` works before init; consumed by `ensure_init`.
    output_tx: Option<OutputSender>,
    /// Receiver for outputs captured via [`TestingOutput::ToChannel`].
    output_rx: OutputReceiver,
    /// Buffered outputs indexed by output ID (the `"id"` field in each
    /// JSON output map).
    output_buffers: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    /// The DORA node under test — created lazily on first `tick` /
    /// `run_to_completion` / `send_output`.
    node: Option<DoraNode>,
    /// The event stream — created lazily with the node.
    event_stream: Option<EventStream>,
}

impl NodeHarness {
    /// Create a new harness.
    ///
    /// The DORA node is **not** created immediately.  Events are buffered
    /// until the first call to [`tick`](Self::tick),
    /// [`run_to_completion`](Self::run_to_completion), or
    /// [`send_output`](Self::send_output), at which point the node is
    /// constructed with all buffered events via [`TestingInput::Input`].
    ///
    /// # Errors
    ///
    /// Returns a [`NodeError`] if the output channel cannot be created.
    /// (In practice this never fails: the channel is unbounded.)
    pub fn new() -> Result<Self, NodeError> {
        // Output capture channel, built by dora's own constructor. The halves
        // are opaque newtypes rather than tokio types, so the channel choice
        // stays out of dora's frozen surface (dora-rs/dora#3239).
        let (output_tx, output_rx) = output_channel();

        Ok(Self {
            pending_events: Vec::new(),
            output_tx: Some(output_tx),
            output_rx,
            output_buffers: HashMap::new(),
            node: None,
            event_stream: None,
        })
    }

    /// Inject a synthetic input event.
    ///
    /// The event is **buffered**, not delivered immediately.  The DORA node
    /// is created lazily on the first [`tick`](Self::tick) /
    /// [`run_to_completion`](Self::run_to_completion) /
    /// [`send_output`](Self::send_output) call, at which point all buffered
    /// events are fed at once via [`TestingInput::Input`].
    pub fn send_input(&mut self, event: TimedIncomingEvent) {
        self.pending_events.push(event);
    }

    /// Convenience: inject input data by ID.
    ///
    /// Wraps `data` in a [`TimedIncomingEvent`] and delegates to
    /// [`send_input`](Self::send_input).  The data type must implement
    /// [`IntoInputData`] — currently [`serde_json::Value`] and
    /// [`arrow::array::ArrayData`].
    ///
    /// # Panics
    ///
    /// Panics if `input_id` is not a valid
    /// [`DataId`](dora_node_api::DataId).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // JSON data — the most common case
    /// harness.send_data("image", serde_json::json!({"width": 640}));
    ///
    /// // Arrow data
    /// let array = Int32Array::from(vec![1, 2, 3]).into_data();
    /// harness.send_data("numbers", array);
    /// ```
    pub fn send_data(&mut self, input_id: &str, data: impl crate::IntoInputData) {
        use dora_node_api::integration_testing::integration_testing_format::{
            IncomingEvent, TimedIncomingEvent,
        };

        self.send_input(TimedIncomingEvent {
            time_offset_secs: 0.0,
            event: IncomingEvent::Input {
                id: input_id.parse().unwrap_or_else(|e| {
                    panic!("NodeHarness::send_data: invalid input_id '{input_id}': {e}")
                }),
                metadata: None,
                data: Some(Box::new(data.into_input_data())),
            },
        });
    }

    /// Convenience: inject a [`Stop`] event.
    ///
    /// The event is buffered and delivered when the node is first driven.
    pub fn send_stop(&mut self) {
        self.send_input(TimedIncomingEvent {
            time_offset_secs: 0.0,
            event:
                dora_node_api::integration_testing::integration_testing_format::IncomingEvent::Stop,
        });
    }

    /// Close the input side.
    ///
    /// With the deferred-init model there is no live input channel, so this
    /// is a no-op.  Kept for API compatibility with code that calls
    /// `close_input()` before `send_output()`.
    pub fn close_input(&mut self) {
        // No live channel to close — inputs are buffered and consumed
        // atomically by ensure_init().
    }

    /// Send an output from the node under test.
    ///
    /// Triggers deferred node creation if not already done.  Delegates to
    /// the underlying [`DoraNode::send_output`].  The output is captured by
    /// [`TestingOutput::ToChannel`] and can be retrieved via
    /// [`recv_output`](Self::recv_output).
    ///
    /// # Errors
    ///
    /// Returns a [`NodeError`] if `output_id` is invalid or the underlying
    /// `send_output` call fails.
    pub fn send_output(
        &mut self,
        output_id: &str,
        data: impl arrow::array::Array + 'static,
    ) -> Result<(), NodeError> {
        let data_id = output_id
            .parse()
            .map_err(|e| NodeError::Output(format!("invalid output_id '{output_id}': {e}")))?;

        self.ensure_init();
        self.node
            .as_mut()
            .expect("NodeHarness: node not initialized")
            .send_output(data_id, Default::default(), DoraArray::from_array(data))
    }

    /// Drive the node to process **one** event from the [`EventStream`].
    ///
    /// If the node hasn't been created yet, this triggers deferred
    /// initialization with all buffered events.
    ///
    /// After the event is received, any outputs produced by the node
    /// are collected into the internal buffers (accessible via
    /// [`recv_output`](Self::recv_output)).
    ///
    /// Returns the event that was processed, or `None` if the stream
    /// is exhausted.
    ///
    /// This is a **synchronous** call: `init_testing()` uses
    /// `blocking_recv` internally and cannot run inside a tokio
    /// runtime.  Use `#[test]` (not `#[tokio::test]`) for tests
    /// that drive the harness.
    pub fn tick(&mut self) -> Option<Event> {
        self.ensure_init();

        let stream = self
            .event_stream
            .as_mut()
            .expect("NodeHarness: event_stream not initialized");
        let event = stream.recv();

        // Collect any outputs the node produced during this tick.
        self.collect_pending_outputs();

        event
    }

    /// Drain all available outputs for `output_id` since the last
    /// call to [`tick`](Self::tick) (or since construction).
    ///
    /// Returns `None` if no output with that ID was produced.
    pub fn recv_output<O: Into<String>>(
        &mut self,
        output_id: O,
    ) -> Option<Vec<serde_json::Map<String, serde_json::Value>>> {
        // Collect any straggling outputs before draining.
        self.collect_pending_outputs();
        self.output_buffers.remove(&output_id.into())
    }

    /// Run the node to completion, pumping events until the event stream
    /// is exhausted, a [`Stop`](Event::Stop) is received, or an
    /// [`InputClosed`](Event::InputClosed) arrives.
    ///
    /// A [`Stop`](Event::Stop) is injected automatically, so callers do not
    /// need to pre-load one.  If the caller already buffered a Stop, the
    /// extra one is harmless.
    ///
    /// Returns all events processed during the run, up to and including the
    /// first terminal event.  After this method returns,
    /// [`send_output`](Self::send_output) and
    /// [`recv_output`](Self::recv_output) are safe to use.
    ///
    /// ```ignore
    /// harness.send_input(my_input);
    /// // No need to call send_stop() — run_to_completion handles it.
    /// let events = harness.run_to_completion();
    /// assert!(events.iter().any(|e| matches!(e, Event::Stop(..))));
    ///
    /// // Now safe: daemon is idle, outputs can be sent
    /// harness.send_output("out", my_array).unwrap();
    /// let outputs = harness.recv_output("out");
    /// ```
    pub fn run_to_completion(&mut self) -> Vec<Event> {
        // Only inject Stop if the node hasn't been created yet — after init,
        // buffered events can never reach the node (TestingInput is consumed
        // atomically).  If the node is already initialized, just drain the
        // existing stream.
        let already_init = self.node.is_some();
        if !already_init {
            self.send_stop();
        }
        self.ensure_init();

        let stream = self
            .event_stream
            .as_mut()
            .expect("NodeHarness: event_stream not initialized");
        let mut events = Vec::new();
        loop {
            let event = stream.recv();
            match event {
                Some(ref e) => {
                    let is_stop = matches!(e, Event::Stop(..));
                    let is_input_closed = matches!(e, Event::InputClosed { .. });
                    events.push(event.unwrap());
                    if is_stop || is_input_closed {
                        break;
                    }
                }
                None => break,
            }
        }
        self.collect_pending_outputs();
        events
    }

    // ── private helpers ────────────────────────────────────────────

    /// Create the DORA node if it hasn't been created yet.
    ///
    /// Consumes all buffered events and feeds them as
    /// [`TestingInput::Input`] so that the daemon thread processes them
    /// without ever blocking on a live input channel.
    ///
    /// If the node is already initialized, any buffered events are
    /// silently cleared — they can never be delivered because
    /// `TestingInput` is consumed atomically at init time.
    fn ensure_init(&mut self) {
        if self.node.is_some() {
            // Node already initialized — buffered events can never reach it.
            // Panic instead of dropping silently: a test that injects input
            // after the first tick would otherwise run against LESS data
            // than the author wrote and still report green.
            assert!(
                self.pending_events.is_empty(),
                "NodeHarness: {} event(s) injected after the node was already \
                 initialized — they can never be delivered.  Call \
                 send_data/send_stop BEFORE the first \
                 tick/run_to_completion/send_output.",
                self.pending_events.len()
            );
            self.pending_events.clear();
            return;
        }

        let events = std::mem::take(&mut self.pending_events);
        let input = IntegrationTestInput::new("test-node".parse().unwrap(), events);
        let tx = self
            .output_tx
            .take()
            .expect("NodeHarness: output_tx already consumed");
        let options = TestingOptions {
            skip_output_time_offsets: true,
        };

        let (node, event_stream) = DoraNode::init_testing(
            TestingInput::Input(input),
            TestingOutput::ToChannel(tx),
            options,
        )
        .expect("NodeHarness: DoraNode::init_testing failed");

        self.node = Some(node);
        self.event_stream = Some(event_stream);
    }

    /// Collect all pending outputs from the output channel into
    /// `output_buffers`, indexed by the `"id"` field in each JSON map.
    fn collect_pending_outputs(&mut self) {
        for output in drain_outputs(&mut self.output_rx) {
            if let Some(id) = output.get("id").and_then(|v| v.as_str()) {
                self.output_buffers
                    .entry(id.to_string())
                    .or_default()
                    .push(output);
            } else {
                // Don't silently drop outputs that lack a string "id" field —
                // store them under a sentinel key so the test author can debug.
                self.output_buffers
                    .entry("<missing-id>".to_string())
                    .or_default()
                    .push(output);
            }
        }
    }
}

// No custom Drop needed — with deferred init, the node + event_stream
// are dropped in declaration order, and the daemon thread exits cleanly
// because all events were pre-baked (no live channel to get stuck on).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_data_json() {
        let mut harness = NodeHarness::new().expect("harness should be created");

        harness.send_data("test_id", serde_json::json!([1, 2, 3]));

        // First tick triggers deferred init and returns the buffered Input.
        let event = harness.tick().expect("should receive Input event");
        match event {
            dora_node_api::Event::Input { id, data, .. } => {
                assert_eq!(id.to_string(), "test_id");
                assert!(!data.as_array().is_empty(), "data should be non-empty");
            }
            other => panic!("expected Input event, got {other:?}"),
        }
    }

    #[test]
    fn test_send_data_arrow() {
        use arrow::array::{Array, Int32Array};

        let mut harness = NodeHarness::new().expect("harness should be created");

        let array = Int32Array::from(vec![42, 99]).into_data();
        harness.send_data("arrow_in", array);

        let event = harness.tick().expect("should receive Input event");
        match event {
            dora_node_api::Event::Input { id, data, .. } => {
                assert_eq!(id.to_string(), "arrow_in");
                assert!(!data.as_array().is_empty(), "data should be non-empty");
            }
            other => panic!("expected Input event, got {other:?}"),
        }
    }

    #[test]
    #[should_panic(expected = "injected after the node was already")]
    fn test_post_init_send_data_panics() {
        // After tick() initializes the node, send_data() must panic —
        // the events could never be delivered, and a silent drop would
        // let a test run against less data than the author wrote while
        // still reporting green.
        let mut harness = NodeHarness::new().expect("harness should be created");
        harness.send_data("first", serde_json::json!([1]));
        harness.tick(); // node init happens here
        harness.send_data("second", serde_json::json!([2]));
        harness.tick(); // must panic here
    }

    #[test]
    #[should_panic(expected = "invalid input_id")]
    fn test_send_data_invalid_input_id() {
        let mut harness = NodeHarness::new().expect("harness should be created");
        harness.send_data("not a valid id!!!", serde_json::json!([1]));
    }

    #[test]
    fn test_recv_output_nonexistent() {
        let mut harness = NodeHarness::new().expect("harness should be created");
        harness.send_stop();
        harness.run_to_completion();
        let result = harness.recv_output("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_send_output_invalid_id() {
        let mut harness = NodeHarness::new().expect("harness should be created");
        harness.send_stop();
        harness.run_to_completion();
        let result = harness.send_output("bad id !!!", arrow::array::Int32Array::from(vec![1]));
        assert!(result.is_err());
        assert!(
            format!("{}", result.unwrap_err()).contains("invalid output_id"),
            "error should mention invalid output_id"
        );
    }
}
