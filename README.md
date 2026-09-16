# dora-test-utils

> **DORA version**: built against the released **dora 1.0.1** (`dora-node-api = { version = "1.0.1", features = ["arrow-v59"] }`, arrow 59). The `arrow-v59` feature is required: it exposes the `DoraArray` accessors and the `IntoArrow` impls for Arrow 59 arrays. Use a matching `dora-cli` (`cargo install dora-cli --locked --version 1.0.1`) so the CLI and the library API cannot drift apart.
>
> 中文版文档: [README-CN.md](README-CN.md)

A Rust testing utility library for the [DORA](https://dora-rs.ai/) dataflow framework. DORA developers can test their nodes the way they test ordinary Rust code: **unit-test a single node without a daemon, integration-test a whole pipeline without modifying the node under test, and regression-test by recording a baseline once and comparing on every future run**.

## How the three testing layers work

```
┌──────────────────────────────────────────────────┐
│  Layer 1: NodeHarness — unit testing             │
│  Drive a single node in #[test], no daemon        │
├──────────────────────────────────────────────────┤
│  Layer 2: TestSource / TestSink — integration    │
│  Drop into a real YAML dataflow, end-to-end      │
├──────────────────────────────────────────────────┤
│  Layer 3: Record / Replay — regression testing   │
│  Record a real run once → replay and compare     │
└──────────────────────────────────────────────────┘
```

### Layer 1: Unit testing (NodeHarness)

**How it works**: NodeHarness constructs a real DORA node inside the test process and feeds it pre-baked events — no daemon, no YAML, no environment variables. The test injects inputs, drives the node's event loop step by step, and asserts on outputs captured from an in-memory channel.

The recommended structure extracts the node's business logic into library functions: **write the logic once**, assert on it directly in tests (cheap, exhaustive over edge cases), then drive the event loop through the harness to verify the wiring (payload parsing, triggering, emitting). The two are complementary — one tests whether it *thinks* correctly, the other whether it's *wired* correctly.

```rust
let mut harness = NodeHarness::new()?;
harness.send_data("image", serde_json::json!([1, 2, 3]));  // inject inputs
while let Some(event) = harness.tick() { /* node logic handles the event */ }
let outputs = harness.recv_output("result");               // capture outputs
assert!(outputs.is_some());
```

### Layer 2: Integration testing (TestSource / TestSink)

**How it works**: without touching the node under test, add two ready-made nodes to the YAML — a feeder at the input (`test-source`, reads a JSON file and emits each value) and an inspector at the output (`test-sink`, compares received data against an expected file and writes a `match: true/false` result). The whole pipeline runs as a real `dora run`; CI checks the `match` in the result file.

Ready-made node binaries:

| Binary | Purpose |
|--------|---------|
| `test-source` | Reads a JSON file and emits the data on DORA outputs (multi-output supported) |
| `test-sink` | Receives data, compares against an expected file, writes the match result |
| `echo-node` | Pass-through, verifies link connectivity |
| `classifier-node` | Splits values by a threshold onto two outputs |
| `distance-guard` | Example node: emits a stop flag when a distance is below the safety threshold (`--safety-distance`, default 0.5 m) |
| `trajectory-node` | Example node: linear 7-joint trajectory interpolation (`--steps` resolution) — the node under test in the Layer 3 demo |

Two comparison modes: **semantic** (default — Arrow-based value comparison, tolerates type differences such as Int32 vs Int64) and **strict** (exact JSON equality).

### Layer 3: Regression testing (RecordSession / ReplaySession)

**How it works**: take a "known-good photo" of a pipeline — on the first run, record what each inspector received as a baseline (with the dataflow path, dora version, timeout, etc.); on later runs, replay and compare automatically. `is_clean()` tells you whether anything regressed, and the `DiffReport` shows which sink, which field, and what changed.

Real pipelines always have non-deterministic noise (tick counts, timestamps, debug logs) — declare it with two filter primitives:

```rust
ReplaySession::load("baseline.json")?
    .replay_sink("test-sink", "out.json")
    .ignore_paths(&["count", "timestamp"])  // skip these fields (descendants too, e.g. data[0])
    .ignore_sink("debug-log")               // skip a whole sink
    .run()?
    .assert_no_regression();                // panics on regression → CI goes red
```

Comparison has two layers: a fast JSON structural diff plus Arrow semantic comparison (type-tolerant). `DiffReport` distinguishes `Match` / `Mismatch` / `Missing` / `Extra`, with per-field paths and before/after values.

## Supported data types

- **Integers**: Int8/16/32/64, UInt8/16/32/64 (overflow-checked; values above 2^53 compare exactly)
- **Floats**: Float32/64
- **Strings**: String, LargeString
- **Other**: Boolean, Null, arrays, objects (Struct)

Injection and comparison use the Arrow wire format throughout; JSON inputs declare the target type via the `data_type` field.

## Runnable demos

All demos share one theme — a **Realman GEN72 7-axis robot arm** — and each layer runs independently:

| Demo | Entry point | What it shows |
|------|-------------|---------------|
| Unit testing | `cargo run --example harness_demo` | GEN72 joint-limit monitor: Part A asserts the logic directly (boundary cases incl. the asymmetric J4/J6 limits) + Part B drives the event loop through the harness + **Part C plants a deliberate bug and shows the test catching it** |
| Integration testing | `bash scripts/demo-integration.sh` | Four real pipelines: 7-joint configuration relay (echo), joint positions + velocities on two outputs (multi-echo), end-effector proximity stop (distance-guard), and a **misconfigured distance-guard (safety distance set too low — caught as match:false)** |
| Regression testing | `cargo run --example demo_replay` | Trajectory-interpolation node records a baseline (140 trajectory values) → interpolation resolution changed 10→5 steps (a real motion-control regression) → 67 differences detected |
| **All in one** | `bash scripts/demo-final.sh` | All three layers + the full test suite; installs the matching `dora-cli` automatically |

`demo/rust-dataflow.yml` additionally keeps a reference example of the tool applied to DORA's official rust-dataflow example with zero modifications.

## Quick start

**Prerequisites**: Rust toolchain; the dora CLI for integration/regression tests (demo-final.sh clones and builds it automatically).

```bash
# Build all binaries
cargo build --bin test-source --bin test-sink --bin echo-node \
    --bin classifier-node --bin distance-guard --bin trajectory-node

# Run the tests (122 total: 91 unit + 5 e2e + 4 record + 13 replay + 6 integration + 3 smoke)
cargo test --lib                          # unit tests
cargo test --test e2e_replay -- --test-threads=1   # replay e2e (needs the dora CLI)
bash scripts/demo-final.sh                # one command: all demos + all tests
```

## Project layout

```
src/               # library: harness (unit-test driver), source/sink (injection &
                   #   comparison), record (record/replay/diff report), mock (daemon-free
                   #   mocks), bin/ (test-source, test-sink, echo, classifier,
                   #   distance-guard, trajectory-node, ...)
tests/             # fixtures (static YAMLs + data files, dora-runnable as-is) + test suites
examples/          # harness_demo.rs (Layer 1), demo_replay.rs (Layer 3)
demo/              # Layer 3 dataflow YAMLs and data files
scripts/           # demo-integration.sh (Layer 2), demo-final.sh (orchestrator)
docs/              # design docs, progress log
```

## API stability

| API | Status |
|-----|--------|
| `NodeHarness`, `TestSource`/`TestSink`, `MockEventStream`/`MockOutputSender`, `IntoInputData` | **Stable** |
| `RecordSession`/`Recording`, `ReplaySession`/`ReplayResult`, `DiffReport` family | **Experimental** |

## CI

`.github/workflows/ci.yml` runs five jobs: `check`, `test` (lib + e2e + smoke), `clippy`, `fmt`, and `integration-test` (installs `dora-cli` from crates.io, runs integration + record/replay e2e serially, 30-minute cap).

## Progress

Weeks 1-11 complete (API design, NodeHarness, TestSource/TestSink, CI, Record/Replay, DORA upgrade); weeks 12-13 polished the demos (three-layer GEN72 theme + two code-review rounds). See [`docs/PROGRESS.md`](docs/PROGRESS.md).

## License

GSoC 2026 project; will ultimately be merged into [dora-rs/dora](https://github.com/dora-rs/dora).
