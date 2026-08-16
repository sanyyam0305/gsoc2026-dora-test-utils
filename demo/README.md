# demo/ — Layer 3: Record/Replay Regression Testing Demo

This directory holds the dataflow files the regression demo runs.
Scenario: **GEN72 joint-space motion control** — a trajectory node
linearly interpolates the 7 joints toward target configurations; the
demo records the resulting trajectory, then replays a dataflow whose
interpolation resolution was changed and detects the regression.

## The pipeline

```
test-source ──target──▶ trajectory-node ──trajectory──▶ test-sink (record mode)
(2 targets,            (linear interpolation,          sink_trajectory.json
 7 joints each)         --steps N per move)
```

| Node | What it does |
|------|--------------|
| `test-source` | Sends two target configurations (7 consecutive Float64 scalars each, degrees): a pose `[30, -35, 109, -17, 46, 66, 0]` and another `[45, -20, 90, 0, 60, 45, 10]`. |
| `trajectory-node` | On every 7th received value it interpolates all 7 joints from the current pose to the target in `--steps` linear steps and emits each step as 7 scalars. Pure computation — fully deterministic. |
| `test-sink` | Records every trajectory value to `sink_trajectory.json` (record mode). |

## How the test works

Record once, replay always, compare:

1. **Record** (`demo_replay.rs` Step 1) — run `demo/trajectory-baseline.yml`
   (`--steps 10`): 2 targets × 10 steps × 7 joints = **140 trajectory
   values**. Saved as the baseline ("known-good trajectory").
2. **Clean replay** (Step 2) — run the same dataflow again and compare.
   Identical → `is_clean() = true`, no regression. The output is pure
   computation, so no filtering is needed here. Real pipelines with
   timing noise use `ignore_paths` / `ignore_sink` to skip
   non-deterministic fields (see README and tests).
3. **Mutate** (Step 3) — switch to `demo/trajectory-mutated.yml`, which
   changes only `trajectory-node`'s `--steps 10` to `--steps 5`. This is
   a real motion-control regression: someone edited the interpolation
   resolution and every trajectory point changed.
4. **Regression detected** (Step 4) — replay the mutated dataflow:
   140 → 70 values, every shared point differs. `is_clean() = false`,
   the DiffReport shows `.count`, `data.length`, and per-point value
   diffs, and `assert_no_regression()` panics.

This is exactly how you would use the tool on your own dataflow: keep a
static YAML, record a baseline once, replay it in CI whenever the code
or configuration changes.

## Files

| File | Role |
|------|------|
| `trajectory-baseline.yml` | **Baseline** — run in Steps 1-2 (`--steps 10`) |
| `trajectory-mutated.yml` | **Mutated** — run in Steps 3-4 (`--steps 5`) |
| `trajectory-targets.json` | The two target configurations fed by test-source |
| `sink_trajectory.json` | Recorded trajectory (generated at runtime, git-ignored) |
| `trajectory-baseline.json` | **The committed baseline** — the demo records it here (repo-relative paths, 140 values) and CI replays against it; re-recording refreshes the timestamp metadata |
| `rust-dataflow.yml` / `rust-dataflow-mutated.yml` | **Bonus example** — the same Record/Replay pattern on DORA's official rust-dataflow example (unmodified upstream nodes); see the notes in those files |

Relative paths in the YAMLs resolve against this directory (dora
behavior), so all files work as-is from the repo root:

```bash
dora run demo/trajectory-baseline.yml --stop-after 10s
```

## Run the full demo

```bash
./scripts/demo-final.sh
```

This clones/pins dora, builds everything, runs the three-layer demo
(Layer 1 harness_demo → Layer 2 integration pipelines → Layer 3
Record/Replay above), then runs the full test suite.
