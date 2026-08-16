#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# dora-test-utils — Layer 2 Demo: Integration Testing
# ─────────────────────────────────────────────────────────────
# Showcases test-source → node → test-sink end-to-end testing on
# Realman GEN72 robot-arm pipelines — three correct pipelines plus
# one MISCONFIGURED pipeline whose error the test-sink catches:
#   1. echo — a 7-joint configuration (J1..J7) relayed and verified
#   2. multi-echo — joint positions + joint velocities, two sinks
#   3. distance-guard — end-effector proximity safety stop
#   4. distance-guard (misconfigured) — safety distance set too low,
#      the sink reports match:false — the error-catch demonstration
#
# Each pipeline runs as a real dora dataflow; test-sink compares the
# node output against an expected file and the script checks the result.
#
# Run: bash scripts/demo-integration.sh   (from anywhere — the script
# cds to the repo root; requires the dora CLI at dora/target/…/dora)
# ─────────────────────────────────────────────────────────────
set -euo pipefail

# Run from the repo root regardless of the calling directory
cd "$(dirname "$0")/.."

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

banner() {
    echo ""
    echo -e "${CYAN}${BOLD}═══ $1 ═══${NC}"
    echo ""
}

step() {
    echo -e "${GREEN}▶ $1${NC}"
}

warn() {
    echo -e "${RED}⚠ $1${NC}"
}

ok() {
    echo -e "${GREEN}✔ $1${NC}"
}

# ─── 0. Prerequisites ────────────────────────────────────
banner "0. Check prerequisites"

DORA_BIN="dora/target/debug/dora"
if [ ! -f "$DORA_BIN" ]; then
    DORA_BIN="dora/target/release/dora"
fi
if [ ! -f "$DORA_BIN" ]; then
    warn "dora CLI not found at dora/target/…/dora"
    echo "Build it first: PYO3_NO_PYTHON=1 cargo build --bin dora --manifest-path dora/binaries/cli/Cargo.toml"
    exit 1
fi
ok "dora CLI: $DORA_BIN"

# ─── 1. Build ────────────────────────────────────────────
# SKIP_BUILD=1 (set by demo-final.sh, which already built everything)
banner "1. Build binaries"

BUILD_LOG=$(mktemp)
trap "rm -f $BUILD_LOG" EXIT

if [ "${SKIP_BUILD:-0}" = "1" ]; then
    ok "build skipped (orchestrated by demo-final.sh)"
else
    step "Build test-source, test-sink, echo-node, distance-guard..."
    if cargo build --bin test-source --bin test-sink --bin echo-node --bin distance-guard > "$BUILD_LOG" 2>&1; then
        tail -1 "$BUILD_LOG"
    else
        warn "Build failed! Last 20 lines:"
        tail -20 "$BUILD_LOG"
        exit 1
    fi
fi

# ─── 2. Pipelines ────────────────────────────────────────
banner "2. GEN72 integration pipelines (test-source → node → test-sink)"

# Run each pipeline and check the test-sink comparison result.
# Fixture args are relative to the YAML's directory (dora spawns nodes
# there), so the static files work as-is from the repo root.
run_pipeline() {
    local yaml="$1"
    shift
    local result_files=("$@")

    # Remove stale result files first — a leftover "match": true from a
    # previous run would otherwise satisfy the check even if this run's
    # sink wrote nothing.
    rm -f "${result_files[@]}"

    step "Running $yaml ..."
    set +e
    timeout 60 "$DORA_BIN" run "$yaml" --stop-after 15s > "$BUILD_LOG" 2>&1
    local dora_exit=$?
    set -e
    if [ $dora_exit -ne 0 ]; then
        warn "dora run failed (exit $dora_exit). Last 10 log lines:"
        tail -10 "$BUILD_LOG"
        exit 1
    fi

    for rf in "${result_files[@]}"; do
        if [ -f "$rf" ] && grep -q '"match": true' "$rf"; then
            ok "$rf — MATCH (test-sink compared against expected file)"
        else
            warn "$rf — MISMATCH or missing:"
            cat "$rf" 2>/dev/null || echo "(file not found)"
            exit 1
        fi
    done
}

# The error-catch pipeline: the node was MISconfigured (safety distance
# too low), so the correct expected file must produce match:false —
# the sink caught the error, which is the point of the demo.
run_pipeline_expect_mismatch() {
    local yaml="$1"
    local result_file="$2"

    rm -f "$result_file"

    step "Running $yaml ..."
    set +e
    timeout 60 "$DORA_BIN" run "$yaml" --stop-after 15s > "$BUILD_LOG" 2>&1
    local dora_exit=$?
    set -e
    # Exit 1 is EXPECTED here: test-sink bails with non-zero status
    # when it finds differences.  The result file is the authority;
    # only fail on real errors (timeout 124, spawn failures, ...).
    if [ $dora_exit -ne 0 ] && [ $dora_exit -ne 1 ]; then
        warn "dora run failed (exit $dora_exit). Last 10 log lines:"
        tail -10 "$BUILD_LOG"
        exit 1
    fi

    if [ -f "$result_file" ] && grep -q '"match": false' "$result_file"; then
        echo -e "  ${RED}${BOLD}❌ $result_file — MISMATCH as expected: the misconfiguration was caught${NC}"
        echo "    differences (excerpt):"
        grep -o '"message": "[^"]*"' "$result_file" | head -3 | sed 's/^/      /'
    else
        warn "$result_file — expected MISMATCH but got:"
        cat "$result_file" 2>/dev/null || echo "(file not found)"
        exit 1
    fi
}

step "Pipeline 1/4: echo — GEN72 7-joint configuration (J1..J7) relayed"
run_pipeline "tests/fixtures/echo-dataflow.yml" "tests/fixtures/result.json"

step "Pipeline 2/4: multi-echo — GEN72 joint positions + joint velocities, two sinks"
run_pipeline "tests/fixtures/multi-echo-dataflow.yml" \
    "tests/fixtures/result-a.json" "tests/fixtures/result-b.json"

step "Pipeline 3/4: distance-guard — GEN72 end-effector proximity stop (0.15 m inside the safety radius)"
run_pipeline "tests/fixtures/distance-guard-dataflow.yml" \
    "tests/fixtures/result-distance.json"

step "Pipeline 4/4: distance-guard MISCONFIGURED — safety distance 0.5 → 0.1 m (error-catch demo)"
run_pipeline_expect_mismatch "tests/fixtures/distance-guard-misconfigured.yml" \
    "tests/fixtures/result-distance-misconfigured.json"

# ─── Done ────────────────────────────────────────────────
banner "Layer 2 Demo Complete"

echo -e "${GREEN}${BOLD}Summary:${NC}"
echo "  • echo:        7-joint configuration relayed — 7/7 values match"
echo "  • multi-echo:  joint positions + joint velocities on two outputs"
echo "  • distance-guard: 0.15 m reading triggered the safety stop"
echo "  • misconfigured distance-guard: safety distance 0.5→0.1 m — CAUGHT as match:false"
echo ""
echo "  Layer 1 (unit):      cargo run --example harness_demo"
echo "  Layer 3 (regression): cargo run --example demo_replay"
echo "  All layers + tests:  bash scripts/demo-final.sh"
