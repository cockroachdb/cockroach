#!/usr/bin/env bash
#
# Run a single concurrency=1 sub-benchmark of BenchmarkPointSelect with
# CPU and allocation profiling enabled, and stash the resulting
# cpu.prof / mem.prof under results/profile-<timestamp>/.
#
# Why a separate script:
#   * Profiling perturbs the timing. The latency metrics emitted by a
#     profiled run are NOT comparable to those from run-baseline.sh —
#     CPU profile sampling alone usually adds 2-5% overhead, and at
#     this query latency that's well above noise.
#   * We profile only at conc=1 so we are measuring single-op cost,
#     not queueing under saturation.
#   * benchtime is bumped to 30s so the timed portion of the test
#     dominates the cluster-startup work in the profile.
#
# Output layout:
#   results/
#     profile-20260420-200000/
#       raw.txt    - bench stdout/stderr (latency numbers are SUSPECT)
#       env.txt    - git SHA, settings, etc.
#       cpu.prof   - sampled CPU profile of the test binary
#       mem.prof   - heap profile sampled at end of run
#
# Inspect with:
#   go tool pprof -http=:8080 results/profile-<ts>/cpu.prof
#   go tool pprof -top results/profile-<ts>/cpu.prof | head -30

set -euo pipefail

# See run-baseline.sh — skips dev's doctor-status check so a freshly
# synced remote worktree (where git is broken) doesn't trip it.
export AUTOMATION="${AUTOMATION:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BENCH_TIME="${BENCH_TIME:-30s}"
TEST_CPU="${TEST_CPU:-8}"
# Default: run only conc=1. Override to profile a saturated regime.
BENCH_FILTER="${BENCH_FILTER:-^BenchmarkPointSelect/conc=1$}"
# If POINT_SELECT_FAST_PATH=1, the profile run uses the variant
# (sql.fast_path.point_select.enabled=true). The result-dir tag and
# the env.txt line both reflect this so it stays clear which run is
# which when comparing later.
fp_tag=""
fp_env=""
if [[ "${POINT_SELECT_FAST_PATH:-0}" == "1" ]]; then
  fp_tag="fastpath-"
  fp_env="-- --test_env=POINT_SELECT_FAST_PATH=1"
fi

ts="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${RESULTS_DIR}/profile-${fp_tag}${ts}"
mkdir -p "${RUN_DIR}"

{
  echo "timestamp:    ${ts}"
  echo "kind:         profile (latency numbers are perturbed)"
  if [[ -n "${fp_tag}" ]]; then
    echo "fast_path:    on"
  else
    echo "fast_path:    off"
  fi
  echo "host:         $(hostname)"
  echo "uname:        $(uname -a)"
  # See note in run-baseline.sh — git may not work on a remote host.
  if git -C "${REPO_ROOT}" rev-parse HEAD &>/dev/null; then
    echo "git_sha:      $(git -C "${REPO_ROOT}" rev-parse HEAD)"
    echo "git_status:"
    git -C "${REPO_ROOT}" status --porcelain | sed 's/^/  /'
  elif [[ -f "${REPO_ROOT}/.local-git-info" ]]; then
    cat "${REPO_ROOT}/.local-git-info"
  else
    echo "git_sha:      (unknown — git unavailable and no .local-git-info snapshot)"
  fi
  echo "bench_time:   ${BENCH_TIME}"
  echo "test_cpu:     ${TEST_CPU} (GOMAXPROCS for in-process server)"
  echo "bench_filter: ${BENCH_FILTER}"
  echo "nodes:        ${POINT_SELECT_NODES:-1} (POINT_SELECT_NODES)"
  echo "splits:       ${POINT_SELECT_SPLITS:-(default)} (POINT_SELECT_SPLITS)"
} > "${RUN_DIR}/env.txt"

cd "${REPO_ROOT}"

# Bazel passes -test.outputdir=<workspace_root>, so relative profile
# paths land in the workspace root. We move them into the run dir
# afterwards.
CPU_OUT="${REPO_ROOT}/cpu.prof"
MEM_OUT="${REPO_ROOT}/mem.prof"
MUTEX_OUT="${REPO_ROOT}/mutex.prof"
BLOCK_OUT="${REPO_ROOT}/block.prof"
rm -f "${CPU_OUT}" "${MEM_OUT}" "${MUTEX_OUT}" "${BLOCK_OUT}"

# Mutex and block profiles answer "where are goroutines getting stuck
# waiting on each other?" — exactly the question raised by the htop
# pattern of pinned-and-idle cores at high concurrency. The Go test
# framework auto-enables mutex sampling (fraction 1) and block
# sampling (rate 1ns) when these flags are passed.
./dev bench pkg/sql/tests:tests_test \
  --filter="${BENCH_FILTER}" \
  --bench-time="${BENCH_TIME}" \
  --count=1 \
  --bench-mem=false \
  --test-args="-test.cpu ${TEST_CPU} -test.cpuprofile=cpu.prof -test.memprofile=mem.prof -test.mutexprofile=mutex.prof -test.blockprofile=block.prof" \
  ${fp_env} \
  2>&1 | tee "${RUN_DIR}/raw.txt"

for pair in "${CPU_OUT}:cpu.prof" "${MEM_OUT}:mem.prof" "${MUTEX_OUT}:mutex.prof" "${BLOCK_OUT}:block.prof"; do
  src="${pair%%:*}"; name="${pair##*:}"
  if [[ -f "${src}" ]]; then
    mv "${src}" "${RUN_DIR}/${name}"
  else
    echo "WARNING: ${src} not found — ${name} likely was not written" >&2
  fi
done

echo
echo "==> Results in: ${RUN_DIR}"
echo "==> Inspect:"
echo "      go tool pprof -http=:8080 ${RUN_DIR}/cpu.prof"
echo "      go tool pprof -top ${RUN_DIR}/mutex.prof | head -30"
echo "      go tool pprof -top ${RUN_DIR}/block.prof | head -30"
