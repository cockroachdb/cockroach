#!/usr/bin/env bash
#
# Same concurrency sweep as run-baseline.sh, but with the experimental
# point-select fast path enabled (POINT_SELECT_FAST_PATH=1, which the
# benchmark turns into a cluster-setting override at server start).
#
# Output goes to results/fastpath-<timestamp>/ so it is easy to
# distinguish from baseline runs. Compare two runs with:
#
#   summarize.sh results/<baseline-ts> results/fastpath-<ts>
#
# Override pinned settings via env vars if you need to:
#   BENCH_TIME=5s   BENCH_COUNT=3   TEST_CPU=4   ./run-fastpath.sh

set -euo pipefail

# See run-baseline.sh — skips dev's doctor-status check so a freshly
# synced remote worktree (where git is broken) doesn't trip it.
export AUTOMATION="${AUTOMATION:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BENCH_TIME="${BENCH_TIME:-10s}"
BENCH_COUNT="${BENCH_COUNT:-5}"
TEST_CPU="${TEST_CPU:-8}"
BENCH_FILTER="${BENCH_FILTER:-^BenchmarkPointSelect$}"

ts="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${RESULTS_DIR}/fastpath-${ts}"
mkdir -p "${RUN_DIR}"

{
  echo "timestamp:    ${ts}"
  echo "kind:         fastpath (sql.fast_path.point_select.enabled = true)"
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
  echo "bench_count:  ${BENCH_COUNT}"
  echo "test_cpu:     ${TEST_CPU} (GOMAXPROCS for in-process server)"
  echo "bench_filter: ${BENCH_FILTER}"
  echo "nodes:        ${POINT_SELECT_NODES:-1} (POINT_SELECT_NODES)"
  echo "splits:       ${POINT_SELECT_SPLITS:-(default)} (POINT_SELECT_SPLITS)"
  echo "placeholder_fast_path: ${POINT_SELECT_PLACEHOLDER_FAST_PATH:-off} (POINT_SELECT_PLACEHOLDER_FAST_PATH)"
  echo "fast_path:    on"
} > "${RUN_DIR}/env.txt"

cd "${REPO_ROOT}"

# Pass POINT_SELECT_FAST_PATH=1 through to the test process via bazel's
# --test_env. The benchmark reads this at server-startup time and
# overrides the corresponding cluster setting.
./dev bench pkg/sql/tests:tests_test \
  --filter="${BENCH_FILTER}" \
  --bench-time="${BENCH_TIME}" \
  --count="${BENCH_COUNT}" \
  --bench-mem=false \
  --test-args="-test.cpu ${TEST_CPU}" \
  -- --test_env=POINT_SELECT_FAST_PATH=1 \
  2>&1 | tee "${RUN_DIR}/raw.txt"

ln -snf "fastpath-${ts}" "${RESULTS_DIR}/latest-fastpath"

echo
echo "==> Results in: ${RUN_DIR}"
echo "==> Symlink:    ${RESULTS_DIR}/latest-fastpath"
echo
echo "Compare with the most recent baseline:"
latest_baseline="$(ls -td "${RESULTS_DIR}"/[0-9]* 2>/dev/null | head -1 || true)"
if [[ -n "${latest_baseline}" ]]; then
  echo "  ${SCRIPT_DIR}/summarize.sh ${latest_baseline} ${RUN_DIR}"
else
  echo "  (no baseline found in ${RESULTS_DIR})"
fi
