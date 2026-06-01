#!/usr/bin/env bash
#
# Re-run the point-select baseline benchmark with pinned settings, save
# raw output and a small env file under results/<timestamp>/, and refresh
# the `results/latest` symlink. Idempotent and re-runnable; takes no
# flags.
#
# Override pinned settings via env vars if you need to:
#   BENCH_TIME=5s   BENCH_COUNT=3   TEST_CPU=4   ./run-baseline.sh
#
# Output layout:
#   results/
#     20260420-194744/
#       raw.txt   - stdout+stderr from the bench run
#       env.txt   - git SHA, hostname, GOMAXPROCS for the sweep, etc.
#     latest -> 20260420-194744/

set -euo pipefail

# AUTOMATION=1 tells `dev` to skip its doctor-status check (see
# pkg/cmd/dev/doctor.go). On a fresh remote checkout the doctor
# wants to run `git submodule update`, which fails when the worktree's
# .git is a pointer to a path that only exists on the local host.
# Setting this is benign locally (doctor would normally pass anyway).
export AUTOMATION="${AUTOMATION:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Pinned config — change these at the top of a run if iterating, but
# please commit them when sharing results so reruns match.
BENCH_TIME="${BENCH_TIME:-10s}"
BENCH_COUNT="${BENCH_COUNT:-5}"
TEST_CPU="${TEST_CPU:-8}"
BENCH_FILTER="${BENCH_FILTER:-^BenchmarkPointSelect$}"

ts="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${RESULTS_DIR}/${ts}"
mkdir -p "${RUN_DIR}"

# Capture environment so a reader can tell what produced the numbers.
{
  echo "timestamp:    ${ts}"
  echo "host:         $(hostname)"
  echo "uname:        $(uname -a)"
  # When running on a remote (gceworker), the local .git is a worktree
  # pointer to a directory that doesn't exist on the remote, so git
  # commands fail. Fall back to .local-git-info if our wrapper dropped
  # one alongside the worktree, otherwise mark as unknown so we don't
  # silently lose provenance. Either way, never abort the run on git.
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
} > "${RUN_DIR}/env.txt"

cd "${REPO_ROOT}"

# Note: ./dev bench defaults to GOMAXPROCS=1; we override via -test.cpu.
# --bench-mem=false because allocation tracking distorts the latency
# numbers we care about and we are not currently measuring allocs.
./dev bench pkg/sql/tests:tests_test \
  --filter="${BENCH_FILTER}" \
  --bench-time="${BENCH_TIME}" \
  --count="${BENCH_COUNT}" \
  --bench-mem=false \
  --test-args="-test.cpu ${TEST_CPU}" \
  2>&1 | tee "${RUN_DIR}/raw.txt"

# Refresh the latest pointer.
ln -snf "${ts}" "${RESULTS_DIR}/latest"

echo
echo "==> Results in: ${RUN_DIR}"
echo "==> Symlink:    ${RESULTS_DIR}/latest"
