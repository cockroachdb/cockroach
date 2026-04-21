#!/usr/bin/env bash
#
# Runs the point-select-fast-path experiment on the user's gceworker so
# we can re-baseline on Linux (where syscalls are cheap) and validate
# that the fast path still wins.
#
# This wrapper does NOT touch the gceworker's main cockroach checkout
# at ~/go/src/github.com/cockroachdb/cockroach. It rsyncs the local
# worktree to a sibling directory and runs everything from there.
#
# Subcommands:
#
#   sync                Rsync the local worktree to the gceworker, into
#                       ~/cockroach-fastpath-experiment/. Incremental
#                       on subsequent runs.
#
#   run <script> [...]  Ssh to the gceworker and execute one of the
#                       experiment scripts (run-baseline.sh,
#                       run-fastpath.sh, run-profile.sh) with any
#                       additional arguments forwarded.
#
#   fetch [<run-dir>]   Rsync results back to the local results/
#                       directory. With no argument, pulls all results
#                       directories that don't already exist locally.
#                       With an argument, pulls just that one directory.
#
#   ssh                 Open an interactive shell on the gceworker, in
#                       the experiment directory.
#
#   status              Show gceworker state and whether the remote
#                       worktree exists.
#
# Examples:
#
#   ./run-on-gceworker.sh sync
#   ./run-on-gceworker.sh run run-baseline.sh
#   ./run-on-gceworker.sh run run-fastpath.sh
#   ./run-on-gceworker.sh fetch
#   ./run-on-gceworker.sh ssh
#
# Requires: gceworker provisioned via scripts/gceworker.sh create. The
# gceworker must be running (or this script will start it).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Resolve the gceworker name the same way scripts/gceworker.sh does.
USER_ID="$(id -un)"
NAME="${GCEWORKER_NAME-gceworker-${USER_ID//./}}"
REMOTE_DIR="${GCEWORKER_REMOTE_DIR-cockroach-fastpath-experiment}"
GCEWORKER="${REPO_ROOT}/scripts/gceworker.sh"

if [[ ! -x "${GCEWORKER}" ]]; then
  echo "error: scripts/gceworker.sh not found or not executable" >&2
  exit 1
fi

# Resolve the gceworker's project + zone. Order of precedence:
#   1. GCEWORKER_PROJECT / GCEWORKER_ZONE env vars (matches gceworker.sh).
#   2. The first matching `Host gceworker-${NAME}.${zone}.${project}` line
#      in ~/.ssh/config (written by `gceworker.sh start` and the most
#      authoritative source on what the user actually has provisioned).
#   3. gceworker.sh's own defaults (cockroach-workers / us-east1-b).
# Discovered values are exported as CLOUDSDK_* so all later gcloud calls
# in this script (and in any sub-process) target the right instance.
detect_zone_project_from_ssh_config() {
  # Prints "<zone> <project>" for the first matching Host line, or
  # nothing if no matching entry exists.
  local line fqn rest
  line=$(grep -E "^Host ${NAME}\." "${HOME}/.ssh/config" 2>/dev/null | head -1) || return 0
  [[ -z "${line}" ]] && return 0
  fqn="${line#Host }"        # gceworker-matt.us-west1-b.cockroach-workers
  rest="${fqn#${NAME}.}"     # us-west1-b.cockroach-workers
  echo "${rest%.*} ${rest##*.}"
}

if [[ -z "${GCEWORKER_PROJECT:-}" || -z "${GCEWORKER_ZONE:-}" ]]; then
  read -r detected_zone detected_project <<<"$(detect_zone_project_from_ssh_config)"
fi
GCEWORKER_PROJECT="${GCEWORKER_PROJECT:-${detected_project:-cockroach-workers}}"
GCEWORKER_ZONE="${GCEWORKER_ZONE:-${detected_zone:-us-east1-b}}"
export CLOUDSDK_CORE_PROJECT="${GCEWORKER_PROJECT}"
export CLOUDSDK_COMPUTE_ZONE="${GCEWORKER_ZONE}"

# fqname returns the fully qualified ssh alias that scripts/gceworker.sh
# writes into ~/.ssh/config when the worker is started. Built from the
# resolved project + zone above so it always matches what's in the
# config file.
fqname() {
  echo "${NAME}.${GCEWORKER_ZONE}.${GCEWORKER_PROJECT}"
}

# ensure_running starts the VM if it isn't already up. Quiet on success.
ensure_running() {
  local state
  state="$(gcloud compute instances describe "${NAME}" --format='value(status)' 2>/dev/null || echo "UNKNOWN")"
  case "${state}" in
    RUNNING) ;;
    TERMINATED|STOPPED|SUSPENDED)
      echo "==> gceworker is ${state}, starting..."
      "${GCEWORKER}" start >/dev/null
      ;;
    UNKNOWN)
      echo "error: cannot find gceworker named '${NAME}' in project=${GCEWORKER_PROJECT} zone=${GCEWORKER_ZONE}." >&2
      echo "       If the instance exists in a different project/zone, set GCEWORKER_PROJECT and/or GCEWORKER_ZONE." >&2
      echo "       If it doesn't exist yet, run 'scripts/gceworker.sh create'." >&2
      exit 1
      ;;
    *)
      echo "==> gceworker state is ${state}, attempting start"
      "${GCEWORKER}" start >/dev/null
      ;;
  esac
}

# ssh_exec runs a remote shell command via gceworker.sh ssh, forwarding
# any local exit code.
ssh_exec() {
  "${GCEWORKER}" ssh --command="$*"
}

# rsync_to syncs the local worktree to ${REMOTE_DIR} on the gceworker.
# Excludes heavy build outputs to keep the transfer manageable; .git is
# included so the experiment scripts' env.txt capture (git rev-parse,
# git status) works the same as locally.
rsync_to() {
  local fq
  fq="$(fqname)"
  if ! grep -q "Host ${fq}" "${HOME}/.ssh/config" 2>/dev/null; then
    echo "==> ssh config entry for ${fq} missing; running 'gceworker.sh start' to refresh it"
    "${GCEWORKER}" start >/dev/null
  fi
  # accept-new auto-trusts a previously-unknown host key (without
  # silently re-trusting on key changes, which would defeat the
  # purpose of host-key checking).
  local ssh_opts="-o StrictHostKeyChecking=accept-new"
  echo "==> ensuring ~/${REMOTE_DIR}/ exists on ${NAME}"
  ssh ${ssh_opts} "${fq}" "mkdir -p ~/${REMOTE_DIR}"
  echo "==> rsyncing ${REPO_ROOT}/ -> ${fq}:${REMOTE_DIR}/"
  rsync -az --delete \
    --exclude '_bazel/' \
    --exclude 'bin/' \
    --exclude '/.git/lfs' \
    --exclude '*.test' \
    --exclude '*.prof' \
    --exclude 'experiments/point-select-fast-path/results/' \
    --progress \
    -e "ssh ${ssh_opts}" \
    "${REPO_ROOT}/" "${fq}:${REMOTE_DIR}/"

  # Capture local git provenance into a file on the remote. The run-*
  # scripts pick this up when their own `git` calls fail (which they
  # always will on the gceworker, since the worktree's .git is a
  # pointer to a path that only exists locally).
  echo "==> writing local-git-info snapshot to remote"
  local local_sha local_status
  local_sha="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo "(unknown)")"
  local_status="$(git -C "${REPO_ROOT}" status --porcelain 2>/dev/null || true)"
  ssh ${ssh_opts} "${fq}" "cat > ${REMOTE_DIR}/.local-git-info" <<EOF
git_sha:      ${local_sha}  (snapshot captured locally at sync time)
git_status:
$(echo "${local_status}" | sed 's/^/  /')
EOF

  # We deliberately do NOT run `dev doctor` on the remote. Doctor (a)
  # prompts interactively for a .bazelrc.user edit, which doesn't work
  # over non-interactive ssh, and (b) runs `git submodule update`,
  # which fails because our worktree's .git is a pointer file to a
  # path that only exists on the local host. Instead, the run-*
  # scripts set AUTOMATION=1 before invoking `./dev`, which causes
  # dev to skip its doctor-status check (pkg/cmd/dev/doctor.go).
  # Submodule contents are already shipped by rsync, so bazel can
  # find them.

  echo "==> sync complete"
}

remote_results_dir() {
  echo "${REMOTE_DIR}/experiments/point-select-fast-path/results"
}

cmd="${1-}"
[[ -n "${cmd}" ]] && shift || true

case "${cmd}" in
  sync)
    ensure_running
    rsync_to
    ;;

  run)
    if [[ $# -lt 1 ]]; then
      echo "usage: $0 run <script-name> [args...]" >&2
      echo "  e.g.: $0 run run-baseline.sh" >&2
      echo "        $0 run run-fastpath.sh" >&2
      echo "        POINT_SELECT_FAST_PATH=1 $0 run run-profile.sh" >&2
      exit 2
    fi
    script="$1"; shift
    case "${script}" in
      run-baseline.sh|run-fastpath.sh|run-profile.sh) ;;
      *) echo "error: unknown script '${script}'. Allowed: run-baseline.sh, run-fastpath.sh, run-profile.sh" >&2; exit 2 ;;
    esac
    ensure_running

    # Forward known env vars our scripts read. Each is quoted so values
    # with whitespace survive the round-trip through ssh. Uses
    # `printf %q` instead of bash 4.4+ ${var@Q} so the script works on
    # the system bash on macOS (which is still 3.2).
    env_prefix=""
    for v in POINT_SELECT_FAST_PATH POINT_SELECT_NODES POINT_SELECT_SPLITS BENCH_FILTER BENCH_TIME BENCH_COUNT TEST_CPU; do
      if [[ -n "${!v:-}" ]]; then
        env_prefix+="${v}=$(printf %q "${!v}") "
      fi
    done

    args=()
    for a in "$@"; do args+=("$(printf %q "${a}")"); done

    echo "==> running ${env_prefix}experiments/point-select-fast-path/${script} ${args[*]:-} on ${NAME}"
    ssh_exec "cd ${REMOTE_DIR} && ${env_prefix}experiments/point-select-fast-path/${script} ${args[*]:-}"
    ;;

  fetch)
    ensure_running
    fq="$(fqname)"
    target="${SCRIPT_DIR}/results"
    mkdir -p "${target}"
    if [[ $# -ge 1 ]]; then
      run_dir="$1"
      echo "==> fetching ${fq}:$(remote_results_dir)/${run_dir} -> ${target}/"
      rsync -az --progress \
        "${fq}:$(remote_results_dir)/${run_dir}/" \
        "${target}/${run_dir}/"
    else
      echo "==> fetching all new result dirs from ${fq}:$(remote_results_dir)/"
      rsync -az --progress --ignore-existing \
        "${fq}:$(remote_results_dir)/" \
        "${target}/"
    fi
    ;;

  ssh)
    ensure_running
    "${GCEWORKER}" ssh --command="cd ${REMOTE_DIR} 2>/dev/null && exec \"\${SHELL:-bash}\" -l || exec \"\${SHELL:-bash}\" -l"
    ;;

  status)
    state="$(gcloud compute instances describe "${NAME}" --format='value(status)' 2>/dev/null || echo "MISSING")"
    echo "gceworker:        ${NAME}"
    echo "state:            ${state}"
    echo "remote dir:       ~/${REMOTE_DIR}"
    if [[ "${state}" == "RUNNING" ]]; then
      remote_check="$(ssh_exec "test -d ${REMOTE_DIR} && echo PRESENT || echo MISSING" 2>/dev/null || echo "UNKNOWN")"
      echo "remote worktree:  ${remote_check}"
      if [[ "${remote_check}" == "PRESENT" ]]; then
        echo "remote results:"
        ssh_exec "ls -1 ${REMOTE_DIR}/experiments/point-select-fast-path/results/ 2>/dev/null | sed 's/^/  /' || echo '  (none)'"
      fi
    else
      echo "remote worktree:  unknown (start the gceworker to check)"
    fi
    ;;

  ""|help|-h|--help)
    sed -n '3,/^set -euo pipefail$/p' "${BASH_SOURCE[0]}" | sed 's/^# \?//' | sed '$d'
    ;;

  *)
    echo "$0: unknown command '${cmd}'. See '$0 help'." >&2
    exit 2
    ;;
esac
